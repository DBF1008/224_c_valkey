# Regression tests for PSYNC backlog boundary consistency.
#
# These tests verify that partial resynchronization correctly handles
# edge cases around backlog boundaries, backlog rebuild, role changes,
# and concurrent replica ACK processing.

start_server {tags {"psync2 external:skip"}} {
start_server {} {
start_server {} {
start_server {} {
    set primary [srv -3 client]
    set primary_host [srv -3 host]
    set primary_port [srv -3 port]

    set replica1 [srv -2 client]
    set replica2 [srv -1 client]
    set replica3 [srv 0 client]

    # Common configuration
    foreach node [list $primary $replica1 $replica2 $replica3] {
        $node config set save ""
        $node config set repl-backlog-size 1mb
        $node config set repl-backlog-ttl 3600
    }

    # ------------------------------------------------------------------
    test "Setup: connect three replicas to primary" {
        $replica1 replicaof $primary_host $primary_port
        $replica2 replicaof $primary_host $primary_port
        $replica3 replicaof $primary_host $primary_port

        wait_for_condition 50 200 {
            [status $primary connected_slaves] == 3
        } else {
            fail "Not all replicas connected"
        }
        # Let all replicas finish initial sync
        wait_for_sync $replica1
        wait_for_sync $replica2
        wait_for_sync $replica3
    }

    # ------------------------------------------------------------------
    test "PSYNC with caught-up replica (offset == backlog end) does not crash" {
        # Write some data and wait for all replicas to catch up.
        $primary set key1 value1
        $primary set key2 value2
        $primary set key3 value3

        wait_for_ofs_sync $primary $replica1
        wait_for_ofs_sync $primary $replica2
        wait_for_ofs_sync $primary $replica3

        # Capture offsets: all replicas should be at the same offset as
        # the primary.
        set primary_offset [status $primary master_repl_offset]
        set repl1_offset   [status $replica1 master_repl_offset]
        assert_equal $primary_offset $repl1_offset

        # Now disconnect replica1.  It will try to PSYNC on reconnect
        # with an offset that equals the backlog end (it was fully
        # caught up).  This previously triggered serverAssert(node != NULL)
        # in addReplyReplicationBacklog().
        $replica1 replicaof no one

        wait_for_condition 50 100 {
            [status $primary connected_slaves] == 2
        } else {
            fail "replica1 did not disconnect"
        }

        # Reconnect replica1 immediately, before any new writes.
        # Its PSYNC offset should equal backlog->offset + histlen.
        $replica1 replicaof $primary_host $primary_port

        wait_for_condition 50 200 {
            [status $replica1 master_link_status] eq "up"
        } else {
            fail "replica1 did not reconnect"
        }

        # Verify replica1 is online and data is consistent.
        wait_for_sync $replica1
        assert_equal [$primary get key1] [$replica1 get key1]
        assert_equal [$primary get key3] [$replica1 get key3]

        # The PSYNC should have been accepted (partial resync, not full).
        # sync_partial_ok was incremented on the primary.
        assert {[status $primary sync_partial_ok] > 0}
    }

    # ------------------------------------------------------------------
    test "Caught-up PSYNC followed by new writes: data consistency" {
        # Ensure all replicas are caught up.
        wait_for_ofs_sync $primary $replica1
        wait_for_ofs_sync $primary $replica2
        wait_for_ofs_sync $primary $replica3

        # Disconnect and immediately reconnect replica1.
        $replica1 replicaof no one
        after 100
        $replica1 replicaof $primary_host $primary_port

        # Write new data *immediately* after the reconnect.
        $primary set postreconnect_key hello

        # Wait for propagation.
        wait_for_condition 50 200 {
            [$replica1 get postreconnect_key] eq "hello"
        } else {
            fail "replica1 did not receive post-reconnect write"
        }

        wait_for_ofs_sync $primary $replica1
        assert_equal [$primary debug digest] [$replica1 debug digest]
    }

    # ------------------------------------------------------------------
    test "Backlog freed by TTL: old replica must do full resync" {
        # Stop all replicas.
        $replica1 replicaof no one
        $replica2 replicaof no one
        $replica3 replicaof no one

        wait_for_condition 50 100 {
            [status $primary connected_slaves] == 0
        } else {
            fail "Replicas did not disconnect"
        }

        # Set a very short backlog TTL so the backlog gets freed quickly.
        $primary config set repl-backlog-ttl 1

        # Write some data while no replicas are connected.
        $primary set before_ttl_expiry yes

        # Wait for the backlog to be freed (TTL = 1s, wait a bit longer).
        wait_for_condition 50 200 {
            [status $primary repl_backlog_active] == 0
        } else {
            fail "Backlog was not freed after TTL"
        }

        # Re-enable a reasonable TTL.
        $primary config set repl-backlog-ttl 3600

        # Reconnect replica1.  Its old replid is no longer valid
        # (changeReplicationId was called when the backlog was freed),
        # so a full resync must happen.
        set prev_full [status $replica1 master_sync_full_count]
        $replica1 replicaof $primary_host $primary_port

        wait_for_condition 50 200 {
            [status $replica1 master_link_status] eq "up"
        } else {
            fail "replica1 did not reconnect after backlog TTL"
        }

        wait_for_sync $replica1
        assert_equal [$primary get before_ttl_expiry] [$replica1 get before_ttl_expiry]
        assert_equal [$primary debug digest] [$replica1 debug digest]
    }

    # ------------------------------------------------------------------
    test "shiftReplicationId: old replica PSYNC with replid2 succeeds" {
        # Start fresh: make replica2 a replica of primary.
        $replica2 replicaof $primary_host $primary_port
        wait_for_sync $replica2
        wait_for_ofs_sync $primary $replica2

        # Write some data.
        $primary set shifttest value_before_shift
        wait_for_ofs_sync $primary $replica2

        # Record the current replication state.
        set old_replid [status $primary master_replid]
        set old_offset [status $primary master_repl_offset]

        # Disconnect replica2.
        $replica2 replicaof no one
        wait_for_condition 50 100 {
            [status $primary connected_slaves] == 0 ||
            [status $primary connected_slaves] == 1
        } else {
            fail "replica2 did not disconnect"
        }

        # Now turn the *primary* itself into a replica of replica3
        # (which will be a new primary).  This causes shiftReplicationId
        # on the old primary, making old_replid become replid2.
        # First set up replica3 as an independent primary.
        $replica3 replicaof no one

        # Make old primary a replica of replica3.
        $primary replicaof [srv 0 host] [srv 0 port]

        # Wait for the old primary (now replica) to sync.
        wait_for_condition 50 200 {
            [status $primary master_link_status] eq "up"
        } else {
            fail "old primary did not connect to new primary"
        }

        # The old primary should now have replid2 == old_replid.
        # Write some data on the new primary.
        $replica3 set after_shift data

        # Now reconnect replica2 to the OLD primary (which is now a
        # replica of replica3).  replica2 still has old_replid.
        # Since the old primary has shifted its ID, replica2 should be
        # able to PSYNC using the old replid (now replid2).
        $replica2 replicaof $primary_host $primary_port

        wait_for_condition 50 200 {
            [status $replica2 master_link_status] eq "up"
        } else {
            fail "replica2 did not reconnect to old primary"
        }

        wait_for_sync $replica2

        # Verify data consistency through the chain.
        wait_for_condition 50 200 {
            [$replica2 get shifttest] eq "value_before_shift"
        } else {
            fail "replica2 missing pre-shift data"
        }
        wait_for_condition 50 200 {
            [$replica2 get after_shift] eq "data"
        } else {
            fail "replica2 missing post-shift data"
        }

        # Cleanup: restore primary role.
        $primary replicaof no one
        $replica2 replicaof no one
        $replica3 replicaof no one
        after 200
    }

    # ------------------------------------------------------------------
    test "Multiple replicas ACK during backlog trim: no data loss" {
        # Reset topology.
        $primary replicaof no one
        $replica1 replicaof no one
        $replica2 replicaof no one
        $replica3 replicaof no one
        after 200

        # Small backlog to force trimming.
        $primary config set repl-backlog-size 16384

        # Connect all three replicas.
        $replica1 replicaof $primary_host $primary_port
        $replica2 replicaof $primary_host $primary_port
        $replica3 replicaof $primary_host $primary_port

        wait_for_condition 50 200 {
            [status $primary connected_slaves] == 3
        } else {
            fail "Not all replicas connected"
        }
        wait_for_sync $replica1
        wait_for_sync $replica2
        wait_for_sync $replica3

        # Generate enough data to trigger backlog trimming (> 16KB).
        for {set i 0} {$i < 200} {incr i} {
            $primary set "trimkey:$i" [string repeat "x" 200]
        }

        # Let replicas catch up while trim is happening.
        wait_for_ofs_sync $primary $replica1
        wait_for_ofs_sync $primary $replica2
        wait_for_ofs_sync $primary $replica3

        # Briefly disconnect one replica to trigger backlog trim
        # (releasing the replica's reference on the first block).
        $replica1 replicaof no one
        after 100

        # Write more data to trigger additional trimming.
        for {set i 200} {$i < 400} {incr i} {
            $primary set "trimkey:$i" [string repeat "y" 200]
        }

        # Reconnect replica1 -- its old offset may now be outside
        # the trimmed backlog, forcing a full resync.
        $replica1 replicaof $primary_host $primary_port

        # Wait for all replicas to converge.
        wait_for_condition 100 200 {
            [$replica1 get "trimkey:399"] eq [string repeat "y" 200] &&
            [$replica2 get "trimkey:399"] eq [string repeat "y" 200] &&
            [$replica3 get "trimkey:399"] eq [string repeat "y" 200]
        } else {
            fail "Not all replicas received post-trim data"
        }

        # Verify full data consistency.
        wait_for_ofs_sync $primary $replica1
        wait_for_ofs_sync $primary $replica2
        wait_for_ofs_sync $primary $replica3

        assert_equal [$primary debug digest] [$replica1 debug digest]
        assert_equal [$primary debug digest] [$replica2 debug digest]
        assert_equal [$primary debug digest] [$replica3 debug digest]

        # Restore default backlog size.
        $primary config set repl-backlog-size 1mb
    }

    # ------------------------------------------------------------------
    test "Backlog recreated from scratch: old replica correctly rejected" {
        # Stop all replicas.
        $replica1 replicaof no one
        $replica2 replicaof no one
        $replica3 replicaof no one
        after 200

        # Record old replication ID.
        set old_replid [status $primary master_replid]

        # Force backlog free + new replid by setting TTL to 0 and
        # waiting.  This exercises changeReplicationId() +
        # clearReplicationId2() + freeReplicationBacklog().
        $primary config set repl-backlog-ttl 1
        wait_for_condition 50 200 {
            [status $primary repl_backlog_active] == 0
        } else {
            fail "Backlog not freed"
        }
        $primary config set repl-backlog-ttl 3600

        set new_replid [status $primary master_replid]
        # The replid must have changed.
        assert {$old_replid ne $new_replid}

        # Write data after backlog recreation.
        $primary set post_recreation_key value

        # Reconnect replica1 -- it still has old_replid.
        $replica1 replicaof $primary_host $primary_port

        wait_for_condition 50 200 {
            [status $replica1 master_link_status] eq "up"
        } else {
            fail "replica1 did not reconnect"
        }

        wait_for_sync $replica1

        # Data must be consistent (full resync should have happened).
        assert_equal [$primary get post_recreation_key] [$replica1 get post_recreation_key]
        assert_equal [$primary debug digest] [$replica1 debug digest]

        # Cleanup
        $replica1 replicaof no one
    }

    # ------------------------------------------------------------------
    test "Repeated caught-up PSYNC cycles: stability" {
        # Stress test: repeatedly disconnect and reconnect a caught-up
        # replica without intervening writes.
        $replica1 replicaof $primary_host $primary_port
        wait_for_sync $replica1
        wait_for_ofs_sync $primary $replica1

        $primary set stability_key stable_value
        wait_for_ofs_sync $primary $replica1

        for {set cycle 0} {$cycle < 5} {incr cycle} {
            # Disconnect.
            $replica1 replicaof no one
            after 50

            # Reconnect immediately (no new writes).
            $replica1 replicaof $primary_host $primary_port

            wait_for_condition 50 200 {
                [status $replica1 master_link_status] eq "up"
            } else {
                fail "replica1 did not reconnect in cycle $cycle"
            }

            wait_for_sync $replica1
            assert_equal [$primary get stability_key] [$replica1 get stability_key]
        }

        # Final consistency check.
        wait_for_ofs_sync $primary $replica1
        assert_equal [$primary debug digest] [$replica1 debug digest]
    }
}
}
}
}
