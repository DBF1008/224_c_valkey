# Regression tests for the partial-resync / replication-backlog boundary.
#
# These exercise the boundary that is shared by the accept/reject decision
# (primaryTryPartialResynchronization) and the data serving path
# (addReplyReplicationBacklog), focusing on the reconnect, backlog-rebuild and
# multi-replica concurrent-ACK scenarios where the backlog offset window and
# the replication ID change underneath a (re)connecting replica.

start_server {tags {"repl" "external:skip"}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    $master config set repl-backlog-size 1mb
    $master config set repl-backlog-ttl 3600
    $master config set dual-channel-replication-enabled no

    start_server {} {
        set replica [srv 0 client]
        $replica config set dual-channel-replication-enabled no

        test "Partial resync after a brief reconnect (no full resync)" {
            $master set k0 v0
            $replica replicaof $master_host $master_port
            wait_for_sync $replica
            verify_replica_online $master 0 100

            # Baseline counters taken after the initial full sync, so the
            # assertions below only reflect the reconnect.
            set full_before [status $master sync_full]
            set partial_before [status $master sync_partial_ok]

            $master set k1 v1
            # Break the link from the replica side; the replica will reconnect.
            $replica client kill type master

            # Writes performed while the replica is briefly disconnected. They
            # stay well within repl-backlog-size, so the offset the replica asks
            # for remains inside the backlog window and a partial resync is
            # expected.
            for {set i 0} {$i < 50} {incr i} { $master set key:$i val:$i }

            wait_for_sync $replica
            verify_replica_online $master 0 100
            wait_for_ofs_sync $master $replica

            # The reconnect must have been served as a partial resync, with no
            # additional full resync, and the data must be identical.
            assert_equal $full_before [status $master sync_full]
            assert {[status $master sync_partial_ok] > $partial_before}
            assert_equal [$master debug digest] [$replica debug digest]
        }
    }
}

start_server {tags {"repl" "external:skip"}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    $master config set dual-channel-replication-enabled no

    start_server {} {
        set replica [srv 0 client]
        $replica config set dual-channel-replication-enabled no

        test "Backlog rebuild after TTL forces a clean full resync (no discontinuity, no crash)" {
            $master set k0 v0
            $replica replicaof $master_host $master_port
            wait_for_sync $replica
            verify_replica_online $master 0 100

            set replid_before [status $master master_replid]

            # Detach the replica so the primary has no replicas attached.
            $replica replicaof no one
            wait_for_condition 50 100 {
                [status $master connected_slaves] == 0
            } else {
                fail "replica still attached to master"
            }

            # Let the backlog expire: it is freed and, as part of the same path,
            # the replication ID is changed. This is the "switch backlog"
            # boundary an old replica can race against.
            $master config set repl-backlog-ttl 1
            wait_for_condition 100 100 {
                [status $master repl_backlog_active] == 0
            } else {
                fail "replication backlog was not freed after its TTL"
            }
            assert {[status $master master_replid] ne $replid_before}

            # New writes after the rebuild; the old replica has none of these.
            for {set i 0} {$i < 20} {incr i} { $master set new:$i v:$i }

            set full_before [status $master sync_full]

            # Reconnect. The replication ID no longer matches, so this must be a
            # full resync. The key property is that the master never serves a
            # chunk discontinuous with the (rebuilt) backlog and the datasets
            # end up identical.
            $replica replicaof $master_host $master_port
            wait_for_sync $replica
            verify_replica_online $master 0 100
            wait_for_ofs_sync $master $replica

            assert {[status $master sync_full] > $full_before}
            assert_equal [$master debug digest] [$replica debug digest]
        }
    }
}

start_server {tags {"repl" "external:skip"}} {
    set master [srv 0 client]
    set master_host [srv 0 host]
    set master_port [srv 0 port]
    $master config set dual-channel-replication-enabled no

    start_server {} {
        set replica1 [srv 0 client]
        $replica1 config set dual-channel-replication-enabled no

        start_server {} {
            set replica2 [srv 0 client]
            $replica2 config set dual-channel-replication-enabled no

            test "Concurrent ACKs from multiple replicas converge and satisfy WAIT" {
                $replica1 replicaof $master_host $master_port
                $replica2 replicaof $master_host $master_port
                wait_for_sync $replica1
                wait_for_sync $replica2
                verify_replica_online $master 0 100
                verify_replica_online $master 1 100

                $master set wk v
                # Both replicas must acknowledge the write offset. This drives
                # replicationCountAcksByOffset() across multiple online replicas.
                assert_equal 2 [$master wait 2 5000]

                wait_for_ofs_sync $master $replica1
                wait_for_ofs_sync $master $replica2
                assert_equal [$master debug digest] [$replica1 debug digest]
                assert_equal [$master debug digest] [$replica2 debug digest]
            }
        }
    }
}
