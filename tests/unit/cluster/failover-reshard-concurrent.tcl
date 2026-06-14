# Regression test: concurrent failover and slot migration.
#
# Verifies that configEpoch and slot ownership remain consistent when
# failover and slot migration (resharding) interleave. Covers:
#   1. CLUSTER FAILOVER TAKEOVER followed by slot import finalization
#      (epoch bump when node already has max epoch)
#   2. Auto-failover during active SETSLOT MIGRATING/IMPORTING
#   3. Full migration lifecycle (MIGRATING -> NODE -> STABLE) across failover

proc get_cluster_role {srv_idx} {
    set flags [dict get [cluster_get_myself $srv_idx] flags]
    set role [lindex $flags 1]
    return $role
}

proc wait_for_role {srv_idx role} {
    wait_for_condition 100 100 {
        [lindex [split [R $srv_idx ROLE] " "] 0] eq $role
    } else {
        fail "R $srv_idx didn't assume the $role role in time"
    }
    wait_for_condition 100 100 {
        [get_cluster_role $srv_idx] eq $role
    } else {
        fail "R $srv_idx cluster role didn't match $role in time"
    }
}

proc fail_server {server_id} {
    set node_timeout [lindex [R 0 CONFIG GET cluster-node-timeout] 1]
    pause_process [srv [expr -1*$server_id] pid]
    after [expr 3*$node_timeout]
    resume_process [srv [expr -1*$server_id] pid]
}

proc migrate_slot {from to slot} {
    set from_id [R $from CLUSTER MYID]
    set to_id [R $to CLUSTER MYID]
    assert_equal {OK} [R $from CLUSTER SETSLOT $slot MIGRATING $to_id]
    assert_equal {OK} [R $to CLUSTER SETSLOT $slot IMPORTING $from_id]
}

proc migrate_keys {from to slot keys} {
    set from_id [R $from CLUSTER MYID]
    set to_id [R $to CLUSTER MYID]
    set from_port [srv [expr -1*$from] port]
    set to_port [srv [expr -1*$to] port]
    foreach key $keys {
        R $from SET $key "value_$key"
    }
    foreach key $keys {
        R $from MIGRATE 127.0.0.1 $to_port $key 0 5000
    }
}

# ============================================================================
# Scenario 1: TAKEOVER + slot import finalization
#
# After a takeover failover, the new primary already has the highest epoch.
# If it then finalizes a slot import (SETSLOT NODE), the epoch bump must
# still succeed — producing a strictly higher epoch.
# ============================================================================
start_cluster 3 3 {tags {external:skip cluster} overrides {cluster-allow-replica-migration no cluster-node-timeout 1000}} {

    test "Scenario 1: Setup — initiate slot migration after takeover" {
        wait_for_cluster_state ok

        set R0_id [R 0 CLUSTER MYID]
        set R1_id [R 1 CLUSTER MYID]
        set R3_id [R 3 CLUSTER MYID]

        # R3 is replica of R0. Perform takeover so R3 becomes primary.
        R 3 CLUSTER FAILOVER TAKEOVER
        wait_for_role 3 master

        wait_for_cluster_propagation

        # Record R3's epoch after takeover
        set R3_epoch_after_takeover [CI 3 cluster_my_epoch]
        assert {$R3_epoch_after_takeover > 0}

        # Now initiate a slot migration from R1 to R3.
        # R1 owns slots 5462-10922, so slot 5462 is R1's first slot.
        migrate_slot 1 3 5462

        # Migrate the actual keys for slot 5462
        # "key:{5462}" hashes to slot 5462 (we need a key in that slot)
        # Use the crc16-based slot: slot 5462
        # Just set a key in slot 5462 on R1 and migrate it
        R 1 SET "{slot5462test}" "hello"
        set to_port [srv -3 port]
        R 1 MIGRATE 127.0.0.1 $to_port "{slot5462test}" 0 5000

        # Finalize the slot on R3 (importing side): SETSLOT NODE
        # This triggers clusterBumpConfigEpochWithoutConsensus() on R3,
        # which already has the highest epoch from the takeover.
        R 3 CLUSTER SETSLOT 5462 NODE $R3_id

        # Verify: R3's epoch must have been bumped beyond the takeover epoch
        set R3_epoch_after_setslot [CI 3 cluster_my_epoch]
        assert {$R3_epoch_after_setslot > $R3_epoch_after_takeover} \
            "Epoch was not bumped after SETSLOT NODE (takeover epoch: $R3_epoch_after_takeover, after: $R3_epoch_after_setslot)"

        # Finalize on R1 (source side)
        R 1 CLUSTER SETSLOT 5462 NODE $R3_id

        # Wait for the whole cluster to agree on the new topology
        wait_for_cluster_propagation

        # Verify R3 owns slot 5462 from all nodes' perspectives
        for {set j 0} {$j < 6} {incr j} {
            wait_for_condition 100 100 {
                [dict get [cluster_get_node_by_id $j $R3_id] config_epoch] == $R3_epoch_after_setslot
            } else {
                fail "Node $j doesn't see R3's new epoch ($R3_epoch_after_setslot)"
            }
        }
    }

    test "Scenario 1: Verify cluster consistency after takeover + setslot" {
        wait_for_cluster_state ok
        wait_for_cluster_propagation

        # Verify no open slots remain (migration was fully finalized)
        assert_equal {} [get_open_slots 3]
    }
}

# ============================================================================
# Scenario 2: Auto-failover during active SETSLOT MIGRATING/IMPORTING
#
# A slot migration is in progress (MIGRATING/IMPORTING states set but not
# yet finalized). The source master fails and its replica takes over.
# Verify:
#   - Migration states transfer to the new primary
#   - Other nodes update their importing/migrating references
#   - MOVED/ASK redirects remain consistent
# ============================================================================
start_cluster 3 3 {tags {external:skip cluster} overrides {cluster-allow-replica-migration no cluster-node-timeout 1000}} {

    test "Scenario 2: Setup — start migration then failover source" {
        wait_for_cluster_state ok

        set R0_id [R 0 CLUSTER MYID]
        set R1_id [R 1 CLUSTER MYID]
        set R3_id [R 3 CLUSTER MYID]

        # Initiate migration of slot 609 from R0 to R1
        migrate_slot 0 1 609

        # Verify migration states are set
        wait_for_slot_state 0 "\[609->-$R1_id\]"
        wait_for_slot_state 1 "\[609-<-$R0_id\]"
        # R3 (replica of R0) should also see the migrating state
        wait_for_slot_state 3 "\[609->-$R1_id\]"

        # Record epoch before failover
        set epoch_before [CI 0 cluster_current_epoch]

        # Trigger auto-failover on R0's shard: R0 fails, R3 takes over
        fail_server 0
        wait_for_role 0 slave

        # R3 should now be the new primary of the shard
        wait_for_role 3 master

        # Verify epoch was bumped
        set epoch_after [CI 3 cluster_current_epoch]
        assert {$epoch_after > $epoch_before} \
            "Epoch not bumped after auto-failover (before: $epoch_before, after: $epoch_after)"

        # Wait for the cluster to converge
        wait_for_cluster_propagation

        # R3 should own R0's old slots
        set R3_node [cluster_get_node_by_id 3 $R3_id]
        assert {[dict get $R3_node slots] ne {}}

        # R0 should own no slots (it's now a replica)
        set R0_node [cluster_get_node_by_id 3 $R0_id]
        assert {[dict get $R0_node slots] eq {}}

        # Verify migration states transferred: R3 should now be migrating
        # slot 609 to R1 (migration source updated from R0 to R3)
        wait_for_slot_state 3 "\[609->-$R1_id\]"
        wait_for_slot_state 1 "\[609-<-$R3_id\]"
    }

    test "Scenario 2: MOVED/ASK redirects are consistent after failover" {
        set R1_id [R 1 CLUSTER MYID]
        set R3_id [R 3 CLUSTER MYID]

        wait_for_cluster_propagation

        # Slot 609 is owned by R3 (new primary), with migrating state to R1.
        # A client hitting R3 for a key in slot 609:
        # - If key exists locally -> served by R3
        # - If key doesn't exist -> ASK redirect to R1
        # A client hitting R1 for a key in slot 609:
        # - Without ASKING -> MOVED to R3 (R3 is the owner)
        # - With ASKING -> served locally (importing)

        # Test MOVED redirect from R1 (without ASKING) -> should redirect to R3
        set R3_port [srv -3 port]
        catch {R 1 GET "aga"} err
        # "aga" hashes to slot 609
        assert_match "MOVED 609 *:$R3_port" $err
    }
}

# ============================================================================
# Scenario 3: Full migration lifecycle across a failover
#
# Start a migration, trigger a failover on the target shard, then complete
# the migration (SETSLOT NODE on both sides). Verify the finalization
# succeeds even though the target's epoch changed during failover.
# ============================================================================
start_cluster 3 3 {tags {external:skip cluster} overrides {cluster-allow-replica-migration no cluster-node-timeout 1000}} {

    test "Scenario 3: Complete slot migration across target-shard failover" {
        wait_for_cluster_state ok

        set R0_id [R 0 CLUSTER MYID]
        set R1_id [R 1 CLUSTER MYID]
        set R4_id [R 4 CLUSTER MYID]

        # Start migration of slot 609 from R0 to R1
        migrate_slot 0 1 609

        # Set a key in slot 609 on R0 and migrate it to R1
        R 0 SET "{slot609key}" "data"
        set R1_port [srv -1 port]
        R 0 MIGRATE 127.0.0.1 $R1_port "{slot609key}" 0 5000

        # Verify migration states
        wait_for_slot_state 0 "\[609->-$R1_id\]"
        wait_for_slot_state 1 "\[609-<-$R0_id\]"
        wait_for_slot_state 4 "\[609-<-$R0_id\]"

        # Trigger failover on R1's shard: R1 fails, R4 takes over
        fail_server 1
        wait_for_role 1 slave
        wait_for_role 4 master

        wait_for_cluster_propagation

        # Migration source should auto-update: R0 now migrates to R4
        wait_for_slot_state 0 "\[609->-$R4_id\]"
        # R4 should be importing from R0
        wait_for_slot_state 4 "\[609-<-$R0_id\]"

        # Now finalize the migration on R4 (importing side): SETSLOT NODE
        # This triggers clusterBumpConfigEpochWithoutConsensus() on R4.
        # R4 already got a new epoch from the failover. The bump must succeed.
        set R4_epoch_before [CI 4 cluster_my_epoch]

        R 4 CLUSTER SETSLOT 609 NODE $R4_id

        set R4_epoch_after [CI 4 cluster_my_epoch]
        assert {$R4_epoch_after > $R4_epoch_before} \
            "Epoch not bumped on import finalization after failover (before: $R4_epoch_before, after: $R4_epoch_after)"

        # Finalize on R0 (source side)
        R 0 CLUSTER SETSLOT 609 NODE $R4_id

        # Clear migration states
        R 0 CLUSTER SETSLOT 609 STABLE
        R 4 CLUSTER SETSLOT 609 STABLE

        # Wait for the whole cluster to converge
        wait_for_cluster_propagation
        wait_for_cluster_state ok

        # Verify R4 owns slot 609 from all perspectives
        for {set j 0} {$j < 6} {incr j} {
            wait_for_condition 100 100 {
                [dict get [cluster_get_node_by_id $j $R4_id] config_epoch] == $R4_epoch_after
            } else {
                fail "Node $j doesn't see R4's epoch ($R4_epoch_after) after SETSLOT NODE"
            }
        }

        # Verify the key is accessible on R4
        assert_equal "data" [R 4 GET "{slot609key}"]

        # Verify no open slots remain
        assert_equal {} [get_open_slots 0]
        assert_equal {} [get_open_slots 4]
    }
}
