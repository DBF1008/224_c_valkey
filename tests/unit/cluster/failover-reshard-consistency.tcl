# Regression coverage for configEpoch / slot-ownership consistency when a
# primary failover is interleaved with a slot reshard.
#
# Valkey applies a slot-ownership change as a single same-tick transaction:
# the in-memory configEpoch and the slot table are mutated synchronously in the
# packet/command handler, and the config is fsync'd to nodes.conf *before* the
# ownership PONG is gossiped (see clusterBeforeSleep() in src/cluster_legacy.c,
# which runs CLUSTER_TODO_SAVE_CONFIG before CLUSTER_TODO_BROADCAST_ALL, plus
# clusterFailoverReplaceYourPrimary(), clusterUpdateSlotsConfigWith() and
# clusterCommandSetSlot()).
#
# These tests assert that the *committed* owner of a slot stays consistent
# across every observable surface even when a failover races a reshard:
#   1. every node's gossip view (CLUSTER SLOTS) agrees on the owner,
#   2. slot coverage is never lost and the cluster stays in state "ok",
#   3. no two slot-serving primaries share a configEpoch (no split-brain),
#   4. the on-disk nodes.conf of every node names the same owner (persisted
#      state matches the gossiped/announced state),
#   5. client redirection (MOVED) and the replication topology agree with the
#      committed owner.
#
# The existing slot-migration.tcl only checks that the MIGRATING/IMPORTING
# open-slot markers follow a failover; this file additionally locks in the
# stronger ownership/epoch/persistence invariants.

# Absolute path to a node's persisted cluster config (nodes.conf).
proc frc_nodes_conf_path {id} {
    set dir [lindex [R $id config get dir] 1]
    set conf [lindex [R $id config get cluster-config-file] 1]
    return [file join $dir $conf]
}

# Return 1 if $slot is contained in a list of slot-range tokens such as
# {0-5460 5461 10923-16383}. Migration markers like "[609-<-id]" are ignored.
proc frc_slot_in_ranges {slot ranges} {
    foreach tok $ranges {
        if {[string match {\[*} $tok]} continue
        if {[regexp {^(\d+)-(\d+)$} $tok -> a b]} {
            if {$slot >= $a && $slot <= $b} {return 1}
        } elseif {[regexp {^\d+$} $tok]} {
            if {$slot == $tok} {return 1}
        }
    }
    return 0
}

# The primary node-id that owns $slot, according to node $idx's CLUSTER SLOTS.
proc frc_slot_owner_id {idx slot} {
    foreach entry [R $idx cluster slots] {
        if {$slot >= [lindex $entry 0] && $slot <= [lindex $entry 1]} {
            return [lindex $entry 2 2]
        }
    }
    return ""
}

# Does reader node $reader_idx's on-disk nodes.conf record $owner_id as the
# owner of $slot? (nodes.conf lines share the CLUSTER NODES format.)
proc frc_conf_owner_has_slot {reader_idx owner_id slot} {
    set path [frc_nodes_conf_path $reader_idx]
    if {![file exists $path]} {return 0}
    set fd [open $path r]
    set data [read $fd]
    close $fd
    foreach line [split $data "\n"] {
        set line [string trim $line]
        if {$line eq ""} continue
        set toks [split $line]
        if {[lindex $toks 0] ne $owner_id} continue
        return [frc_slot_in_ranges $slot [lrange $toks 8 end]]
    }
    return 0
}

# configEpoch of $node_id as seen by node $idx (-1 if unknown).
proc frc_config_epoch {idx node_id} {
    foreach n [get_cluster_nodes $idx] {
        if {[dict get $n id] eq $node_id} {
            return [dict get $n config_epoch]
        }
    }
    return -1
}

# Fail if any two primaries that actually serve slots share a configEpoch.
proc frc_assert_no_epoch_collision {idx} {
    set seen [dict create]
    foreach n [get_cluster_nodes $idx] {
        if {![cluster_has_flag $n master]} continue
        set serving 0
        foreach tok [dict get $n slots] {
            if {![string match {\[*} $tok]} {set serving 1; break}
        }
        if {!$serving} continue
        set e [dict get $n config_epoch]
        set id [dict get $n id]
        if {[dict exists $seen $e]} {
            fail "configEpoch collision: epoch $e shared by [dict get $seen $e] and $id"
        }
        dict set seen $e $id
    }
}

# Total number of covered slots according to node $idx (should be 16384).
proc frc_total_covered_slots {idx} {
    set total 0
    foreach entry [R $idx cluster slots] {
        set total [expr {$total + [lindex $entry 1] - [lindex $entry 0] + 1}]
    }
    return $total
}

# Assert the committed owner of $slot is $owner_id across every surface.
proc frc_assert_committed_owner {slot owner_id} {
    # The cluster must first settle into a consistent, fully-covered "ok" state.
    wait_for_cluster_propagation
    wait_for_cluster_state ok

    # 1) Every alive node's gossip view agrees on the owner.
    for {set j 0} {$j < [llength $::servers]} {incr j} {
        if {![process_is_alive [srv [expr -1*$j] pid]]} continue
        assert_equal $owner_id [frc_slot_owner_id $j $slot] \
            "node $j gossip view of slot $slot owner"
    }

    # 2) Full slot coverage (ownership moved, never lost).
    assert_equal 16384 [frc_total_covered_slots 0] "total covered slots"

    # 3) No split-brain: distinct configEpoch per slot-serving primary.
    frc_assert_no_epoch_collision 0

    # 4) Persisted state matches the announced owner on every node.
    for {set j 0} {$j < [llength $::servers]} {incr j} {
        if {![process_is_alive [srv [expr -1*$j] pid]]} continue
        wait_for_condition 50 100 {
            [frc_conf_owner_has_slot $j $owner_id $slot] == 1
        } else {
            fail "nodes.conf on node $j does not record $owner_id owning slot $slot"
        }
    }
}

start_cluster 3 3 {tags {external:skip cluster} overrides {cluster-allow-replica-migration no cluster-node-timeout 1000}} {

    # Pick a key and the slot it hashes to, then discover the slot's owner.
    set hashtag "frc"
    set rkey "{$hashtag}:1"
    set slot [R 0 cluster keyslot $rkey]

    set src -1
    foreach p {0 1 2} {
        if {[R $p cluster myid] eq [frc_slot_owner_id 0 $slot]} {set src $p}
    }
    assert {$src != -1}
    set dst [expr {$src == 0 ? 1 : 0}]
    set src_replica [expr {$src + 3}]
    set dst_replica [expr {$dst + 3}]
    set src_id [R $src cluster myid]
    set dst_id [R $dst cluster myid]

    test "Reshard finalizes consistently when the source shard fails over mid-migration" {
        set pre_epoch [frc_config_epoch 0 $src_id]

        # Start a cross-shard reshard of $slot from the source to the destination.
        assert_equal {OK} [R $src cluster setslot $slot migrating $dst_id]
        assert_equal {OK} [R $dst cluster setslot $slot importing $src_id]

        # Fail the SOURCE shard over to its replica while the migration is open.
        assert_equal {OK} [R $src_replica cluster failover]
        wait_for_role $src_replica master
        wait_for_role $src slave
        wait_for_cluster_propagation

        set new_src $src_replica
        set new_src_id [R $new_src cluster myid]
        # Ownership has not transferred yet: still the source shard, now new_src.
        assert_equal $new_src_id [frc_slot_owner_id 0 $slot]

        # Finalize ownership to the destination shard.
        assert_equal {OK} [R $dst cluster setslot $slot node $dst_id]
        assert_equal {OK} [R $new_src cluster setslot $slot node $dst_id]

        # The committed owner is the destination, consistently everywhere.
        frc_assert_committed_owner $slot $dst_id

        # Finalizing the import bumps the importer's epoch past the slot's
        # previous owner epoch (monotonic, collision-free).
        assert {[frc_config_epoch 0 $dst_id] > $pre_epoch}
    }

    test "Failover of the new owner shard preserves the freshly acquired slot" {
        set pre_epoch [frc_config_epoch 0 $dst_id]

        # Fail the DESTINATION shard (current owner of $slot) over to its replica.
        # The promoted replica must claim the just-acquired slot atomically with
        # its election-won configEpoch.
        assert_equal {OK} [R $dst_replica cluster failover]
        wait_for_role $dst_replica master
        wait_for_role $dst slave

        set promoted_id [R $dst_replica cluster myid]
        frc_assert_committed_owner $slot $promoted_id

        # Election winner advances the configEpoch beyond the demoted primary.
        assert {[frc_config_epoch 0 $promoted_id] > $pre_epoch}
    }

    test "MOVED redirection and replication topology agree with the committed owner" {
        set owner_idx $dst_replica
        set owner_id [R $owner_idx cluster myid]

        # A cluster-aware client (which follows MOVED) routes the key correctly.
        set cl [valkey_cluster 127.0.0.1:[srv 0 port]]
        $cl set $rkey "hello-frc"
        assert_equal "hello-frc" [$cl get $rkey]
        $cl close

        # Direct read on the committed owner succeeds.
        assert_equal "hello-frc" [R $owner_idx get $rkey]

        # A non-owner primary returns MOVED for this slot (advisory redirect
        # consistent with the committed owner).
        set other -1
        foreach p {0 1 2 3 4 5} {
            if {$p == $owner_idx} continue
            if {[get_cluster_role $p] ne "master"} continue
            set other $p
            break
        }
        assert {$other != -1}
        catch {R $other get $rkey} e
        assert_match "MOVED $slot *" $e

        # Replication topology: the demoted destination primary now replicates
        # the promoted owner.
        wait_for_condition 100 100 {
            [dict get [cluster_get_myself $dst] slaveof] eq $owner_id
        } else {
            fail "node $dst does not report $owner_id as its primary"
        }
    }
}
