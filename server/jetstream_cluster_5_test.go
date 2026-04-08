// Copyright 2024-2026 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build !skip_js_tests && !skip_js_cluster_tests_5

package server

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// TestJetStreamClusterStreamFilestoreDivergenceOnRapidLeaderFlip reproduces a bug
// where a stream leader transition leaves followers with a different filestore than
// the new leader. The Raft WAL is consistent across all nodes, but the underlying
// filestores diverge. The new leader does not force followers to resync.
//
// Field scenario: after a network stall, Leader A wins election and sends catchup to
// followers. Seconds later, Leader B wins with a higher term. Leader B's filestore
// differs from what Leader A sent to followers because Leader B became leader before
// its own catchup completed -- the snapshot entry was skipped at applyStreamEntries
// (jetstream_cluster.go:4000) because mset.IsLeader() was already true. No resync
// is triggered. Followers retain Leader A's data.
//
// This test creates the post-divergence end state directly (truncated filestore on
// one node, forced to become leader) rather than racing the catchup path. It validates
// all observable consequences: filestore divergence, Raft reporting all current,
// new messages silently skipped on followers, and consumers delivering inconsistent data.
func TestJetStreamClusterStreamFilestoreDivergenceOnRapidLeaderFlip(t *testing.T) {
	c := createJetStreamClusterExplicit(t, "R3S", 3)
	defer c.shutdown()

	nc, js := jsClientConnect(t, c.randomServer())
	defer nc.Close()

	_, err := js.AddStream(&nats.StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"service.>"},
		Replicas:          3,
		MaxMsgsPerSubject: 5,
	})
	require_NoError(t, err)

	numSubjects := 20
	msgsPerSubject := 10
	for i := 0; i < numSubjects; i++ {
		for j := 0; j < msgsPerSubject; j++ {
			_, err := js.Publish(fmt.Sprintf("service.%d", i), []byte(fmt.Sprintf("msg-%d-%d", i, j)))
			require_NoError(t, err)
		}
	}

	c.waitOnAllCurrent()
	checkFor(t, 5*time.Second, 500*time.Millisecond, func() error {
		return checkState(t, c, globalAccountName, "TEST")
	})

	leaderSrv := c.streamLeader(globalAccountName, "TEST")
	leaderAcc, err := leaderSrv.lookupAccount(globalAccountName)
	require_NoError(t, err)
	leaderMset, err := leaderAcc.lookupStream("TEST")
	require_NoError(t, err)
	var origState StreamState
	leaderMset.store.FastState(&origState)
	t.Logf("Initial state: Msgs=%d FirstSeq=%d LastSeq=%d", origState.Msgs, origState.FirstSeq, origState.LastSeq)

	// Pick a follower to become the diverged leader (Node B).
	divergedSrv := c.randomNonStreamLeader(globalAccountName, "TEST")
	divergedAcc, err := divergedSrv.lookupAccount(globalAccountName)
	require_NoError(t, err)
	divergedMset, err := divergedAcc.lookupStream("TEST")
	require_NoError(t, err)

	// Truncate Node B's filestore to simulate it having fewer messages.
	// This mirrors the field state where Node 2 had 108 msgs vs 9,407 on others.
	// We only modify the filestore, not the WAL, matching the field behavior where
	// the WAL is consistent but the filestore diverges because a snapshot entry was
	// skipped on the leader (jetstream_cluster.go:4000).
	// Pattern from TestJetStreamClusterDesyncAfterRestartReplacesLeaderSnapshot.
	truncateSeq := origState.FirstSeq + (origState.LastSeq-origState.FirstSeq)/2
	divergedMset.mu.Lock()
	err = divergedMset.store.Truncate(truncateSeq)
	divergedMset.lseq = truncateSeq
	divergedMset.mu.Unlock()
	require_NoError(t, err)

	var truncatedState StreamState
	divergedMset.store.FastState(&truncatedState)
	t.Logf("Truncated Node B state: Msgs=%d FirstSeq=%d LastSeq=%d", truncatedState.Msgs, truncatedState.FirstSeq, truncatedState.LastSeq)

	// Force Node B to become stream leader. Put all other nodes' stream raft
	// nodes into observer mode so they won't campaign, then step down the current
	// leader and switch Node B to leader directly.
	divergedRN := divergedMset.raftNode().(*raft)
	for _, srv := range c.servers {
		if srv == divergedSrv {
			continue
		}
		acc, err := srv.lookupAccount(globalAccountName)
		require_NoError(t, err)
		mset, err := acc.lookupStream("TEST")
		require_NoError(t, err)
		rn := mset.raftNode().(*raft)
		rn.Lock()
		rn.observer = true
		rn.Unlock()
	}
	leaderMset.raftNode().StepDown()
	divergedRN.switchToLeader()

	c.waitOnStreamLeader(globalAccountName, "TEST")
	newLeader := c.streamLeader(globalAccountName, "TEST")
	require_Equal(t, newLeader.Name(), divergedSrv.Name())

	// Remove observer mode so the rest of the test works normally.
	for _, srv := range c.servers {
		if srv == divergedSrv {
			continue
		}
		acc, err := srv.lookupAccount(globalAccountName)
		require_NoError(t, err)
		mset, err := acc.lookupStream("TEST")
		require_NoError(t, err)
		rn := mset.raftNode().(*raft)
		rn.Lock()
		rn.observer = false
		rn.Unlock()
	}

	// Verify filestore divergence: leader should have fewer messages than followers.
	states := make(map[string]StreamState)
	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		for _, srv := range c.servers {
			acc, err := srv.lookupAccount(globalAccountName)
			if err != nil {
				return err
			}
			mset, err := acc.lookupStream("TEST")
			if err != nil {
				return err
			}
			var st StreamState
			mset.store.FastState(&st)
			states[srv.Name()] = st
		}
		leaderState := states[divergedSrv.Name()]
		for _, srv := range c.servers {
			if srv == divergedSrv {
				continue
			}
			if states[srv.Name()].LastSeq <= leaderState.LastSeq {
				return fmt.Errorf("waiting for divergence: follower %s LastSeq=%d <= leader LastSeq=%d",
					srv.Name(), states[srv.Name()].LastSeq, leaderState.LastSeq)
			}
		}
		return nil
	})

	for name, st := range states {
		t.Logf("Server %s (leader=%v): Msgs=%d FirstSeq=%d LastSeq=%d",
			name, name == divergedSrv.Name(), st.Msgs, st.FirstSeq, st.LastSeq)
	}

	leaderState := states[divergedSrv.Name()]

	// Verify Raft reports all replicas as "current" despite filestore divergence.
	nc.Close()
	nc, js = jsClientConnect(t, divergedSrv)
	defer nc.Close()

	si, err := js.StreamInfo("TEST")
	require_NoError(t, err)
	for _, r := range si.Cluster.Replicas {
		if !r.Current {
			t.Fatalf("Expected replica %s to be current, but it is not", r.Name)
		}
	}
	t.Logf("Raft reports all replicas current despite filestore divergence")

	if err := checkState(t, c, globalAccountName, "TEST"); err == nil {
		t.Fatalf("Expected checkState to report divergence, but it returned nil")
	} else {
		t.Logf("checkState correctly detected divergence: %v", err)
	}

	// Publish a new message and verify it's silently skipped on followers.
	// The leader encodes lseq=truncatedLastSeq in the Raft entry. Followers with
	// last=origLastSeq see lseq < last and skip (jetstream_cluster.go:4127).
	_, err = js.Publish("service.0", []byte("new-msg-after-divergence"))
	require_NoError(t, err)

	// Verify followers did not store the new message.
	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		// First confirm leader stored it.
		acc, err := divergedSrv.lookupAccount(globalAccountName)
		if err != nil {
			return err
		}
		mset, err := acc.lookupStream("TEST")
		if err != nil {
			return err
		}
		var st StreamState
		mset.store.FastState(&st)
		if st.LastSeq != leaderState.LastSeq+1 {
			return fmt.Errorf("leader LastSeq=%d, want %d", st.LastSeq, leaderState.LastSeq+1)
		}
		return nil
	})

	for _, srv := range c.servers {
		if srv == divergedSrv {
			continue
		}
		acc, err := srv.lookupAccount(globalAccountName)
		require_NoError(t, err)
		mset, err := acc.lookupStream("TEST")
		require_NoError(t, err)
		var st StreamState
		mset.store.FastState(&st)
		oldState := states[srv.Name()]
		if st.LastSeq != oldState.LastSeq {
			t.Fatalf("Follower %s LastSeq changed from %d to %d; expected no change (message should be silently skipped)",
				srv.Name(), oldState.LastSeq, st.LastSeq)
		}
	}
	t.Logf("New message stored on leader, silently skipped on followers")

	// Verify consumer divergence: consumers on different nodes see different data.
	// The follower's filestore has more subjects and messages than the leader's,
	// so consumers get inconsistent results depending on Raft leader placement.
	createAndCount := func(t *testing.T, name, deliverSubj string, targetSrv *Server) int {
		t.Helper()
		_, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
			Durable:        name,
			DeliverSubject: deliverSubj,
			DeliverPolicy:  nats.DeliverLastPerSubjectPolicy,
			FilterSubject:  "service.>",
			AckPolicy:      nats.AckExplicitPolicy,
		})
		require_NoError(t, err)
		c.waitOnConsumerLeader(globalAccountName, "TEST", name)

		for attempts := 0; attempts < 10; attempts++ {
			cl := c.consumerLeader(globalAccountName, "TEST", name)
			if cl == targetSrv {
				break
			}
			resp, err := nc.Request(fmt.Sprintf(JSApiConsumerLeaderStepDownT, "TEST", name), nil, 2*time.Second)
			require_NoError(t, err)
			var cdResp JSApiConsumerLeaderStepDownResponse
			require_NoError(t, json.Unmarshal(resp.Data, &cdResp))
			c.waitOnConsumerLeader(globalAccountName, "TEST", name)
		}
		cl := c.consumerLeader(globalAccountName, "TEST", name)
		if cl != targetSrv {
			t.Skipf("Could not move consumer %s leader to target server %s", name, targetSrv.Name())
		}

		cNC, _ := jsClientConnect(t, targetSrv)
		defer cNC.Close()
		sub, err := cNC.SubscribeSync(deliverSubj)
		require_NoError(t, err)
		defer sub.Unsubscribe()

		var count int
		for {
			_, err := sub.NextMsg(time.Second)
			if err != nil {
				break
			}
			count++
		}
		return count
	}

	var followerSrv *Server
	for _, srv := range c.servers {
		if srv != divergedSrv {
			followerSrv = srv
			break
		}
	}

	followerMsgCount := createAndCount(t, "dlps-follower", "deliver.dlps-follower", followerSrv)
	leaderMsgCount := createAndCount(t, "dlps-leader", "deliver.dlps-leader", divergedSrv)

	t.Logf("Consumer on follower node (%s) received %d messages", followerSrv.Name(), followerMsgCount)
	t.Logf("Consumer on leader node (%s) received %d messages", divergedSrv.Name(), leaderMsgCount)

	if followerMsgCount == leaderMsgCount {
		t.Fatalf("Expected consumer counts to differ (divergence), but both got %d", followerMsgCount)
	}
	t.Logf("BUG CONFIRMED: consumer on follower delivered %d msgs, leader delivered %d msgs (should be equal)",
		followerMsgCount, leaderMsgCount)

	// Verify that new messages published after the divergence are never delivered
	// to the follower consumer. The follower's filestore already has a higher LastSeq,
	// so new messages from the leader are silently skipped in applyStreamMsgOp.
	_, err = js.Publish("service.new-after-divergence", []byte("post-divergence"))
	require_NoError(t, err)

	checkFor(t, 5*time.Second, 200*time.Millisecond, func() error {
		followerAcc, err := followerSrv.lookupAccount(globalAccountName)
		if err != nil {
			return err
		}
		followerMset, err := followerAcc.lookupStream("TEST")
		if err != nil {
			return err
		}
		var followerFinalState StreamState
		followerMset.store.FastState(&followerFinalState)
		followerOldState := states[followerSrv.Name()]
		if followerFinalState.LastSeq != followerOldState.LastSeq {
			return fmt.Errorf("follower LastSeq changed from %d to %d", followerOldState.LastSeq, followerFinalState.LastSeq)
		}
		return nil
	})
	t.Logf("Post-divergence message silently skipped on follower (LastSeq unchanged at %d)", states[followerSrv.Name()].LastSeq)
}
