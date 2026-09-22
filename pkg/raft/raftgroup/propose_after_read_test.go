package raftgroup_test

import (
	"context"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/stretchr/testify/require"
)

func TestProposeAfterReadFencesLogAndTermChanges(t *testing.T) {
	rg := raftgroup.New(newTestOptions(raftgroup.WithStorage(newTestStorage())))
	require.NoError(t, rg.Start())
	t.Cleanup(rg.Stop)
	n := raft.NewNode(0, types.RaftState{}, raft.NewOptions(raft.WithKey("slot"), raft.WithNodeId(1)))
	rg.AddRaft(n)
	cfg := types.Config{Leader: 1, Term: 2, Version: 1, Role: types.RoleLeader, Replicas: []uint64{1}}
	require.NoError(t, rg.AddEventWait("slot", types.Event{Type: types.ConfChange, Config: cfg}))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	before, err := rg.ReadLeaderState(ctx, "slot")
	require.NoError(t, err)
	index, err := rg.ProposeAfterRead(ctx, "slot", before, 1, []byte("first"))
	require.NoError(t, err)
	require.Equal(t, uint64(1), index)
	_, err = rg.ProposeAfterRead(ctx, "slot", before, 2, []byte("stale read"))
	require.ErrorIs(t, err, raftgroup.ErrReadStateChanged)
	before, err = rg.ReadLeaderState(ctx, "slot")
	require.NoError(t, err)
	cfg.Term++
	require.NoError(t, rg.AddEventWait("slot", types.Event{Type: types.ConfChange, Config: cfg}))
	_, err = rg.ProposeAfterRead(ctx, "slot", before, 3, []byte("old term"))
	require.ErrorIs(t, err, raftgroup.ErrReadStateChanged)
	state, err := rg.ReadLeaderState(ctx, "slot")
	require.NoError(t, err)
	require.Equal(t, uint64(1), state.AppliedIndex)
	index, err = rg.ProposeAfterRead(ctx, "slot", state, 4, []byte("new term"))
	require.NoError(t, err)
	require.Equal(t, uint64(2), index)
}
