package cluster

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/clusterconfig"
	"github.com/WuKongIM/WuKongIM/pkg/trace"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

func TestChannelConfigReconcileRPC(t *testing.T) {
	previous := trace.GlobalTrace
	trace.SetGlobalTrace(trace.New(context.Background(), trace.NewOptions()))
	t.Cleanup(func() { trace.SetGlobalTrace(previous) })
	addresses := make(map[uint64]string)
	for id := uint64(1); id <= 2; id++ {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		addresses[id] = listener.Addr().String()
		require.NoError(t, listener.Close())
	}
	var servers []*Server
	for id := uint64(1); id <= 2; id++ {
		config := newTestOptions(t, id, addresses, clusterconfig.WithSlotCount(2), clusterconfig.WithSlotMaxReplicaCount(2))
		s := New(NewOptions(WithAddr("tcp://"+addresses[id]), WithConfigOptions(config), WithDataDir(t.TempDir()),
			WithDBWKDbShardNum(1), WithDBSlotShardNum(1), WithDBWKDbMemTableSize(1<<20), WithDBSlotMemTableSize(1<<20)))
		require.NoError(t, s.Start())
		t.Cleanup(s.Stop)
		// Control all hints in this test, while leaving the real RPC handlers and
		// applied metadata barrier running on both TCP peers.
		s.configReconciler.stop()
		servers = append(servers, s)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for _, s := range servers {
		require.NoError(t, s.WaitAllSlotReady(ctx, 2))
	}
	owner, leader := servers[0], servers[1]
	var id string
	for i := 0; i < 1000; i++ {
		candidate := fmt.Sprintf("reconcile-rpc-%d", i)
		if owner.cfgServer.SlotLeaderId(owner.getSlotId(candidate)) == 1 {
			id = candidate
			break
		}
	}
	require.NotEmpty(t, id)
	cfg := wkdb.ChannelClusterConfig{ChannelId: id, ChannelType: 2, LeaderId: 2, Term: 1, ConfVersion: 1, Replicas: []uint64{2}}
	require.NoError(t, owner.db.SaveChannelClusterConfig(cfg))
	require.NoError(t, owner.requestChannelConfigReconcile(ctx, cfg))
	require.False(t, leader.channelServer.ExistChannel(id, 2), "ordinary dormant channel must stay dormant")
	require.NoError(t, leader.channelServer.WakeLeaderIfNeed(cfg))
	old := cfg.Clone()
	cfg.ConfVersion = 2
	require.NoError(t, owner.db.SaveChannelClusterConfig(cfg))
	require.ErrorIs(t, owner.requestChannelConfigReconcile(ctx, old), ErrConversationReadRetry, "stale hint must reload metadata")
	require.NoError(t, owner.requestChannelConfigReconcile(ctx, cfg))
	state, err := leader.channelServer.ReadLeaderState(ctx, id, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(2), state.ConfigVersion)
	// The next attempt follows the new message leader; delayed requests to the
	// old leader cannot install their stale routing snapshot.
	old = cfg.Clone()
	cfg.ConfVersion, cfg.LeaderId, cfg.Term, cfg.Replicas = 3, 1, 2, []uint64{1, 2}
	require.NoError(t, owner.db.SaveChannelClusterConfig(cfg))
	require.ErrorIs(t, owner.requestChannelConfigReconcile(ctx, old), ErrConversationReadRetry)
	require.NoError(t, owner.channelServer.WakeLeaderIfNeed(cfg))
	require.NoError(t, leader.reconcileChannelConfig(ctx, channelConfigKey{id, 2}))
	require.NoError(t, owner.reconcileChannelConfig(ctx, channelConfigKey{id, 2}))
	state, err = owner.channelServer.ReadLeaderState(ctx, id, 2)
	require.NoError(t, err)
	require.Equal(t, uint64(3), state.ConfigVersion)
	require.Equal(t, uint64(1), state.LeaderID)
}
