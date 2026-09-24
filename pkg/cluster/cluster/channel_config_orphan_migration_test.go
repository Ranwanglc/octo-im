package cluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	rafttype "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

func orphanMigrationConfig(t *testing.T, owner *Server, target uint64) wkdb.ChannelClusterConfig {
	t.Helper()
	id := fmt.Sprintf("orphan-migration-%d", target)
	for owner.cfgServer.SlotLeaderId(owner.getSlotId(id)) != owner.opts.ConfigOptions.NodeId {
		id += "x"
	}
	return wkdb.ChannelClusterConfig{ChannelId: id, ChannelType: 2, LeaderId: 1, Term: 1,
		ReplicaMaxCount: 3, Replicas: []uint64{1}, MigrateFrom: target, MigrateTo: target,
		Status: wkdb.ChannelClusterStatusNormal}
}

func TestConfigOrphanMigrationRecoversWithoutSend(t *testing.T) {
	servers, ctx := newConfigRPCServers(t, 3)
	owner := servers[0]
	require.Eventually(t, func() bool {
		return len(owner.cfgServer.AllowVoteAndJoinedOnlineNodes()) == 3
	}, 3*time.Second, 20*time.Millisecond)
	var configs []wkdb.ChannelClusterConfig
	// Cover both reported targets and a target no longer in the cluster. Repair
	// must choose eligible nodes, not blindly restore the old target as a learner.
	for _, target := range []uint64{2, 3, 99} {
		cfg := orphanMigrationConfig(t, owner, target)
		version, err := owner.saveChannelConfig(ctx, cfg)
		require.NoError(t, err)
		cfg.ConfVersion = version
		msg := consistencyMessage(1)
		msg.ChannelID = cfg.ChannelId
		require.NoError(t, owner.db.AppendMessages(cfg.ChannelId, cfg.ChannelType, []wkdb.Message{msg}))
		configs = append(configs, cfg)
	}
	// Discover persisted interrupted expansion through the real background scan;
	// there is no foreground send, read hint, or manual marker cleanup.
	owner.initChannelConfigReconciler()
	owner.configReconciler.start()
	require.Eventually(t, func() bool {
		for _, cfg := range configs {
			current, err := owner.loadConversationConfig(ctx, cfg.ChannelId, cfg.ChannelType)
			if err != nil || len(current.Replicas) != 3 || len(current.Learners) != 0 ||
				current.MigrateFrom != 0 || current.MigrateTo != 0 {
				return false
			}
		}
		return true
	}, 6*time.Second, 50*time.Millisecond)
	for _, cfg := range configs {
		current, err := owner.loadConversationConfig(ctx, cfg.ChannelId, cfg.ChannelType)
		require.NoError(t, err)
		require.ElementsMatch(t, []uint64{1, 2, 3}, current.Replicas)
		require.Greater(t, current.ConfVersion, cfg.ConfVersion)
		for _, peer := range servers {
			messages, err := peer.db.LoadLastMsgs(cfg.ChannelId, cfg.ChannelType, 10)
			require.NoError(t, err)
			require.Len(t, messages, 1)
			require.Equal(t, int64(1), messages[0].MessageID)
			require.Equal(t, uint32(1), messages[0].MessageSeq)
			require.Equal(t, []byte("diagnostic"), messages[0].Payload)
		}
	}
}

func TestConfigOrphanMigrationPreservesOtherTransitions(t *testing.T) {
	for _, tc := range []struct {
		name     string
		replicas []uint64
		learners []uint64
		from, to uint64
	}{
		{"active expansion", []uint64{1}, []uint64{2}, 2, 2},
		{"other learner pending", []uint64{1}, []uint64{3}, 2, 2},
		{"orphan learner", []uint64{1}, []uint64{2}, 0, 0},
		{"leader transfer", []uint64{1, 2}, nil, 1, 2},
		{"replica replacement", []uint64{1}, nil, 1, 2},
		{"different absent endpoints", []uint64{1}, nil, 2, 3},
		{"source only", []uint64{1}, nil, 2, 0},
		{"target only", []uint64{1}, nil, 0, 2},
		{"target already a replica", []uint64{1, 2}, nil, 2, 2},
		{"already full", []uint64{1, 2, 3}, nil, 99, 99},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := wkdb.ChannelClusterConfig{ReplicaMaxCount: 3, Replicas: tc.replicas,
				Learners: tc.learners, MigrateFrom: tc.from, MigrateTo: tc.to}
			before := cfg.Clone()
			changed, err := (&Server{}).joinNewRepliceIfNeed(&cfg)
			require.NoError(t, err)
			require.False(t, changed)
			require.Equal(t, before, cfg)
		})
	}
}

func TestConfigOrphanMigrationConcurrentRepairIsFenced(t *testing.T) {
	servers, ctx := newConfigRPCServers(t, 3)
	owner := servers[0]
	require.Eventually(t, func() bool {
		return len(owner.cfgServer.AllowVoteAndJoinedOnlineNodes()) == 3
	}, 3*time.Second, 20*time.Millisecond)
	cfg := orphanMigrationConfig(t, owner, 2)
	version, err := owner.saveChannelConfig(ctx, cfg)
	require.NoError(t, err)
	cfg.ConfVersion = version
	start, results := make(chan struct{}), make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start
			_, err := owner.maintainChannelConfig(ctx, cfg)
			results <- err
		}()
	}
	close(start)
	winners := 0
	for i := 0; i < 2; i++ {
		err := <-results
		if err == nil {
			winners++
		} else {
			require.ErrorIs(t, err, raft.ErrConfigVersionStale)
		}
	}
	require.Equal(t, 1, winners)
	current, err := owner.loadConversationConfig(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.Equal(t, []uint64{1}, current.Replicas)
	require.ElementsMatch(t, []uint64{2, 3}, current.Learners)
	require.Contains(t, current.Learners, current.MigrateTo)
	require.Equal(t, current.MigrateTo, current.MigrateFrom)
	require.Greater(t, current.ConfVersion, cfg.ConfVersion)
	// A delayed callback from before repair must not erase the new learners.
	_, err = owner.saveChannelConfigTransition(ctx, cfg.ChannelId, cfg.ChannelType, rafttype.Config{
		Version: cfg.ConfVersion, Term: cfg.Term, Leader: cfg.LeaderId, Replicas: []uint64{1, 2},
	})
	require.ErrorIs(t, err, raft.ErrConfigVersionStale)
	after, err := owner.loadConversationConfig(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.Equal(t, current, after)
	require.Empty(t, cfg.Learners, "maintenance must not mutate its input snapshot")
}
