package cluster

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	rafttype "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"github.com/stretchr/testify/require"
)

func TestConfigRecoveryRepairsDurableTermAndReads(t *testing.T) {
	s, ctx := newConversationConsistencyServer(t, nil)
	cfg, err := s.GetOrCreateChannelClusterConfigFromSlotLeader("analysis-channel", 2)
	require.NoError(t, err)
	key := wkutil.ChannelToKey(cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, s.db.AppendMessages(cfg.ChannelId, cfg.ChannelType, []wkdb.Message{consistencyMessage(1)}))
	require.NoError(t, s.db.SaveRaftHardState(key, rafttype.HardState{Term: 5, Vote: 2}))
	// Reopening after a peer adopted a higher term must retain that durable term.
	require.NoError(t, s.channelServer.WakeLeaderIfNeed(cfg))
	s.hintChannelConfig(cfg.ChannelId, cfg.ChannelType)
	require.Eventually(t, func() bool {
		current, err := s.loadConversationConfig(ctx, cfg.ChannelId, cfg.ChannelType)
		if err != nil || current.Term <= 5 || current.ConfVersion <= cfg.ConfVersion {
			return false
		}
		state, err := s.channelServer.ReadLeaderState(ctx, cfg.ChannelId, cfg.ChannelType)
		return err == nil && conversationStateReady(state, current)
	}, 5*time.Second, 10*time.Millisecond)
	seq, err := s.GetChannelLastMessageSeq(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.Equal(t, uint64(1), seq, "recover the complete stored tail without a new send")
	state, err := s.db.RaftHardState(key)
	require.NoError(t, err)
	require.Greater(t, state.Term, uint32(5))
	require.NotEqual(t, uint64(2), state.Vote, "the previous term's vote must not survive the new term")
	msg := consistencyMessage(2)
	msg.MessageSeq = 0
	data, err := msg.Marshal()
	require.NoError(t, err)
	resps, err := s.channelServer.ProposeBatchUntilAppliedTimeoutForLocal(ctx, cfg.ChannelId, cfg.ChannelType, rafttype.ProposeReqSet{{Id: 2, Data: data}})
	require.NoError(t, err)
	require.Equal(t, uint64(2), resps[0].Index)
	seq, err = s.GetChannelLastMessageSeq(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.Equal(t, uint64(2), seq)
}

func TestConfigDelayedRoleCallbackCannotOverwriteNewMetadata(t *testing.T) {
	s, ctx := newConversationConsistencyServer(t, nil)
	cfg, err := s.GetOrCreateChannelClusterConfigFromSlotLeader("callback", 2)
	require.NoError(t, err)
	stale := rafttype.Config{Version: cfg.ConfVersion, Term: cfg.Term, Leader: cfg.LeaderId, Replicas: []uint64{1, 2}}
	next := cfg.Clone()
	next.Term++
	next.Learners = []uint64{3}
	version, err := s.saveChannelConfig(ctx, next)
	require.NoError(t, err)
	require.ErrorIs(t, s.onSaveChannelConfig(cfg.ChannelId, cfg.ChannelType, stale), raft.ErrConfigVersionStale)
	current, err := s.loadConversationConfig(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.Equal(t, version, current.ConfVersion)
	require.Equal(t, next.Term, current.Term)
	require.Equal(t, next.Learners, current.Learners)
	require.Equal(t, next.Replicas, current.Replicas)
}

type configDecisionDB struct {
	wkdb.DB
	armed     atomic.Bool
	afterRead func()
}

func (d *configDecisionDB) GetChannelClusterConfig(id string, typ uint8) (wkdb.ChannelClusterConfig, error) {
	cfg, err := d.DB.GetChannelClusterConfig(id, typ)
	if d.armed.CompareAndSwap(true, false) {
		d.afterRead()
	}
	return cfg, err
}

func TestConfigSaveRejectsChangeBetweenReadAndAdmission(t *testing.T) {
	var db *configDecisionDB
	s, ctx := newConversationConsistencyServer(t, func(s *Server) {
		db = &configDecisionDB{DB: s.db}
		s.db = db
	})
	s.configReconciler.stop()
	cfg, err := s.GetOrCreateChannelClusterConfigFromSlotLeader("race", 2)
	require.NoError(t, err)
	newer := cfg.Clone()
	newer.Term++
	var version uint64
	db.afterRead = func() {
		var err error
		version, err = s.saveChannelConfig(ctx, newer)
		require.NoError(t, err)
	}
	db.armed.Store(true)
	_, err = s.saveChannelConfig(ctx, cfg)
	require.ErrorIs(t, err, raft.ErrConfigVersionStale)
	current, err := s.loadConversationConfig(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.Equal(t, version, current.ConfVersion)
	require.Equal(t, newer.Term, current.Term)
}

func TestConfigReconcileExpiredRPCDoesNotRequirePeer(t *testing.T) {
	s := &Server{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, s.requestChannelConfigReconcile(ctx, consistencyConfig()), context.Canceled)
}
