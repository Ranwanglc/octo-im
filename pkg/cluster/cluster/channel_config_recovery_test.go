package cluster

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/channel"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
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

type callbackResumeDB struct {
	wkdb.DB
	fail   atomic.Bool
	failed chan struct{}
}

func (d *callbackResumeDB) SaveRaftHardState(key string, state rafttype.HardState) error {
	if d.fail.Load() {
		select {
		case d.failed <- struct{}{}:
		default:
		}
		return errors.New("temporary callback persistence failure")
	}
	return d.DB.SaveRaftHardState(key, state)
}
func TestConfigRoleCallbackWaitsForResume(t *testing.T) {
	var db *callbackResumeDB
	s, ctx := newConversationConsistencyServer(t, func(s *Server) {
		db = &callbackResumeDB{DB: s.db, failed: make(chan struct{}, 1)}
		s.channelServer = channel.NewServer(channel.NewOptions(channel.WithNodeId(1), channel.WithGroupCount(1),
			channel.WithTransport(s.opts.ChannelTransport), channel.WithSlot(s.slotServer), channel.WithNode(s.cfgServer),
			channel.WithCluster(s), channel.WithDB(db), channel.WithRPC(s.rpcClient), channel.WithOnSaveConfig(s.onSaveChannelConfig)))
		s.store = store.New(store.NewOptions(store.WithNodeId(1), store.WithSlot(s.slotServer), store.WithChannel(s.channelServer), store.WithDB(s.db)))
	})
	s.configReconciler.stop()
	cfg, err := s.GetOrCreateChannelClusterConfigFromSlotLeader("callback-resume", 2)
	require.NoError(t, err)
	require.NoError(t, s.channelServer.WakeLeaderIfNeed(cfg))
	db.fail.Store(true)
	defer db.fail.Store(false)
	done := make(chan error, 1)
	go func() {
		done <- s.onSaveChannelConfig(cfg.ChannelId, cfg.ChannelType, rafttype.Config{Version: cfg.ConfVersion, Term: cfg.Term + 1, Leader: cfg.LeaderId, Replicas: cfg.Replicas})
	}()
	select {
	case <-db.failed:
	case <-ctx.Done():
		t.Fatal("callback did not reach hard-state persistence")
	}
	select {
	case err := <-done:
		t.Fatalf("callback returned terminal error after committed metadata: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	saved, err := s.loadConversationConfig(ctx, cfg.ChannelId, cfg.ChannelType)
	require.NoError(t, err)
	require.Greater(t, saved.ConfVersion, cfg.ConfVersion)
	require.Equal(t, cfg.Term+1, saved.Term)
	db.fail.Store(false)
	select {
	case err := <-done:
		require.NoError(t, err, "SaveConfig must receive success after transient persistence recovery")
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	require.NoError(t, s.ValidateLocalChannelRead(ctx, saved))
}
