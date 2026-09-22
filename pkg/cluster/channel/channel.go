package channel

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/raft"
	"github.com/WuKongIM/WuKongIM/pkg/raft/raftgroup"
	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	rafttype "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/WuKongIM/WuKongIM/pkg/wkutil"
	"go.uber.org/zap"
)

type Channel struct {
	*raft.Node
	s *Server
	wklog.Log
	rg         *raftgroup.RaftGroup
	channelKey string
	// Owned by rg.Do; retained if the caller expires between Step and Ready.
	needsConfigResume bool
}

func createChannel(cfg wkdb.ChannelClusterConfig, s *Server, rg *raftgroup.RaftGroup) (*Channel, error) {
	channelKey := wkutil.ChannelToKey(cfg.ChannelId, cfg.ChannelType)
	ch := &Channel{
		s:          s,
		Log:        wklog.NewWKLog("channel"),
		rg:         rg,
		channelKey: channelKey,
	}

	state, err := s.storage.GetState(cfg.ChannelId, cfg.ChannelType)
	if err != nil {
		ch.Error("get state failed", zap.String("channelKey", channelKey), zap.Error(err))
		return nil, err
	}

	lastLogStartIndex, err := s.storage.GetTermStartIndex(channelKey, state.LastTerm)
	if err != nil {
		ch.Error("get last term failed", zap.String("channelKey", channelKey), zap.Error(err))
		return nil, err
	}

	ch.Node = raft.NewNode(
		lastLogStartIndex,
		state,
		raft.NewOptions(
			raft.WithKey(channelKey),
			raft.WithSaveHardState(func(state types.HardState) error { return s.storage.db.SaveRaftHardState(channelKey, state) }),
			raft.WithAutoSuspend(true),
			raft.WithAutoDestory(true),
			raft.WithNodeId(s.opts.NodeId),
			raft.WithDestoryAfterIdleTick(s.opts.DestoryAfterIdleTick),
		))

	return ch, nil
}

func (ch *Channel) switchConfig(cfg rafttype.Config) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return ch.applyConfig(ctx, cfg)
}

var ErrConfigConflict = errors.New("channel configuration conflicts with runtime")

// Compare and step on the owner: comparing the creation-time metadata snapshot
// misses later updates and replaying ConfChange resets replica sync progress.
func (ch *Channel) applyConfig(ctx context.Context, cfg rafttype.Config) error {
	cfg = cfg.Clone()
	err := ch.rg.Do(ctx, ch.channelKey, func(r raftgroup.IRaft) error {
		if r != ch {
			return raftgroup.ErrRaftNotExist
		}
		current := ch.Config()
		if cfg.Version < current.Version {
			return fmt.Errorf("%w: incoming=%d runtime=%d", raft.ErrConfigVersionStale, cfg.Version, current.Version)
		}
		if cfg.Term != 0 && cfg.Term < current.Term {
			return fmt.Errorf("%w: incoming term=%d runtime term=%d", ErrConfigConflict, cfg.Term, current.Term)
		}
		if sameChannelConfig(current, cfg) {
			return nil
		}
		if cfg.Version != 0 && cfg.Version == current.Version {
			return fmt.Errorf("%w: incoming=%+v runtime=%+v", ErrConfigConflict, cfg, current)
		}
		if err := ch.Step(rafttype.Event{Type: rafttype.ConfChange, Config: cfg}); err != nil {
			return err
		}
		ch.needsConfigResume = true
		return nil
	})
	if err != nil {
		return err
	}
	// Keep this owner hop after ConfChange: Ready must persist a newly
	// selected term before ResumeReplication can certify the stored tail.
	return ch.rg.Do(ctx, ch.channelKey, func(r raftgroup.IRaft) error {
		if r != ch {
			return raftgroup.ErrRaftNotExist
		}
		if ch.needsConfigResume && len(ch.Config().Replicas) == 1 {
			ch.ResumeReplication()
		}
		ch.needsConfigResume = false
		return nil
	})
}

func sameChannelConfig(a, b rafttype.Config) bool {
	return a.Version == b.Version && a.Term == b.Term && a.Leader == b.Leader && a.Role == b.Role &&
		a.MigrateFrom == b.MigrateFrom && a.MigrateTo == b.MigrateTo &&
		slices.Equal(a.Replicas, b.Replicas) && slices.Equal(a.Learners, b.Learners)
}

func channelConfigToRaftConfig(currentNodeId uint64, cfg wkdb.ChannelClusterConfig) rafttype.Config {

	var role rafttype.Role
	if wkutil.ArrayContainsUint64(cfg.Learners, currentNodeId) {
		role = rafttype.RoleLearner
	} else {
		if cfg.LeaderId == currentNodeId {
			role = rafttype.RoleLeader
		} else {
			role = rafttype.RoleFollower
		}
	}

	return types.Config{
		MigrateFrom: cfg.MigrateFrom,
		MigrateTo:   cfg.MigrateTo,
		Replicas:    cfg.Replicas,
		Learners:    cfg.Learners,
		Term:        cfg.Term,
		Leader:      cfg.LeaderId,
		Role:        role,
		Version:     cfg.ConfVersion,
	}
}
