package raftgroup

import (
	"context"
	"errors"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
)

var ErrReadStateChanged = errors.New("raft changed after metadata read")

// ProposeAfterRead admits a local proposal only if no log, leadership or config
// change occurred since an applied metadata snapshot. It never forwards stale
// decisions to a new leader. The ordinary command encoding remains unchanged.
func (rg *RaftGroup) ProposeAfterRead(ctx context.Context, key string, before ReadState, id uint64, data []byte) (uint64, error) {
	r := rg.GetRaft(key)
	if r == nil {
		return 0, ErrReadStateChanged
	}
	// Coordinate index allocation with existing generic proposal callers.
	locker, ok := r.(interface{ TryLock() bool })
	if !ok || !locker.TryLock() {
		return 0, ErrReadStateChanged
	}
	var index uint64
	err := rg.Do(ctx, key, func(current IRaft) error {
		if current != r || !before.Exists || !before.Ready || !r.IsLeader() ||
			r.LeaderId() != before.LeaderID || r.Config().Term != before.Term ||
			r.Config().Version != before.ConfigVersion || r.AppliedIndex() != before.AppliedIndex ||
			r.LastLogIndex() != before.AppliedIndex || r.CommittedIndex() != before.AppliedIndex {
			return ErrReadStateChanged
		}
		if ready, ok := r.(interface{ LeaderReadReady() bool }); !ok || !ready.LeaderReadReady() {
			return ErrReadStateChanged
		}
		index = r.LastLogIndex() + 1
		if index == 0 {
			return errors.New("raft log index exhausted")
		}
		return r.Step(types.Event{Type: types.Propose, Logs: []types.Log{{Id: id, Index: index, Term: before.Term, Data: data}}})
	})
	r.Unlock()
	if err != nil {
		return 0, err
	}
	// Once admitted, errors are outcome-unknown, never ErrReadStateChanged.
	// Callers must reload metadata instead of replaying across newer changes.
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		state, err := rg.ReadLeaderState(ctx, key)
		if err != nil {
			return index, err
		}
		if !state.Exists || state.LeaderID != before.LeaderID || state.Term != before.Term {
			return index, types.ErrNotLeader
		}
		if state.AppliedIndex >= index {
			return index, nil
		}
		select {
		case <-ctx.Done():
			return index, ctx.Err()
		case <-ticker.C:
		}
	}
}
