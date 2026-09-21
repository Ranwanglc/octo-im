package store

import (
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

type progressFailureDB struct {
	wkdb.DB
	failIndex uint64
}

func (d *progressFailureDB) SetSlotAppliedIndex(slot uint32, index uint64) error {
	if index == d.failIndex {
		return errors.New("injected progress fsync failure")
	}
	return d.DB.SetSlotAppliedIndex(slot, index)
}

func applyTestLog(t *testing.T, index uint64, tp CMDType, body []byte) types.Log {
	t.Helper()
	data, err := NewCMD(tp, body).Marshal()
	require.NoError(t, err)
	return types.Log{Index: index, Data: data}
}

func TestSlotApplyRetriesCommittedPrefixAndCrashGap(t *testing.T) {
	s, _ := recoveryStore(t)
	db := s.DB()
	wrapped := &progressFailureDB{DB: db, failIndex: 2}
	s.wdb = wrapped
	logs := []types.Log{
		applyTestLog(t, 1, CMDAddDenylist, EncodeMembers("g", 2, []wkdb.Member{{Uid: "a"}})),
		applyTestLog(t, 2, CMDRemoveDenylist, EncodeChannelUids("g", 2, []string{"a"})),
		applyTestLog(t, 3, CMDAddDenylist, EncodeMembers("g", 2, []wkdb.Member{{Uid: "b"}})),
	}
	require.Error(t, s.ApplySlotLogs(0, logs))
	index, err := db.SlotAppliedIndex(0)
	require.NoError(t, err)
	require.Equal(t, uint64(1), index)
	// A new Store has no process-local replay memory. The durable prefix and
	// the idempotent current entry must be sufficient to finish this batch.
	restarted := New(NewOptions(WithDB(db)))
	require.NoError(t, restarted.ApplySlotLogs(0, logs))
	require.NoError(t, restarted.ApplySlotLogs(0, logs))
	members, err := db.GetDenylist("g", 2)
	require.NoError(t, err)
	require.Len(t, members, 1)
	require.Equal(t, "b", members[0].Uid)
	channel, err := db.GetChannel("g", 2)
	require.NoError(t, err)
	require.EqualValues(t, 1, channel.DenylistCount)
}

func TestSlotApplyEventMarkerSurvivesProgressFailure(t *testing.T) {
	s, _ := recoveryStore(t)
	db := s.DB()
	s.wdb = &progressFailureDB{DB: db, failIndex: 2}
	event := func(id, payload string) *wkdb.MessageEvent {
		return &wkdb.MessageEvent{ChannelId: "g", ChannelType: 2, ClientMsgNo: "m", EventID: id, EventType: wkdb.EventTypeStreamDelta, Payload: []byte(payload)}
	}
	logs := []types.Log{
		applyTestLog(t, 1, CMDAppendMessageEvent, EncodeCMDMessageEvent(event("a", "A"), 1)),
		applyTestLog(t, 2, CMDAppendMessageEvent, EncodeCMDMessageEvent(event("b", "B"), 1)),
		applyTestLog(t, 3, CMDAppendMessageEvent, EncodeCMDMessageEvent(event("c", "C"), 1)),
	}
	require.Error(t, s.ApplySlotLogs(0, logs))
	// Change LastEventID through another writer to prove last-event-only
	// deduplication does not protect the interrupted log's commit gap.
	_, _, err := db.AppendMessageEventWithState(event("other", "X"))
	require.NoError(t, err)
	s.wdb = db
	require.NoError(t, s.ApplySlotLogs(0, logs))
	state, err := db.GetMessageEventState("g", 2, "m", "main")
	require.NoError(t, err)
	require.Equal(t, "ABXC", string(state.SnapshotPayload))
	require.EqualValues(t, 4, state.LastMsgEventSeq)
}

func TestSlotApplyUnknownCommandNeverAdvances(t *testing.T) {
	s, _ := recoveryStore(t)
	err := s.ApplySlotLogs(0, []types.Log{applyTestLog(t, 1, CMDType(65535), nil)})
	var permanent *PermanentApplyError
	require.ErrorAs(t, err, &permanent)
	index, err := s.DB().SlotAppliedIndex(0)
	require.NoError(t, err)
	require.Zero(t, index)
}

func TestSlotApplyRecoveryPrefixCannotUndoInterruptedLegacyDelete(t *testing.T) {
	s, slots := recoveryStore(t)
	db := s.DB()
	effect := wkdb.ConversationEffect{UID: "a", ChannelID: "g", ChannelType: 2, Version: 1, ConversationID: 7, CreatedAt: time.Now().UnixNano()}
	body, err := json.Marshal([]wkdb.ConversationEffect{effect})
	require.NoError(t, err)
	logs := []types.Log{
		applyTestLog(t, 1, CMDConversationEffects, body),
		applyTestLog(t, 2, CMDDeleteConversation, EncodeCMDDeleteConversation("a", "g", 2)),
	}
	s.wdb = &progressFailureDB{DB: db, failIndex: 2}
	slot := slots.GetSlotId("a")
	require.Error(t, s.ApplySlotLogs(slot, logs))
	index, err := db.SlotAppliedIndex(slot)
	require.NoError(t, err)
	require.Zero(t, index, "the recovery prefix shares its next durable checkpoint")
	s.wdb = db
	require.NoError(t, s.ApplySlotLogs(slot, logs))
	_, err = db.GetConversation("a", "g", 2)
	require.ErrorIs(t, err, wkdb.ErrNotFound, "replayed recovery cannot resurrect the legacy deletion")
}
