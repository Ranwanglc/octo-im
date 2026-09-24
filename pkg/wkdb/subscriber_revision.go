package wkdb

import (
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"github.com/cockroachdb/pebble"
)

// Keep the authority fence after receipt expiry and channel deletion. A late
// request must never acquire a new source lifecycle just because its receipt
// was collected. This namespace is introduced by subscriber protocol 4.
const recoveryBusinessRevision byte = 12
const recoveryBusinessPage byte = 13

type subscriberRevision struct {
	SnapshotID  string `json:"snapshot_id,omitempty"`
	PageCount   uint32 `json:"page_count,omitempty"`
	Revision    uint64 `json:"revision"`
	Digest      string `json:"digest"`
	OperationID string `json:"operation_id"`
}

func subscriberRevisionKey(channel string, channelType uint8) []byte {
	return recoveryChannelKey(recoveryBusinessRevision, channel, channelType, "")
}

// Called under the source partition/channel locks. Admission and the resulting
// revision update share the source membership/intent/receipt transaction.
func (wk *wukongDB) checkSubscriberRevision(db *pebble.DB, o SubscriberOperation) (string, error) {
	var current subscriberRevision
	found, err := recoveryRead(db, subscriberRevisionKey(o.ChannelID, o.ChannelType), &current)
	if err != nil || !found {
		return "", err
	}
	if o.Mode != "reconcile" {
		return "managed_channel", nil
	}
	if o.BusinessRevision < current.Revision {
		return "stale_revision", nil
	}
	if o.BusinessRevision == current.Revision {
		if o.PageCount != current.PageCount || o.SnapshotID != current.SnapshotID {
			return "revision_conflict", nil
		}
		if o.PageCount == 0 {
			if o.Digest() != current.Digest || o.OperationID != current.OperationID {
				return "revision_conflict", nil
			}
		} else {
			var digest string
			found, err := recoveryRead(db, subscriberPageKey(o), &digest)
			if err != nil {
				return "", err
			}
			if found && digest != o.Digest() {
				return "revision_conflict", nil
			}
		}
	}
	return "", nil
}

// SubscriberBusinessManaged also guards legacy metadata/delta commands during
// replay. Authority survives channel deletion and receipt collection.
func (wk *wukongDB) SubscriberBusinessManaged(channel string, channelType uint8) (bool, error) {
	var revision subscriberRevision
	return recoveryRead(wk.channelDb(channel, channelType), subscriberRevisionKey(channel, channelType), &revision)
}

func (o SubscriberOperation) snapshotContains(uid string) bool {
	return o.PageCount == 0 || (uid >= o.RangeStart && (o.RangeEnd == "" || uid < o.RangeEnd))
}

func (o SubscriberOperation) validateSnapshotPage() error {
	if o.PageCount == 0 {
		if o.PageIndex != 0 || o.SnapshotID != "" || o.RangeStart != "" || o.RangeEnd != "" {
			return errors.New("invalid snapshot page")
		}
		return nil
	}
	digest, err := hex.DecodeString(o.SnapshotID)
	if o.Mode != "reconcile" || err != nil || len(digest) != 32 || o.PageIndex >= o.PageCount ||
		(o.PageIndex == 0 && o.RangeStart != "") || (o.PageIndex > 0 && o.RangeStart == "") ||
		(o.PageIndex == o.PageCount-1 && o.RangeEnd != "") || (o.PageIndex < o.PageCount-1 && o.RangeEnd == "") ||
		(o.RangeEnd != "" && o.RangeStart >= o.RangeEnd) || len(o.RangeStart) > 1024 || len(o.RangeEnd) > 1024 {
		return errors.New("invalid snapshot page bounds")
	}
	for _, list := range [][]string{o.UIDs, o.DenyUIDs} {
		for _, uid := range list {
			if !o.snapshotContains(uid) {
				return errors.New("subscriber outside snapshot page")
			}
		}
	}
	return nil
}

func subscriberPageKey(o SubscriberOperation) []byte {
	return recoveryChannelKey(recoveryBusinessPage, o.ChannelID, o.ChannelType, fmt.Sprintf("%08x", o.PageIndex))
}

func (wk *wukongDB) stageSubscriberRevision(db *pebble.DB, batch *pebble.Batch, o SubscriberOperation) error {
	var current subscriberRevision
	_, err := recoveryRead(db, subscriberRevisionKey(o.ChannelID, o.ChannelType), &current)
	if err != nil {
		return err
	}
	if o.BusinessRevision > current.Revision {
		prefix := recoveryKey(recoveryBusinessPage, o.ChannelID, string([]byte{o.ChannelType}))
		if err := batch.DeleteRange(prefix, append(append([]byte(nil), prefix...), 0xff), wk.noSync); err != nil {
			return err
		}
	}
	if o.PageCount > 0 {
		if err := recoverySet(batch, subscriberPageKey(o), o.Digest()); err != nil {
			return err
		}
	}
	return recoverySet(batch, subscriberRevisionKey(o.ChannelID, o.ChannelType), subscriberRevision{
		Revision: o.BusinessRevision, Digest: o.Digest(), OperationID: o.OperationID, SnapshotID: o.SnapshotID, PageCount: o.PageCount,
	})
}

// Each page replaces only its lexical UID range. Old members outside a page
// survive until their own page applies; a newer revision fences ALL older
// pages immediately. HTTP completion requires every page and target barrier.
func (wk *wukongDB) stageSnapshotDenylist(batch *pebble.Batch, o SubscriberOperation, at time.Time) error {
	if o.PageCount == 0 {
		o.Mode, o.UIDs = "deny_set", o.DenyUIDs
		return wk.stageRecoveryDenylist(batch, o, at)
	}
	existing, err := wk.recoveryDenylist(o.ChannelID, o.ChannelType)
	if err != nil {
		return err
	}
	wanted := make(map[string]bool, len(o.DenyUIDs))
	for _, uid := range o.DenyUIDs {
		wanted[uid] = true
	}
	delta := 0
	for _, member := range existing {
		if !o.snapshotContains(member.Uid) {
			continue
		}
		if wanted[member.Uid] {
			delete(wanted, member.Uid)
			continue
		}
		if err := wk.removeDenylist(o.ChannelID, o.ChannelType, member, batch); err != nil {
			return err
		}
		delta--
	}
	for uid := range wanted {
		if err := wk.writeDenylist(o.ChannelID, o.ChannelType, Member{Id: key.HashWithString(uid), Uid: uid, CreatedAt: &at, UpdatedAt: &at}, batch); err != nil {
			return err
		}
		delta++
	}
	if delta != 0 {
		pk, err := wk.getChannelPrimaryKey(o.ChannelID, o.ChannelType)
		if err != nil {
			return err
		}
		return wk.incChannelInfoDenylistCount(pk, delta, batch)
	}
	return nil
}
