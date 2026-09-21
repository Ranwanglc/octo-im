package service

import (
	"github.com/WuKongIM/WuKongIM/pkg/wkdb/key"
	"sync"
)

var subscriberTagLocks [64]sync.Mutex
var subscriberTagVersions [64]uint64 // accessed under the matching lock

// SubscriberTagVersion is captured before reading membership or calling RPC.
// A collision only retries a fill; it never allows a stale tag to be published.
func SubscriberTagVersion(ch string, tp uint8) uint64 {
	i := key.ChannelToNum(ch, tp) % 64
	subscriberTagLocks[i].Lock()
	defer subscriberTagLocks[i].Unlock()
	return subscriberTagVersions[i]
}

func PublishSubscriberTag(ch string, tp uint8, version uint64, tagKey string) bool {
	i := key.ChannelToNum(ch, tp) % 64
	subscriberTagLocks[i].Lock()
	defer subscriberTagLocks[i].Unlock()
	if subscriberTagVersions[i] != version {
		return false
	}
	TagManager.SetChannelTag(ch, tp, tagKey)
	return true
}

// LockSubscriberTag serializes an authoritative tag fill with invalidation.
// Invalidation after a source change cannot be overwritten by a stale fill.
func LockSubscriberTag(ch string, tp uint8) func() {
	m := &subscriberTagLocks[key.ChannelToNum(ch, tp)%64]
	m.Lock()
	return m.Unlock
}

func InvalidateSubscriberTag(ch string, tp uint8) {
	unlock := LockSubscriberTag(ch, tp)
	defer unlock()
	subscriberTagVersions[key.ChannelToNum(ch, tp)%64]++
	tag := TagManager.GetChannelTag(ch, tp)
	TagManager.RemoveChannelTag(ch, tp)
	if tag != "" {
		TagManager.RemoveTag(tag)
	}
}
