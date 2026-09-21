package service

import (
	"github.com/stretchr/testify/require"
	"testing"
)

type recoveryTagManager struct {
	ITagManager
	tag string
}

func (m *recoveryTagManager) GetChannelTag(string, uint8) string          { return m.tag }
func (m *recoveryTagManager) SetChannelTag(_ string, _ uint8, tag string) { m.tag = tag }
func (m *recoveryTagManager) RemoveChannelTag(string, uint8)              { m.tag = "" }
func (m *recoveryTagManager) RemoveTag(string)                            {}

func TestSubscriberTagInvalidationRejectsDelayedFill(t *testing.T) {
	old := TagManager
	defer func() { TagManager = old }()
	m := &recoveryTagManager{}
	TagManager = m
	beforeRead := SubscriberTagVersion("g", 2)
	InvalidateSubscriberTag("g", 2)
	require.False(t, PublishSubscriberTag("g", 2, beforeRead, "stale"))
	require.Empty(t, m.tag)
	require.True(t, PublishSubscriberTag("g", 2, SubscriberTagVersion("g", 2), "fresh"))
	require.Equal(t, "fresh", m.tag)
}
