package cluster

import (
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	rafttype "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wklog"
	"github.com/stretchr/testify/require"
)

func TestSlotApplyLogsReturnsErrorWithoutPanicking(t *testing.T) {
	s := &Server{
		store: store.New(store.NewOptions()),
		opts:  NewOptions(WithSubscriberRecoveryEnabled(true)),
		Log:   wklog.NewWKLog("cluster-test"),
	}
	var applyErr error
	require.NotPanics(t, func() {
		applyErr = s.slotApplyLogs(1, []rafttype.Log{{Index: 1, Data: []byte{0}}})
	})
	require.Error(t, applyErr)
}

func TestSlotApplyLogsPreservesFailFastWhenRecoveryDisabled(t *testing.T) {
	s := &Server{
		store: store.New(store.NewOptions()),
		opts:  NewOptions(),
		Log:   wklog.NewWKLog("cluster-test"),
	}
	require.Panics(t, func() {
		_ = s.slotApplyLogs(1, []rafttype.Log{{Index: 1, Data: []byte{0}}})
	})
}
