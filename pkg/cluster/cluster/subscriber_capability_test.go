package cluster

import (
	"context"
	"errors"
	"testing"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/stretchr/testify/require"
)

func TestSubscriberCapabilityRequiresEveryNodeAndSurvivesOutage(t *testing.T) {
	dir := t.TempDir()
	open := func() wkdb.DB {
		db := wkdb.NewWukongDB(wkdb.NewOptions(wkdb.WithDir(dir), wkdb.WithShardNum(1), wkdb.WithMemTableSize(1<<20)))
		require.NoError(t, db.Open())
		return db
	}
	db := open()
	nodes := []*types.Node{{Id: 1}, {Id: 2, ClusterAddr: "node2", Online: true}}
	old := func(context.Context, uint64) (bool, error) { return false, nil }
	good := func(context.Context, uint64) (bool, error) { return true, nil }
	down := func(context.Context, uint64) (bool, error) { return false, errors.New("unreachable") }
	require.ErrorContains(t, checkSubscriberNodes(context.Background(), nodes, 1, db, old), "upgraded")
	require.ErrorContains(t, checkSubscriberNodes(context.Background(), nodes, 1, db, down), "unconfirmed")
	require.NoError(t, checkSubscriberNodes(context.Background(), nodes, 1, db, good))
	require.NoError(t, db.Close())
	db = open()
	defer db.Close()
	require.NoError(t, checkSubscriberNodes(context.Background(), nodes, 1, db, down))
	require.ErrorContains(t, checkSubscriberNodes(context.Background(), nodes, 1, db, old), "upgraded")
	require.ErrorContains(t, checkSubscriberNodes(context.Background(), nodes, 1, db, down), "unconfirmed")
	nodes[1].ClusterAddr = "replacement"
	require.ErrorContains(t, checkSubscriberNodes(context.Background(), nodes, 1, db, down), "unconfirmed")
}

func TestSubscriberCapabilityJoinEncodingPreservesLegacyPrefix(t *testing.T) {
	r := ClusterJoinReq{NodeId: 2, ServerAddr: "node2", SubscriberProtocol: subscriberProtocolVersion}
	b, err := r.Marshal()
	require.NoError(t, err)
	var decoded ClusterJoinReq
	require.NoError(t, decoded.Unmarshal(b))
	require.Equal(t, r, decoded)
	var legacy ClusterJoinReq
	require.NoError(t, legacy.Unmarshal(b[:len(b)-4]))
	require.Zero(t, legacy.SubscriberProtocol)
}
