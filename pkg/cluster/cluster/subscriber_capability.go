package cluster

import (
	"context"
	"fmt"

	"github.com/WuKongIM/WuKongIM/pkg/cluster/node/types"
	"github.com/WuKongIM/WuKongIM/pkg/cluster/store"
	rafttypes "github.com/WuKongIM/WuKongIM/pkg/raft/types"
	"github.com/WuKongIM/WuKongIM/pkg/wkdb"
	"github.com/WuKongIM/WuKongIM/pkg/wkserver/proto"
	"golang.org/x/sync/errgroup"
)

const subscriberProtocolVersion uint32 = 2
const subscriberCapabilityPath = "/rpc/cluster/subscriber-capability"

func (s *Server) checkSubscriberProposal(ctx context.Context, _ uint32, reqs rafttypes.ProposeReqSet) error {
	needed := false
	for _, req := range reqs {
		cmd := &store.CMD{}
		if err := cmd.Unmarshal(req.Data); err != nil {
			return err
		}
		switch cmd.CmdType {
		case store.CMDSubscriberOperation, store.CMDConversationEffects, store.CMDSubscriberCheckpoint:
			needed = true
		}
	}
	if !needed {
		return nil
	}
	version := s.NodeVersion()
	err := checkSubscriberNodes(ctx, s.Nodes(), s.opts.ConfigOptions.NodeId, s.db, func(ctx context.Context, id uint64) (bool, error) {
		response, err := s.RequestWithContext(ctx, id, subscriberCapabilityPath, nil)
		if err != nil {
			return false, err
		}
		return response.Status == proto.StatusOK && string(response.Body) == "2", nil
	})
	if err != nil {
		return err
	}
	if version != s.NodeVersion() {
		return fmt.Errorf("cluster membership changed during subscriber capability check")
	}
	return ctx.Err()
}

// Check every configured node, including offline replicas and future migration
// targets. A previous durable handshake permits recovery while that node is
// unavailable; a reachable node explicitly reporting an old protocol is always
// rejected. Downgrading a node after activation is not supported.
func checkSubscriberNodes(ctx context.Context, nodes []*types.Node, local uint64, db wkdb.SlotApplyDB, probe func(context.Context, uint64) (bool, error)) error {
	if len(nodes) == 0 {
		return fmt.Errorf("cluster membership unavailable")
	}
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(8)
	for _, node := range nodes {
		if node.Id == local {
			continue
		}
		node := node
		g.Go(func() error {
			identity := fmt.Sprintf("%d/%s/%d", node.Id, node.ClusterAddr, node.CreatedAt)
			known, err := db.ClusterCapability(identity)
			if err != nil {
				return err
			}
			if !node.Online && known >= subscriberProtocolVersion {
				return nil
			}
			supported, err := probe(ctx, node.Id)
			if err != nil && known >= subscriberProtocolVersion {
				return nil
			}
			if err != nil {
				return fmt.Errorf("node %d subscriber capability unconfirmed: %w", node.Id, err)
			}
			if !supported {
				if known != 0 {
					if err := db.SetClusterCapability(identity, 0); err != nil {
						return err
					}
				}
				return fmt.Errorf("node %d must be upgraded before subscriber recovery can activate", node.Id)
			}
			if known < subscriberProtocolVersion {
				return db.SetClusterCapability(identity, subscriberProtocolVersion)
			}
			return nil
		})
	}
	return g.Wait()
}
