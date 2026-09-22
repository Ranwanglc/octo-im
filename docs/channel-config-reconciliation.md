# 扩容后的频道配置同步与会话 503

## 问题与修复范围

在 `f00056b083d54511141bdc442e61281a11ba3c2c` 上，单节点已有频道扩容到三节点后，槽 owner 可能迁走，频道 leader 仍留在原节点。下一次发送触发 `joinNewRepliceIfNeed`，learner 配置成功保存到槽数据库，但已有频道 leader 的快速提案路径不会重新加载配置，导致 DB version=4、runtime version=3。

会话读取要求两者的 leader、term、version 一致，因此 `/conversation/sync` 持续返回 503。并行读取中一个检查失败会取消其他 RPC，日志中的 `conversationConfig/v2 ... context canceled` 容易掩盖真正的版本落后。相关路由在所有节点均有注册。

`v2.2.5-20260422-fix1`（`02cf8cd19877e307a31cc13412155579a36bf0a4`）同样可能保留这两个不同版本，但没有新分支的严格读取检查。修复恢复完整会话的可用性，同时保留检查；故障、选举和收敛期间仍允许暂态 503。

## 配置如何收敛

- 存储成功回调只提交频道标识，不执行网络请求，不等待 Raft。worker 再经过槽 applied 屏障读取当前配置，不把回调时间当成整个槽已应用完成。
- 当前槽 owner 分发配置；频道 leader 上的读取提示也可直接触发本地核对。每次重试重新查询当前 owner、配置和目标 leader。
- 内部 `/rpc/channel/reconcileConfig/v1` 接收标识和观察到的版本，重新读取权威元数据，返回 `applied`、`dormant`、`superseded` 或 `retry`。不支持该协议、错误身份、过期版本均不能当成同步成功。
- 运行态比较和 `ConfChange` 在同一个 Raft owner 操作中完成。拒绝旧版本、相同非零版本的不同内容，以及落后的 term。相同配置不重置副本同步状态；删除创建时的陈旧配置比较依据。保留 Ready 持久化之后再恢复单副本复制的顺序。
- 普通休眠频道保持休眠。有 learner 或迁移任务的休眠频道，由配置指定的 leader 自动唤醒以完成迁移。读取遇到版本落后只提交提示并继续返回可重试错误。
- 不改变正常发送的快速路径，不增加逐条发送必需的远程配置查询，不改变存储格式。

## 有界维护与恢复

每节点 4 个 worker，最多保留 4096 个不同频道，同频道只有一个在途任务。新提示有代次，旧请求完成不能清除处理期间到达的新提示。每次处理预算 2 秒，失败从 100 毫秒退避到 2 秒，另加最多 25% 抖动。

数据库是恢复依据。启动和集群配置改变会触发扫描；每 100 毫秒最多取 128 条配置，当前节点只调度自己拥有的槽。一轮扫描结束后等待 30 秒再开始下一轮，以发现游标之前的变化。队列满时触发扫描，游标不越过未入队的行。满队列中的失败任务会让出空间，由后续完整扫描重新发现，避免不可达频道挡住后面的健康频道。

因此，单次通知丢失、队列溢出和重启均不要求再次发送或手工调用频道 `/start`。全库覆盖时间随数据量、RPC 和磁盘延迟变化，不能把小规模测试时间当成生产环境恢复上界。

每 30 秒记录结构化维护统计：`pending`、队列内 `oldest`、`completed`、`retries`、`overflow`。持续重试按频道限频告警；配置冲突包含 incoming/runtime 信息，远程应用拒绝在目标节点的 debug 日志中记录。这些是日志统计，尚未添加独立的 Prometheus 指标或大规模扫描基准。

关闭时先取消维护 worker，再停止其依赖服务。

## 验证方法

`test/e2e/channel_config_reconcile.py` 启动独立的三个服务器进程，用 64 个槽、12 个群频道和真实 HTTP 请求验证。脚本会校验频道集合、消息 ID、序号、payload、配置版本、learner 和迁移标记；每种成功场景在收敛后从每个入口连续读取 30 次。扩容场景强制验证槽 owner 与频道 leader 分离。正常新建三节点时，两者可以相同。

```sh
go build -o /tmp/octo-im-fixed main.go
python3 test/e2e/channel_config_reconcile.py --binary /tmp/octo-im-fixed \
  --scenario expand --output /tmp/reconcile-expand
python3 test/e2e/channel_config_reconcile.py --binary /tmp/octo-im-fixed \
  --scenario fresh --output /tmp/reconcile-fresh
python3 test/e2e/channel_config_reconcile.py --binary /tmp/octo-im-fixed \
  --scenario upgrade --legacy-binary /path/to/v2.2.5-20260422-fix1 \
  --output /tmp/reconcile-upgrade
python3 test/e2e/channel_config_reconcile.py --binary /tmp/octo-im-fixed \
  --scenario repair --legacy-binary /path/to/f00056b0 \
  --output /tmp/reconcile-repair
```

输出目录必须不存在。只使用 Python 标准库。每轮生成 `events.jsonl`、节点日志和 `summary.json`，默认在子进程退出后删除本轮数据库，使用 `--keep-data` 可保留。三进程数据约占数 GB，不建议在容量较小的 tmpfs 上并行运行多轮。

`upgrade` 先用旧 tag 创建和发送消息，保留数据库换成补丁，再扩容发送。`repair` 先用故障基线制造版本落后，并断言三个入口均为 503，然后保留全部数据重启为补丁，验证自动恢复。`expand --expect-stale` 配合故障二进制可单独运行反向对照。

新增 Go 测试覆盖：成功写入才通知、队列代次与溢出、重试和取消、扫描继续推进、并发提示、相同配置重复 100 次保留同步进度、旧版本和同版本冲突、正常/迁移休眠频道、读侧提示、分页边界、跨节点 RPC 以及目标 leader 更新。

```sh
go test ./pkg/cluster/channel ./pkg/cluster/cluster ./pkg/cluster/store ./pkg/wkdb \
  -run 'Test(Config|ChannelConfigReconcileRPC|ChannelClusterConfigRecoveryPagination|Conversation)' -count=1 -timeout=90s
go test -race ./pkg/cluster/cluster \
  -run '^TestConfig(Queue|Scan|Workers|ReadHint)' -count=1 -timeout=60s
```

2026-09-22 本地验证：扩容、旧 tag 数据升级扩容、故障数据重启恢复、新建三节点四种场景通过；每种场景 90 次完整读取。配置应用、存储通知、队列和分页的定向竞态测试通过。

已执行 `go test ./... -count=1 -timeout=300s` 及后续 60 秒限时全量检查，**全量未通过**。以下失败也在未修改的 `f00056b0` 中复现：

| 包 | 基线失败 |
| --- | --- |
| `internal/server`、`pkg/wknet` | 测试监听端口占用 |
| `internal/track` | `TestMessageString` 轨迹位图断言 |
| `internal/user/event` | `TestUserEventPool_AddConnectEvent` 超时 |
| `pkg/cluster/cluster` | `TestSendBatchOptimization`、`TestProcessBatchLatency`、`TestImprovedNode_BackpressureStrategies` |
| `pkg/wkdb` | 缓存统计、设备查询、截断日志、PluginUser 断言等既有失败 |
| `pkg/wkserver` | `TestReconnect` |

最后一轮并行全量运行还出现 `pkg/wkserver/test.TestSendAndRecv` 超时；该包在补丁和基线分别单独复测均通过（约 2.1 秒），保留为全量运行不稳定项，不计作完整测试通过。

完整集群初始化的 `-race` 检查也会报告基线已有的 clusterconfig/event/Raft 并发访问，不能声明整个集群已无竞态。本次新增维护与应用原语的定向测试通过；完整 channel、store 和 API 包检查通过。

## 部署边界

参与节点全部升级后才具备完整的自动同步能力。滚动期间旧 peer 不支持新内部 RPC 时会继续重试，不会误报成功。公网 API 和数据库格式不变；生产发布前仍需在业务容量下验证维护开销，并对现场账号检查完整会话和迁移状态。本 PR 不包含镜像发布或生产部署。
