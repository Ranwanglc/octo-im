# Subscriber recovery

Membership APIs return HTTP 200 after the source change, conversation effects,
and tag invalidation have completed. A timeout returns 503 with an operation
receipt; already committed work remains durable and can be polled at
`/channel/subscriber_operation`. Reuse `operation_id` or `Idempotency-Key` when
retrying a lost response.
Existing matching receipts are checked before contacting the message leader for
a read floor, so a completed retry does not depend on that leader's availability.

Cleanup and denylist requests against a missing channel remain successful
no-ops, with a completed receipt so retrying after later channel creation cannot
change that new channel. `/channel/delete` retains its reversible disband flag:
it preserves subscribers and their conversations. Explicit subscriber removal
continues to delete the corresponding conversations.

`subscriberRecovery.enabled` defaults to `true`. Setting it to `false` pauses
background workers. Once lifecycle state exists, foreground membership changes
continue through the versioned path, including after a restart. Deployments
which have never activated recovery retain the legacy path while disabled.
Person channels retain their existing behavior; live channels do not acquire
recent conversations. Reset, denylist restoration, read positions, and command
channel tags keep their existing semantics.

`workers` accepts 1 or 2 (capped by database shards); `interval` must be at least
10ms, and `timeout` and `maxPending` must be positive. Legacy `oldV1Api` migration
is rejected before API startup when recovery is enabled or has durable state.

`maxPending` defaults to 1024 outstanding effects/finalizations per source
slot and database partition. An idle partition admits one larger operation,
then applies backpressure until it drains. There is no 4096-member ceiling for
an existing group. The HTTP/operation payload limit remains 1 MiB. Conversation
effects are sent in pages of at most 64, including when many users hash to the
same slot. The configured timeout covers inline completion, lock waits, and
forwarding to another slot leader.

Pending work stores a small scheduling header and immutable chunks of 64
effects. Background scans read only headers, including during backoff; workers
read at most one processing page, and checkpoints update the header and remove
fully acknowledged chunks. Checkpoint I/O therefore scales with the processed
page, not the original group size. Opening an older data directory atomically
migrates each monolithic pending record once, preserving its progress. A crash
during migration leaves either the old record or the complete paged replacement.

Successful and rejected operation IDs are retained for seven days. Expired
receipts are removed in bounded batches on subsequent source writes; pending
operations never expire. This is the idempotency window: after it expires,
clients must use a new operation ID and inspect current state before retrying
an old request. Records from before this retention index was introduced are
retained. Lifecycle fences remain durable per user/channel because delayed
Raft entries and old cache flushes must not resurrect departed members. They
are overwritten by newer membership generations, not appended per operation.

Before activation, all configured node identities must confirm subscriber
protocol version 3 (replicated confirmations and paged pending work). The config
leader probes unconfirmed identities after startup and commits successful
confirmations through config Raft, independently of membership traffic. Each
update carries the complete known proof set so an upgraded node also receives
confirmations it skipped while running an old binary. All slot leaders inherit
that durable proof; normal proposals make no capability RPC. A slot leader
behind on config apply can obtain the committed proof from the config leader.
Confirmed replicas may be offline: source mutations and pending effects then
need Raft quorum, without needing the unavailable node to answer a handshake.

A never-confirmed node (including a configured node that has never started)
still blocks first activation; quorum alone cannot establish that an offline
old binary understands recovery commands. Upgrade and confirm every node before
activation. A replacement address/creation identity needs a new confirmation.
Joining nodes must advertise the protocol when recovery is enabled or active.
Downgrading an activated data directory, including to protocol 2, is unsupported.
Disabling background workers does not downgrade the format or remove fences.

Slot apply checkpoints legacy mutations before executing later commands;
contiguous configuration saves keep their existing batching.
Recovery entries use their transactional version/progress fences and share a
batch checkpoint. Blacklist/allowlist mutations are idempotent, and message-event
projections have an atomic per-slot replay marker. Transient storage failures
can retry even when workers are paused. Explicitly retired stream, notify-queue,
channel-clear/config-delete and plugin command enum values retain their historic
no-op replay behavior. Unrecognized new command types and malformed live commands
fail fast rather than being silently skipped or retried indefinitely.
Persisted recovery invariant failures are classified the same way. Slot apply
runs through `raftgroup`, whose worker panic handler re-panics and terminates
the process; it does not use the standalone `raft.Raft` worker pool. Real slot
integration tests cover transient-fault recovery and process exit on poison
entries. No failed committed command is silently acknowledged or skipped.

Legacy conversation writes still wait for durable commits before acknowledging
apply, including `UpdateConversationIfSeqGreaterAsync` (its legacy name is kept
for API compatibility). Conversation commands stay ordered individually;
configuration saves retain batching. Reverse relations use the channel shard.
Writes fenced by deleted or mismatched lifecycle IDs remain rejected, with one
warning per filtered batch reporting the reasons; arbitrary old IDs are never
rebound to a new membership generation.
Deleting a recent conversation does not leave the channel. A later unread or
read-position update can re-create a missing row using the current, non-deleted
lifecycle ID. Membership tombstones and explicitly stale IDs remain fenced.

Foreground recovery locks are keyed by operation and reclaimed when no caller
uses them. Source writes lock their database partition; tag membership reads and
RPC run outside the tag publication lock. Tag generations are per channel;
entries are reclaimed after all active fills release them, so unrelated channels
sharing a lock stripe cannot invalidate each other's fills. Lifecycle reads use a bounded cache
with commit generations to reject delayed cache fills. Single-member and
single-conversation deletions remove existing columns in the same atomic batch,
avoiding per-record range tombstones and preserving any unknown stored columns'
deletion semantics.
