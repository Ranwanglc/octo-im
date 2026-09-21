# Subscriber recovery

Membership APIs return HTTP 200 after the source change, conversation effects,
and tag invalidation have completed. A timeout returns 503 with an operation
receipt; already committed work remains durable and can be polled at
`/channel/subscriber_operation`. Reuse `operation_id` or `Idempotency-Key` when
retrying a lost response.

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

Successful and rejected operation IDs are retained for seven days. Expired
receipts are removed in bounded batches on subsequent source writes; pending
operations never expire. This is the idempotency window: after it expires,
clients must use a new operation ID and inspect current state before retrying
an old request. Records from before this retention index was introduced are
retained. Lifecycle fences remain durable per user/channel because delayed
Raft entries and old cache flushes must not resurrect departed members. They
are overwritten by newer membership generations, not appended per operation.

Before proposing recovery commands, the slot leader checks all configured
nodes for subscriber protocol version 2. An unconfirmed or explicitly older
node blocks activation. Confirmed capabilities are persisted so an unavailable
replica does not prevent majority-side recovery after a restart. A replacement
node address/identity requires a new confirmation. Joining nodes must advertise
the protocol when recovery is enabled or active. Upgrade every node before
activation; downgrading an activated node is unsupported. Disabling background
workers does not downgrade the data format or remove lifecycle protection.

Slot apply checkpoints legacy mutations before executing later commands;
contiguous configuration saves keep their existing batching.
Recovery entries use their transactional version/progress fences and share a
batch checkpoint. Blacklist/allowlist mutations are idempotent, and message-event
projections have an atomic per-slot replay marker. Transient storage failures
can retry even when workers are paused. Unknown or malformed committed commands
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

Foreground recovery locks are keyed by operation and reclaimed when no caller
uses them. Source writes lock their database partition; tag membership reads and
RPC run outside the tag publication lock. Lifecycle reads use a bounded cache
with commit generations to reject delayed cache fills. Single-member and
single-conversation deletions remove existing columns in the same atomic batch,
avoiding per-record range tombstones and preserving any unknown stored columns'
deletion semantics.
