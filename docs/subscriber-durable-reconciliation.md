# Subscriber durability, throughput and business revisions

PR #51 backported PR #50's shorter cleanup path on another base. It is not a
literal ancestor of PR #54. PR #54 correctly introduced durable source intent,
receipts, target lifecycle fencing, reverse-relation cleanup and restartable
checkpoints. This follow-up retains those mechanisms. Leader admission alone
is not durable completion, and eventual recovery does not erase an HTTP failure.

The throughput changes remove redundant recovery watermarks; separate same-slot
append/apply locks while truncation stays exclusive; overlap independent physical
DB Sync commits with a wait-all barrier; combine target rows and reverse relations
in one per-shard batch; reduce slot memtables; and coalesce WAL sync requests.
Only a replay-safe Raft applied cursor can use NoSync after durable effects and
replay fences. Raft logs, memberships, receipts and lifecycle effects still Sync.
Arbitrary apply callbacks and manual applied-cursor writes remain synchronous.

Adjacent metadata updates are collapsed into atomic per-channel final state,
bounded to 256 commands and never moved across other command types. Every shard
must Sync before the legacy applied watermark advances. Managed business channels
are filtered before this legacy batch path. The speculative two-second minimum
remaining proposal budget is removed: a healthy target may use the actual time
left before the unchanged request deadline.

## Business authority protocol

The IM queue cannot repair a business request it never received, or infer SQL
compensation after a successful reply was lost. A companion backend therefore
commits a dirty revision with each audited SQL mutation and sends authoritative
member/deny snapshots after releasing the transaction.

`POST /channel/subscriber_reconcile` accepts group/child identity, a positive
`revision`, stable `operation_id`, subscribers, denylist, and Ban/Large/Disband
flags. The optional paged form includes a 64-hex `snapshot_id`, `page_index`,
`page_count`, and lexical UID interval `[range_start, range_end)`. The first
start and final end are empty. Only that range is replaced; the last range also
removes old surplus members. Each request retains the one-MiB body bound.

A per-channel revision and current-revision page digests survive receipt expiry
and channel deletion. A higher revision immediately fences every older page.
Exact retained receipt retries return their prior completion without replay;
new obsolete requests and conflicting same-revision payloads return 409.
Already-committed stale legacy writes become deterministic no-ops during Apply,
rather than permanently stalling a slot. New legacy proposals on managed channels
are rejected. Business snapshots preserve metadata they do not own.

Protocol 4 uses a distinct command and capability/activation RPC. Protocol 3
commands retain their wire format. Every current node identity must have a
replicated protocol-4 proof before the new command is proposed. A lagging leader
can obtain that proof even if an already-confirmed peer is offline. After
activation, both join admission and config Apply prevent an old node from
lowering the protocol requirement. An individual capability advertisement does
not activate the entire cluster.

Upgrade every IM replica before enabling the companion backend's authority
flag; coordinate all business writers. Do not downgrade protocol-4 data to an
old binary: an old binary cannot enforce new command/fence semantics. This is
an explicit upgrade contract, not automatic downgrade compatibility.

## Evidence and limits

StrictMem and failure tests cover actual lost applied cursors, durable effects,
partial shard Sync in both orders, replay, stale lifecycle suppression, adjacent
metadata ordering/checkpoint failure, receipt expiry and permanent revision
fencing. Negative controls fail on the old cursor/deadline implementations.

Protocol-4 runtime testing covers mixed-version rejection, offline-peer catchup,
source SIGKILL with 256 durable pending removals, new-leader draining, restart
fences, rejection of an old joining process and successful join of the same
identity after its binary upgrade. These are functional checks, not clustered
throughput capacity measurements.

The full Go suite was run: 27 packages passed and 7 failed in unchanged baseline
cache/device/plugin/network/track/event fixtures. The suite is not green.
Targeted changed storage/slot/store/protocol race tests pass.

A previous throughput candidate passed 600 seconds but failed a controlled hour:
22,534,730 HTTP attempts, 9 HTTP503 failures and 4 Slot Raft timeouts. All failed
operations recovered; full membership/conversation checks, 2,562 sampled message
identities and all 378 tails (21,678,828 messages) passed before and after restart.
No ENOSPC or OOM occurred. The first timeout window showed I/O pressure before
memory-limit encounters; this does not identify a specific hardware fault.
The current metadata/deadline candidate still needs controlled load acceptance.
Do not present this follow-up as a demonstrated zero-timeout fix until that runs.

The local stress mapping uses 378 channels, 25 users, 225 initial memberships,
42 churn + 42 message + 16 metadata workers, 2 CPU/4 GiB and the existing 5-second
deadline with same-ID retries. The original external screenshot script is not
available, so this is not a byte-identical reproduction. Actual business tests
also use real backend routes and WKSDK clients; their workload and final
membership count differ and must be reported separately.
