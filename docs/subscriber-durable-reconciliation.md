# Subscriber durability, throughput and business revisions

PR #51 backported PR #50's per-slot locking, synchronous group commits, async
cleanup and non-leader proposal-routing fix on another base. It is not a
literal ancestor of PR #54. Those fixes remain useful; PR51 explicitly limited
async success to leader admission and excluded multi-node overload/backlog. PR #54 correctly introduced durable source intent,
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

The final full Go suite was run: 26 packages passed and 8 failed. Unchanged
cache/device/plugin/track/event and network fixtures still fail; the suite is
not green. The extra network test package hit a fixed-port collision in the
full run, and a later isolated attempt timed out in TestSendAndRecv. An earlier
isolated attempt passed, so neither outcome is hidden. Relevant slot/store and
subscriber/recovery/metadata wkdb race checks pass. Both protocol-activation
tests were also run explicitly with race detection and verbose output.

The combined protocol/throughput implementation at `636dc81b` passed a controlled
hour: 25,131,544 HTTP attempts, zero HTTP failures and zero Slot/Channel Raft
timeouts. The same process drained, 121.495 seconds of quiet observation passed,
all membership/conversation and witness 0→378→0 checks passed, and 2,555 sampled
message identities plus every channel tail (24,615,089 messages across 378
channels) matched before and after restart. A prior candidate's short pass had
failed at minute 30, so that failure and all later experiments are retained.

This is a latency/correctness result, not a membership-throughput improvement:
the hour sustained 69.525 successful churn operations/s, maximum 2.694 s, with
24,491 churn requests exceeding one second. Its total RPS is mostly enqueue-only
message traffic. The isolated 20 ms candidate sustained 112 churn/s versus the
5 ms candidate's 210/s; PR54's short baseline sustained about 90 successful/s
with 327 HTTP failures and 253 Slot timeouts. Durations and closed-loop traffic
composition differ. Coalescing Sync requests trades peak membership throughput
for fewer competing storage barriers; it does not weaken Sync-before-ACK.

The hour reached its 4 GiB memory limit (4,072 max/reclaim events), with no OOM;
CPU throttling totaled 64 ms and disk free stayed above 377.7 GB. Peak host I/O
PSI avg10 was 9.86% some / 6.93% full. Do not sum physical and device-mapper I/O
counters or interpret no OOM as spare memory capacity.

After that hour, a real adoption test found a separate correctness defect:
PreserveExisting copied DeletedAtMsgSeq but the ordinary row writer omitted it
when assigning the new identity. The final `988a5a80` change persists that field
in the existing atomic lifecycle batch. The negative control failed 20→0;
batch/individual, restart, and StrictMem partial-shard power-loss tests now pass.
No wire flag, generic update behavior, or durability barrier changed. The hour
above certifies its exact earlier binary; final-binary tests are recorded in the
companion validation evidence, not silently substituted for that provenance.

The local stress mapping uses 378 channels, 25 users, 225 initial memberships,
42 churn + 42 message + 16 metadata workers, 2 CPU/4 GiB and the existing 5-second
deadline with same-ID retries. The original external screenshot script is not
available, so this is not a byte-identical reproduction. Actual business tests
also use real backend routes and WKSDK clients; their workload and final
membership count differ and must be reported separately.

Final-binary business fault validation also passed all four cases with the new
25-user fixture: lost successful replies through the foreground deadline;
never-forwarded child creation; backend SIGKILL after SQL commit; failed kick,
rejoin and five delayed old snapshots (all 409, latest membership preserved).
All 266 authority rows completed afterward. Business failure responses remain
the existing HTTP400 envelope; injected IM503 responses are not reported as
normal-load successes. Configuration was restored after the fault run.

Final IM binary `988a5a80` (`60931515d80a7f7c5ce800f9c317870d6199d4e1153e5e680fcd4a389f246c88`) additionally passed
603.452 seconds of the same controlled mixed workload:
8,121,536 HTTP attempts, zero failures and zero Raft
timeouts. Churn sustained 112.650/s,
maximum 1.591 s. Quiet observation, all membership and
witness checks, 448 sampled message identities,
all 378 persisted message tails (7,977,210 messages)
and restart checks passed. This final-binary short regression complements the
explicitly identified earlier hour; it does not relabel that hour's binary.

Machine-readable evidence: [validation record](im-reconciliation-validation-20260925.json).
