# `pi-acp` pilot adapter

`pi-acp` is an experimental ACP adapter for Buzz managed agents. It embeds
`@earendil-works/pi-coding-agent` through `AgentSession` and keeps Buzz authoritative for relay
routing, identity, deadlines, deduplication, and NIP-AM publication.

Status: live production canary validated on Caliper; not the default runtime and not approved for fleet rollout. See [`../../docs/pi-acp-production-canary-2026-08-25.md`](../../docs/pi-acp-production-canary-2026-08-25.md).

The internal pilot supports macOS and Linux. Windows is rejected fail-closed and is not listed in the Desktop runtime catalog.

## Implemented

- ACP `initialize`, `session/new`, `session/prompt`, `_session/steering`, and `session/cancel`;
- strict LF-only JSONL framing, preserving U+2028/U+2029 inside JSON strings;
- multiple ACP sessions per pooled adapter process, with a fresh Pi SDK process per turn and a durable task-scoped Pi session keyed by relay, channel, and thread root;
- extension, skill, template, theme, and context-file discovery disabled;
- explicit built-in tool allowlist (`read` by default);
- per-inbound-event turn, tool, and processed-token budgets with checkpoint steering and abort;
- authoritative Buzz routing metadata supplied by `buzz-acp` under `_meta.buzz` when publication is allowed;
- trusted parent-side publication broker: the Pi SDK subprocess never receives `BUZZ_PRIVATE_KEY`;
- typed `buzz_reply` with non-empty validation, fixed routing, connected stdin, atomically written
  and fsynced reservation/receipt records, fail-closed crash/network ambiguity handling, and terminal
  settlement after a successful publication so provider continuation cannot hold the ACP turn open;
- typed bounded `kanban_tasks` that cannot download the full board;
- authenticated `-status`, `-cloud`, and `-local` fast path: `buzz-acp` marks only exact
  owner-signed, agent-mentioned commands in `_meta.buzz`; the adapter invokes one fixed absolute
  controller executable, publishes its bounded status through the durable reply broker, and never
  starts a Pi `AgentSession`, model turn, or model-callable tool;
- Continuation Capsule v1 validation/export/import with exact Git and Pi leaf lineage, bounded short explicit summary context with raw transcripts omitted, mnemonic/secret/thinking/tool-output rejection, idle/effect gates, and idempotent fresh-child restore;
- cumulative usage mapping, bounded tool output, and process-group cleanup.

`buzz_reply` intentionally reserves before network access. If the process crashes or the network
result is ambiguous, it refuses to retry automatically. This preserves at-most-once publication at
the cost of requiring operator reconciliation for an uncertain delivery. Linux deployments that
automatically recover after host power loss must set `PI_ACP_REQUIRE_POWER_LOSS_DURABILITY=1`;
the adapter fails closed before publication when that strict mode is requested on another OS.
macOS retains process/network-crash protection but does not claim physical power-loss durability.

## Test

```bash
pnpm install --frozen-lockfile
pnpm --filter @buzz/pi-acp test
pnpm --filter @buzz/pi-acp check
cargo test -p buzz-acp prompt_metadata_is_nested_under_buzz_meta --lib
```

## Local canary installation

From a reviewed exact Buzz commit:

```bash
npm install --global ./tools/pi-acp
pi-acp --version  # pi-acp 0.2.8 or newer
```

The adapter uses Pi's existing provider authentication. Run Pi interactively once if the host has
not yet configured a model/provider. For a read-only question canary, leave `PI_ACP_TOOLS=read`.
Raise budgets only through explicit environment values:

```text
PI_ACP_MAX_TURNS
PI_ACP_MAX_TOOLS
PI_ACP_MAX_PROCESSED_TOKENS
PI_ACP_REQUIRE_POWER_LOSS_DURABILITY  # Linux-only strict receipt mode
PI_ACP_CLOUD_CONTROL_COMMAND           # absolute trusted controller client path
PI_ACP_CLOUD_CONTROL_TIMEOUT_MS        # 1000..900000; default 600000
BUZZ_ACP_CLOUD_CONTROL_CHANNEL_ID       # one approved channel UUID; all others reject pre-controller
PI_ACP_TASK_SESSION_ROOT                 # optional absolute durable root; defaults under the Pi agent dir
```

The cloud controller receives a strict JSON object on stdin. A `prepare` request returns
`{"status":"ok|noop|blocked","content":"...","operationId":"..."}`; `operationId` is required
only for `ok`. Pi publishes `content` through durable `buzz_reply`, then sends a `commit` request
bound to the operation and signed receipt event. The bundled `pi-cloud-control` executable writes
that commit atomically for a model-free host supervisor. The local supervisor also reconstructs a
missing commit from the signed receipt, so a shutdown immediately after publication cannot lose
the accepted operation. Effects therefore begin only after the Buzz receipt boundary.

The controller environment is reduced to basic process/user variables: Buzz and model secrets are
not inherited. After buzz-acp validates the signed owner command and real Caliper `p`-tag, pi-acp embeds a
receipt-bound HMAC capability that never enters the controller environment. The host supervisor
verifies that capability with its host-mounted Caliper key, then correlates the exact relay
community, approved channel, owner/agent pubkeys, receipt route, owner generation, and expiry
before invoking a fixed handoff
action. The normalized Buzz read API is not treated as independent signature evidence. Relative paths, extra fields, stale generations, forged events, oversized
content, unknown commands, child failures, and malformed routing fail closed without an LLM
fallback. `pi-cloud-control supervise-once` is designed for launchd/systemd scheduling; its
location-specific mode-600 config and signing credentials remain host-mounted and out of Git.
Supervisor claims serialize overlapping polls; configured handoff actions must remain idempotent by
`operationId` and owner generation so a stale crash claim can safely reconcile against authoritative
handoff state.

Authenticated Buzz prompts derive task identity only from the canonical relay URL, managed-agent pubkey, channel UUID, and a homogeneous thread root supplied by `buzz-acp`. Each task gets a private mode-0700 Pi session directory. Storage is bounded to 512 task directories and 64 MiB per task; pruning preserves active leases and capsule lineage, and quota exhaustion fails closed. Mixed-thread batches and prompts without authenticated Buzz routing remain ephemeral; adapter restarts reopen only an exact task directory. `pi-continuation-capsule validate|export|reissue|import` uses bounded JSON on stdin and never copies raw Pi JSONL between locations. Import requires the capsule digest supplied by the signed control-plane handoff, verifies clean exact Git, creates a fresh child session, injects one sanitized custom context message, and records only lineage/digest metadata.

The process accepts ACP JSON-RPC/NDJSON on stdin and writes only ACP frames to stdout. Diagnostics
go to stderr. `PI_ACP_PI_COMMAND` and `PI_ACP_PI_ARGS_JSON` retain the non-shipping RPC subprocess
path for protocol fixtures; normal execution uses the embedded SDK bridge.

## Rollback

Stop the canary, restore its prior `agent_command` (normally `codex-acp`), restart it, and run one
bounded question smoke test. Never run `pi-acp` and another ACP adapter simultaneously for the same
identity and relay.
