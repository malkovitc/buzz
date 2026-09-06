# Kanban AI Operations

## Purpose and access boundary

Buzz roadmap work is tracked in an operator-managed private Kanban AI project.
This repository intentionally does not publish the instance address, workload or
volume names, credentials, internal identifiers, or migration receipts. Obtain
those values from the approved private operator inventory before performing an
operation.

Do not print, copy into the repository, pass on a command line, or retain in
logs any MCP key or application credential. A helper may read a credential from
its approved secret source directly into process memory and place it only in an
HTTP authorization header.

## Project ownership

- The private `Buzz` project owns changes delivered to Buzz clients, relay,
  agents, operations, or releases.
- `linza` owns Linza changes even when Buzz supplied the architectural idea.
- `[DISCOVERY][BUZZ→LINZA]` work stays in `linza`.
- Cross-project work has one owning card and links to related work; do not
  duplicate cards.

Stop and resolve ownership before creating or moving a card when the delivery
target is ambiguous.

## Supported operations

Prefer authenticated MCP operations for application-facing reads and ordinary
writes. Use read-only project and board reads for discovery and verification.
Deletion requires an exact-ID preflight, explicit confirmation of the expected
owner/project, and a current verified backup.

If the deployed MCP interface cannot preserve an identity required by a move or
restore, direct database access is an exceptional migration. It requires a
reviewed scope lock, the private schema/inventory, a consistent backup, one
transaction, and independent database plus MCP verification. Do not use direct
SQL for ordinary board edits or retain a general-purpose migration script.

## Direct-write safety contract

Before an exceptional database write:

1. Resolve the canonical instance, database, and backup location from the
   private operator inventory; verify that they identify the expected workload.
2. Open discovery handles read-only. Confirm exact source and destination IDs,
   ownership, expected counts, and absence/presence preconditions.
3. Create a consistent online SQLite backup outside the live database triplet.
4. Open the backup read-only and require both `PRAGMA integrity_check` to return
   `ok` and `PRAGMA foreign_key_check` to return no rows; then record its size
   and SHA-256.
5. Enable `PRAGMA foreign_keys = ON` on the write connection and verify it is
   active. Revalidate the pre-write snapshot inside one transaction.
6. Apply only the approved writes and assert exact postconditions before commit.
7. Compare every field and comment that was required to remain unchanged.
8. Require live and backup integrity checks and empty foreign-key checks.
9. Verify project and board state through authenticated MCP reads.

Stop on ambiguous ownership, duplicate project names, missing records,
unexpected counts, changed preserved fields, a failed integrity check, or any
foreign-key violation. Keep non-secret evidence in the private task record, not
in this public repository.

If a reviewed one-time migration script is unavoidable, put it only in an
approved writable temporary location, execute it once, and delete it after
verification. It must not embed credentials, card content, or reusable
operational assumptions.

## Realtime voice roadmap boundary

The private Buzz roadmap sequences VOICE 0–8. Use the existing Huddle relay and
connect an agent as an authorized audio peer. Buzz retains identity,
authorization, room membership, consent, and effects; external speech providers
sit behind the smallest proven media/dialogue boundary. Tool calls use existing
managed-agent boundaries.

Do not add a new SFU, gateway, or plugin framework without measured necessity.
Do not duplicate [PR #7217](https://github.com/block/buzz/pull/7217), which
covers provider-hosted A/V sessions. Treat
[PR #7232](https://github.com/block/buzz/pull/7232) as an STT backend, not
realtime speech-to-speech.

## Backup restore

A full SQLite restore discards every board write made after the selected backup.
For an isolated accidental deletion, prefer an application-level reconstruction
when it can preserve required identity and history; otherwise explicitly accept
or export intervening writes before restoring.

Restore only with the Kanban workload fully stopped and the data volume mounted
through the approved offline operator method. If the private inventory does not
provide such a method, stop: never restart the workload merely to gain `exec`
access.

1. On the same filesystem as the canonical database, copy the selected backup
   to a unique staging path. Verify its recorded hash, then open it with an
   immutable read-only SQLite URI and require a successful integrity check and
   an empty foreign-key check.
2. Assert that no staging `-wal` or `-shm` sidecars exist. Record canonical
   database ownership and mode. Atomically move the canonical database and any
   canonical WAL/SHM sidecars together into a unique quarantine directory.
3. Apply the recorded ownership and mode to the staged database, reassert that
   it has no sidecars, and atomically rename it to the canonical database path.
   Assert that no WAL or SHM exists at the canonical path.
4. While the workload remains stopped, open the restored canonical database
   immutable and read-only. Require a successful integrity check and an empty
   foreign-key check. On failure, keep the workload stopped and atomically
   restore the quarantined database triplet.
5. Start the workload and verify expected projects, counts, statuses, and
   selected comments through authenticated MCP reads.
6. Keep quarantine until application verification succeeds; remove it only
   under the retention policy from the private operator inventory.
