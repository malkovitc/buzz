# buzz-broker-host

`buzz-broker-host` is the host-owned durable authority boundary for managed ACP
runtimes. It always serves `authority.status`. With a host-owned signing key and
verified owner attestation, it also serves `storage.address`, `channel.read`, and `message.post`.
Other broker actions remain unsupported.

The host stores canonical authority identities, lifecycle state, credential
hashes, and bounded request receipts in SQLite. It never stores or prints the
raw bearer. The host runs on Unix only: the database and its owner-only
directory must belong to the broker's effective user. Startup fails closed on
other platforms until equivalent ownership and ACL checks exist. The database
path must be absolute. The HTTP listener is loopback-only, so a colocated
runtime can use plaintext without publishing its bearer.

Schema v5 binds the first successful signer-backed action to the verified owner
for the entire generation. Later `storage.address`, `channel.read`, and
`message.post` requests with another owner are rejected. Upgrading a v1-v4
database quiesces every previously active generation and clears its old
read-only receipts because earlier schemas could not prove which owner had
used it. Drain and fence those generations, then issue successors. Stop older
host processes before upgrading.

## Issue and fence

Prepare a strict `AuthorityIdentity` JSON file using the same shape as
`BUZZ_MANAGED_ACP_AUTHORITY`, then issue into new files:

```bash
install -d -m 700 /var/lib/buzz-broker
buzz-broker-host --state /var/lib/buzz-broker/authority.db issue \
  --authority-file /secure/input/authority.json \
  --credential-output /var/lib/buzz-broker/runtime.credential
```

The credential output is created with mode `0600` on Unix and is never
overwritten. Capture it through the file only; do not put the bearer in argv,
logs, or shell history. Issuance fails if the exact generation was ever issued
before or another generation is active for the same normalized
community/logical-agent/task scope.

Close new admission for the exact identity using the non-secret authority file:

```bash
buzz-broker-host --state /var/lib/buzz-broker/authority.db quiesce \
  --authority-file /secure/input/authority.json
```

Quiescing is durable and idempotent. `authority.status` reports `quiescing`, so
managed clients refuse new turns. The generation still reserves its scope, and
exact retries of already admitted message effects can reconcile while new
message requests fail. Old `active` status receipts are invalidated at the
transition. An already-running v2 host sees the underlying state as `fenced`
and fails closed; the v3 host retains the scope reservation until final fence.

Resolve any pending effect after quiesce:

```bash
buzz-broker-host --state /var/lib/buzz-broker/authority.db drain \
  --authority-file /secure/input/authority.json \
  --signing-key-file /secure/agent.key \
  --auth-tag-file /secure/owner.auth-tag
```

Drain requires quiescing. It takes an owner-only lock for the exact credential,
queries the stored event ID, publishes the same signed event only if absent,
and commits the outcome before releasing the lock. A relay failure leaves the
effect pending and fencing blocked. Runtime retries and operator drain share
the lock, so concurrent attempts cannot send a second copy after a completed
effect or a final fence. Stop any older broker-host process before using drain
during a rolling upgrade.

The relay rejects signed events whose timestamp is more than 15 minutes from
its clock. If an event remained unpublished past that window, drain may never
resolve it: the host leaves the generation quiescing and refuses fence. A later
relay-level terminal proof is needed before this case can support automatic
cutover. Do not force a successor generation by editing broker state.

Then fence the exact identity:

```bash
buzz-broker-host --state /var/lib/buzz-broker/authority.db fence \
  --authority-file /secure/input/authority.json
```

Fencing is monotonic and idempotent. It refuses to complete while a previously
admitted message effect is unresolved. A fenced exact generation cannot be
reissued. Fencing atomically invalidates prior status receipts so a lost
pre-fence `active` or `quiescing` response can never be replayed after the transition. The
bearer remains recognizable only so a fresh `authority.status` can return the
terminal `fenced` verdict; it cannot regain active authority.

## Serve

```bash
buzz-broker-host --state /var/lib/buzz-broker/authority.db serve \
  --bind 127.0.0.1:8787 \
  --signing-key-file /secure/agent.key \
  --auth-tag-file /secure/owner.auth-tag
```

Non-loopback binds are rejected. Point `BUZZ_BROKER_URL` at the loopback origin,
read `BUZZ_BROKER_CREDENTIAL` from the mode-0600 file in the launcher, and pass
the same canonical authority JSON as `BUZZ_MANAGED_ACP_AUTHORITY`. Both signer
files must be regular files owned by the broker user inside owner-only
directories; the key is never passed in argv. The host verifies the NIP-OA
owner signature against the key and only signs `message.post` when that key
matches the authority's executor pubkey. Omitting both signer flags keeps the
status-only host mode.

`message.post` admission and the signed event are committed to a separate,
non-evicting SQLite effect journal under the same generation lock as fencing.
The host submits that exact event to the authority's relay and records success
only after an accepted ACK or an authenticated query proves the same event ID
exists. Exact-byte retries return the same event ID. Changed bytes under an
existing request ID conflict. If the relay outcome remains unknown, the effect
stays pending: fencing and new message effects for that credential fail closed
until an exact retry can reconcile it. The host retains the latest 64
read-only/unsupported receipts per credential independently of this journal.
An exact retry first queries the stored event ID and only republishes it if
the relay confirms it is absent. HTTP endpoints are derived from the relay
origin, including when the WebSocket identity URL contains a path or query.

`storage.address` derives the NIP-AE record address from the host-held agent
key and the verified owner pubkey. It returns only the agent pubkey, event kind,
and keyed `d` tag for the requested slug. The bearer must name an active
generation whose executor matches the signer; quiescing invalidates previous
address receipts. An exact retry also checks the current signer and derived
address before replaying a receipt, so a restarted host without the signer or
with a different verified owner cannot return an old address. Denied requests
also retain their exact-retry receipt. The first successful signer-backed
action durably binds the owner, so even a fresh request ID cannot derive an
address for another owner. Change owners through a successor generation after
quiesce, drain, and fence. This action does not fetch or decrypt a memory record.

`channel.read` makes a host-signed NIP-98 `POST /query` request to the community
relay. Relay membership and channel-access policy remain authoritative. The host
accepts only signed events matching the requested channel, kind, optional root,
and optional agent mention. Each relay page is capped at 16 events and 16 MiB;
callers may lower their page limit if a large result exceeds this cap. A full page returns an opaque,
MAC-protected cursor bound to the filter, relay, signer, and verified owner; the
next request sends the relay's exact `(until, before_id)` boundary. Exact retries
replay the stored page during a relay outage while retained. Receipts are bounded
to 64 entries and 32 MiB per credential, with a 128 MiB table-wide budget; the
oldest entries are evicted first, including across credentials. An exact retry
after eviction queries the relay again and needs it to be available. Successful
read receipts persist
their verified owner and cannot replay after owner rotation. Denied receipts
retain the normal exact-retry contract. Fresh reads under another owner are
rejected once the generation is bound.

## Deliberate boundary

This slice does **not** provision signer files or broker credentials into
Desktop/Kubernetes launchers, serve the other broker actions, execute Git
effects, drain a runtime automatically, or serve TLS. An unresolved effect
needs an exact request retry or operator drain while the relay is reachable.
End-to-end launcher cutover remains MOBILITY 3B3. Voice 2 remains blocked until
the host boundary is adopted by its launch and Huddle paths and revalidated end
to end.
