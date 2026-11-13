# buzz-broker-host

`buzz-broker-host` is the host-owned durable authority boundary for managed ACP
runtimes. It always serves `authority.status`. With a host-owned signing key and
verified owner attestation, it also serves `message.post`. Other broker actions
remain unsupported.

The host stores canonical authority identities, lifecycle state, credential
hashes, and bounded request receipts in SQLite. It never stores or prints the
raw bearer. The host runs on Unix only: the database and its owner-only
directory must belong to the broker's effective user. Startup fails closed on
other platforms until equivalent ownership and ACL checks exist. The database
path must be absolute. The HTTP listener is loopback-only, so a colocated
runtime can use plaintext without publishing its bearer.

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

Fence the exact identity after pending effects have resolved:

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

## Deliberate boundary

This slice does **not** provision signer files or broker credentials into
Desktop/Kubernetes launchers, serve the other broker actions, execute Git
effects, drain a runtime or pending effect automatically, activate a second
runtime, or serve TLS.
An unresolved effect currently needs an exact request retry while the relay is
reachable; automatic background drain and two-runtime cutover remain MOBILITY
3B3. Voice 2 remains blocked until the host boundary is adopted by its launch
and Huddle paths and revalidated end to end.
