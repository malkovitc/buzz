# buzz-broker-host

`buzz-broker-host` is the host-owned durable authority boundary for managed ACP
runtimes. This first production slice serves only `authority.status`; every
other broker action is rejected as unsupported until atomic action commit is
implemented in a later slice.

The host stores canonical authority identities, lifecycle state, credential
hashes, and bounded request receipts in SQLite. It never stores or prints the
raw bearer. On Unix, the database and its owner-only directory must belong to
the broker's effective user. The database path must be absolute. The HTTP
listener is loopback-only, so a colocated runtime can use plaintext without
publishing its bearer.

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

Fence the exact identity using the non-secret authority file:

```bash
buzz-broker-host --state /var/lib/buzz-broker/authority.db fence \
  --authority-file /secure/input/authority.json
```

Fencing is monotonic and idempotent. A fenced exact generation cannot be
reissued. Fencing atomically invalidates prior status receipts so a lost
pre-fence `active` response can never be replayed after the transition. The
bearer remains recognizable only so a fresh `authority.status` can return the
terminal `fenced` verdict; it cannot regain active authority.

## Serve

```bash
buzz-broker-host --state /var/lib/buzz-broker/authority.db serve \
  --bind 127.0.0.1:8787
```

Non-loopback binds are rejected. Point `BUZZ_BROKER_URL` at the loopback origin,
read `BUZZ_BROKER_CREDENTIAL` from the mode-0600 file in the launcher, and pass
the same canonical authority JSON as `BUZZ_MANAGED_ACP_AUTHORITY`.

The host retains the latest 64 receipts per credential. This bounds polling and
unsupported-action storage while preserving exact-byte replay for recent
retries. Eviction is safe in this slice because every served action is
read-only or rejected; a later effect host requires a separate durable effect
ledger rather than relying on this bounded read receipt cache.

## Deliberate boundary

This slice does **not** sign or publish relay events, expose keys, execute Git or
external effects, quiesce/drain a runtime, activate a second runtime, or serve
TLS. Client preflight still does not close the check/commit race. Those require
the later atomic host action and cutover slices; unsupported actions fail closed
here rather than pretending that admission is effect authority.
