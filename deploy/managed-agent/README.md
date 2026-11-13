# Keyless managed ACP Compose canary

This opt-in project runs one managed ACP generation beside `buzz-broker-host`.
The host owns the signing key, owner attestation, and SQLite state. The ACP
container mounts only the issued bearer, exact authority JSON, and workspace.
The two containers share a network namespace so the ACP reaches the host on
`127.0.0.1:8787`; no broker port is published. They do not share a PID or mount
namespace. This project does not modify the relay Compose stack.

## Build and provision

Build both images from the same reviewed Buzz revision. For a local canary:

```bash
docker build -f Dockerfile.broker-host -t buzz-broker-host:canary .
docker build -f Dockerfile.sprig -t buzz-sprig:canary .
docker run --rm buzz-broker-host:canary --state /tmp/authority.db --help
bash deploy/managed-agent/test-compose.sh
```

On a Linux host, prepare distinct bind paths owned by UID/GID 1000:

```bash
install -d -m 0700 -o 1000 -g 1000 /private/buzz-agent/state
install -d -m 0700 -o 1000 -g 1000 /private/buzz-agent/signing
install -d -m 0700 -o 1000 -g 1000 /private/buzz-agent/credential-g1
install -d -m 0700 -o 1000 -g 1000 /private/buzz-agent/workspace
```

Place `agent.key` and `owner.auth-tag` in the signing directory as regular
mode-0600 files owned by UID 1000. The attestation must verify for that exact
agent key. Prepare a strict `AuthorityIdentity` JSON file for this exact relay,
agent, task, and generation; make it readable by UID 1000. Keep the signing
directory and SQLite directory out of the ACP container. The bearer is created
by the host command below; never put its value in Compose, argv, or logs.
The authority's `executorAgentPubkey` must match `agent.key`, and
`BUZZ_ACP_AGENT_OWNER` must match the owner pubkey verified from
`owner.auth-tag`. The authority's relay URL must match `BUZZ_RELAY_URL`.

Copy `.env.example` to `.env` in this directory and set the absolute bind
paths, relay URL, channel UUIDs, owner pubkey, and image references. Pin both
images by digest on a real server. The Sprig image must include the keyless
broker runtime from this fork. Add model-provider configuration to the agent
image or its own private mount before asking it to answer real prompts.

## Issue and launch

Run these commands from `deploy/managed-agent`:

```bash
docker compose --env-file .env run --rm --no-deps broker-host \
  --state /broker/state/authority.db issue \
  --authority-file /broker/authority.json \
  --credential-output /broker/credential/bearer
docker compose --env-file .env up -d --wait broker-host
docker compose --env-file .env up -d agent
docker compose --env-file .env ps
```

Issue is explicit and one-time. A restart reuses the same durable state and
credential; the launcher never mints a replacement generation automatically.
If issue fails after creating a credential file, inspect the state before any
manual cleanup. The ACP entrypoint requires the bearer to be mode 0600 and
owned by UID 1000, removes legacy direct-key variables, then starts `buzz-acp`
in broker mode. The agent does not mount the key or SQLite path.

## Quiesce, drain, fence

For the exact authority file used at issue:

```bash
docker compose --env-file .env run --rm --no-deps broker-host \
  --state /broker/state/authority.db quiesce \
  --authority-file /broker/authority.json
docker compose --env-file .env ps
docker compose --env-file .env run --rm --no-deps broker-host \
  --state /broker/state/authority.db drain \
  --authority-file /broker/authority.json \
  --signing-key-file /broker/signing/agent.key \
  --auth-tag-file /broker/signing/owner.auth-tag
docker compose --env-file .env run --rm --no-deps broker-host \
  --state /broker/state/authority.db fence \
  --authority-file /broker/authority.json
```

Confirm the ACP exited after quiesce before issuing a successor. Drain may
leave a pending event unresolved during relay uncertainty; fence then refuses
to proceed. A successor needs a new authority file and credential directory,
while the host keeps the same durable SQLite directory. Recreate both
containers together if the broker container is restarted or replaced, since
the ACP shares its network namespace. This canary does not host-gate Huddle
audio or prove VOICE 2 completion.
