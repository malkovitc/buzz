#!/usr/bin/env bash
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
temporary="$(mktemp)"
trap 'rm -f "$temporary"' EXIT

BUZZ_BROKER_HOST_IMAGE=local/broker-host:test \
BUZZ_SPRIG_IMAGE=local/sprig:test \
BUZZ_BROKER_STATE_DIR=/tmp/buzz-broker-state \
BUZZ_BROKER_SIGNING_DIR=/tmp/buzz-broker-signing \
BUZZ_BROKER_CREDENTIAL_DIR=/tmp/buzz-broker-credential \
BUZZ_BROKER_AUTHORITY_FILE=/tmp/buzz-authority.json \
BUZZ_AGENT_WORKSPACE_DIR=/tmp/buzz-agent-workspace \
BUZZ_RELAY_URL=wss://relay.example \
BUZZ_ACP_CHANNELS=11111111-1111-4111-8111-111111111111 \
BUZZ_ACP_AGENT_OWNER=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
docker compose -f "$script_dir/compose.yml" config --format json > "$temporary"

python3 - "$temporary" <<'PY'
import json
import sys

services = json.load(open(sys.argv[1], encoding="utf-8"))["services"]
host = services["broker-host"]
agent = services["agent"]
assert agent["network_mode"] == "service:broker-host"
assert not host.get("ports") and not agent.get("ports")
assert host["user"] == agent["user"] == "1000:1000"
assert host["command"][host["command"].index("--bind") + 1] == "127.0.0.1:8787"

def mounts(service):
    return {item["target"]: item for item in service["volumes"]}

host_mounts = mounts(host)
agent_mounts = mounts(agent)
assert "/broker/signing" in host_mounts
assert "/broker/signing" not in agent_mounts
assert "/broker/state" in host_mounts
assert "/broker/state" not in agent_mounts
assert not host_mounts["/broker/credential"].get("read_only", False)
assert agent_mounts["/run/buzz/credential"]["read_only"]
assert agent_mounts["/run/buzz/authority.json"]["read_only"]
assert agent_mounts["/run/buzz/agent-entrypoint.sh"]["read_only"]
assert agent["environment"]["BUZZ_AGENT_MODE"] == "broker"
assert not any(name in agent["environment"] for name in (
    "BUZZ_PRIVATE_KEY", "BUZZ_ACP_PRIVATE_KEY", "NOSTR_PRIVATE_KEY",
    "BUZZ_AUTH_TAG", "BUZZ_RELAY_URL"
))
PY

sh -n "$script_dir/agent-entrypoint.sh"
echo "PASS: managed agent Compose isolation contract"
