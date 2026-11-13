#!/usr/bin/env python3
"""Linux-only synthetic signer -> Compose broker -> keyless ACP smoke."""

import http.server
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time


ROOT = Path(__file__).resolve().parents[2]
COMPOSE = ROOT / "deploy/managed-agent/compose.yml"
BROKER_IMAGE = os.environ.get("BUZZ_BROKER_HOST_IMAGE", "buzz-broker-host:smoke")
SPRIG_IMAGE = os.environ.get(
    "BUZZ_SPRIG_IMAGE",
    "ghcr.io/malkovitc/buzz-sprig@sha256:9f350ff80feeaf0ebfbccc603b18e6b75871ff54482c26c6ba9351ff9981c5ab",
)


class EmptyRelay(http.server.BaseHTTPRequestHandler):
    queries = 0

    def do_POST(self):
        if self.path != "/query":
            self.send_error(404)
            return
        length = int(self.headers.get("content-length", "0"))
        self.rfile.read(length)
        type(self).queries += 1
        self.send_response(200)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", "2")
        self.end_headers()
        self.wfile.write(b"[]")

    def log_message(self, _format, *_args):
        pass


def run(*args):
    subprocess.run(args, cwd=ROOT, check=True)


def capture(*args):
    return subprocess.check_output(args, cwd=ROOT, text=True).strip()


def agent_is_running(compose):
    cid = capture(*compose, "ps", "-q", "agent")
    return bool(cid) and capture("docker", "inspect", "-f", "{{.State.Running}}", cid) == "true"


def chown_fixture(path, uid, gid):
    run(
        "docker", "run", "--rm", "--user", "0",
        "--entrypoint", "/bin/sh",
        "--mount", f"type=bind,source={path},target=/fixture",
        BROKER_IMAGE, "-c",
        f"chown -R {uid}:{gid} /fixture/state /fixture/signing "
        f"/fixture/credential /fixture/workspace /fixture/authority.json",
    )


def wait_for_runtime(compose):
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        if agent_is_running(compose) and EmptyRelay.queries >= 2:
            return
        time.sleep(0.5)
    raise RuntimeError(f"ACP did not poll the synthetic relay (queries={EmptyRelay.queries})")


def wait_for_stop(compose):
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        cid = capture(*compose, "ps", "-a", "-q", "agent")
        if cid and capture("docker", "inspect", "-f", "{{.State.Running}}", cid) == "false":
            exit_code = capture("docker", "inspect", "-f", "{{.State.ExitCode}}", cid)
            if exit_code != "0":
                raise RuntimeError(f"ACP exited after quiesce with status {exit_code}")
            return
        time.sleep(0.5)
    raise RuntimeError("ACP remained active after quiesce")


def main():
    if sys.platform != "linux":
        raise SystemExit("test-runtime.py requires Linux bind-mount UID semantics")
    with tempfile.TemporaryDirectory(prefix="buzz-managed-agent-smoke-") as tmp:
        root = Path(tmp)
        relay_server = http.server.ThreadingHTTPServer(("0.0.0.0", 0), EmptyRelay)
        relay_thread = threading.Thread(target=relay_server.serve_forever, daemon=True)
        relay_thread.start()
        relay_url = f"ws://host.docker.internal:{relay_server.server_port}"
        compose = [
            "docker", "compose", "-p", f"buzz-smoke-{os.getpid()}",
            "-f", str(COMPOSE), "-f", str(root / "override.yml"),
            "--env-file", str(root / ".env"),
        ]
        (root / "override.yml").write_text(
            "services:\n  broker-host:\n    extra_hosts:\n"
            "      - host.docker.internal:host-gateway\n"
            "  agent:\n    environment:\n"
            "      BUZZ_AGENT_PROVIDER: openai\n"
            "      OPENAI_COMPAT_API_KEY: synthetic-smoke-only\n"
            "      OPENAI_COMPAT_MODEL: smoke\n"
            "      OPENAI_COMPAT_BASE_URL: http://127.0.0.1:1\n"
        )
        provisioned = False
        started = False
        try:
            run(
                "cargo", "run", "--locked", "--quiet", "-p", "buzz-broker-host",
                "--example", "managed_agent_fixture", "--", str(root), relay_url,
            )
            provisioned = True
            owner = (root / "owner.pubkey").read_text()
            (root / ".env").write_text(
                f"BUZZ_BROKER_HOST_IMAGE={BROKER_IMAGE}\n"
                f"BUZZ_SPRIG_IMAGE={SPRIG_IMAGE}\n"
                f"BUZZ_BROKER_STATE_DIR={root / 'state'}\n"
                f"BUZZ_BROKER_SIGNING_DIR={root / 'signing'}\n"
                f"BUZZ_BROKER_CREDENTIAL_DIR={root / 'credential'}\n"
                f"BUZZ_BROKER_AUTHORITY_FILE={root / 'authority.json'}\n"
                f"BUZZ_AGENT_WORKSPACE_DIR={root / 'workspace'}\n"
                f"BUZZ_RELAY_URL={relay_url}\n"
                "BUZZ_ACP_CHANNELS=5df7dfa8-e919-43df-8efd-f1dcb8af7071\n"
                f"BUZZ_ACP_AGENT_OWNER={owner}\n"
                "BUZZ_ACP_AGENT_COMMAND=buzz-agent\n"
            )
            chown_fixture(root, 1000, 1000)
            started = True
            run(
                *compose, "run", "--rm", "--no-deps", "broker-host",
                "--state", "/broker/state/authority.db", "issue",
                "--authority-file", "/broker/authority.json",
                "--credential-output", "/broker/credential/bearer",
            )
            run(*compose, "up", "-d", "--wait", "broker-host")
            run(*compose, "up", "-d", "agent")
            wait_for_runtime(compose)
            time.sleep(1)
            if not agent_is_running(compose):
                raise RuntimeError("ACP stopped before quiesce")
            run(
                *compose, "run", "--rm", "--no-deps", "broker-host",
                "--state", "/broker/state/authority.db", "quiesce",
                "--authority-file", "/broker/authority.json",
            )
            wait_for_stop(compose)
            run(
                *compose, "run", "--rm", "--no-deps", "broker-host",
                "--state", "/broker/state/authority.db", "fence",
                "--authority-file", "/broker/authority.json",
            )
            print("PASS: containerized ACP polled, stopped after quiesce, and fenced")
        finally:
            if started:
                subprocess.run([*compose, "down", "--remove-orphans"], cwd=ROOT, check=False)
            if provisioned:
                chown_fixture(root, os.getuid(), os.getgid())
            relay_server.shutdown()
            relay_server.server_close()


if __name__ == "__main__":
    main()
