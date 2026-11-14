# VOICE 2 local Desktop media proof

Base: draft PR #53 (`2af624b`). This is a test and observation slice only.

## Expected observation

For one generated owner, agent, and local broker generation, fixed fixture UUIDs
for the ephemeral and parent channels, and a fixed Huddle generation, the
Desktop broker TTS publisher
obtains an owner grant, sends one encoded v2 frame to the broker, and an
observing single local relay records that frame under the agent identity.
After broker quiesce returns and fence commits, that relay records zero
additional frames for the old generation; its observer has acknowledged a
terminal Close. The broker's joined message must attest local-only audio; no
mesh path participates.

## Failure criterion

The proof fails if grant, identity, channel, Huddle generation, or local-only
attestation differs; if the first frame is not observed and parsed as v2; if
quiesce returns before the relay's close response; if the relay observes an
extra frame before terminal Close; or if fence succeeds while the old audio
session is still open. A Desktop runtime-map stop/registration race test must
reject the final broker-media callback after the tracked generation stops.
The publisher's receiver and socket lease must end while its handle is held.

## Scope and build constraint

Use generated fixture identities and isolated local ports/state. Do not use a
real provider or production Desktop/relay data. Reuse the existing Desktop
Cargo cache and avoid a second broad target; stop any build before free disk
falls below 5 GiB. The single-run proof and the AppState race harness are
attempted only where current test seams permit them without adding a new
framework or changing production behavior.

## Implemented observation and limit

An ignored Desktop test launches the current `buzz-broker-host` binary with
generated fixture identities and an owner-only private directory. Desktop's
real broker TTS connector obtains the owner grant and its existing Opus queue
sends synthetic PCM as one v2 frame. An in-process Axum relay observer checks
the broker-authenticated agent pubkey, protocol v2, and local broker marker;
it declares local-only audio and records frames. The observer holds its Close
response while a real `AppState` runtime-map stop runs. A concurrent final
media-registration guard waits behind that lock, then rejects its callback
after fence. The old publisher handle queues another PCM packet while the
relay's Close is held, but this test does not prove its send task attempted a
network write before closing. The observer's terminal Close barrier and frame
channel record no later old-generation frame. While the old publisher handle
is still held, bounded waits confirm its channel receiver has closed and its
local socket lease has been released. This observes task teardown after broker
stop without claiming that the queued second packet reached a network send.

The relay observer is deliberately synthetic. It does not exercise the real
`buzz-relay` Postgres writer-backed bot-role gate or recipient fanout, and the
test does not construct a Tauri `AppHandle`, a `TtsPipeline`, or a provider.
The registration callback here is an `AtomicBool` behind the real broker
runtime-map guard; it does not enter `register_if_current_huddle` or
`TtsBroadcasters`. Those remain separate live canary evidence requirements.

## Local replay

From this checkout, point `CARGO_TARGET_DIR` at the existing shared Desktop
cache. Build the matching host first and copy it to a private temp directory:
Tauri's sidecar packaging can replace `target/debug/buzz-broker-host` with a
test placeholder during the Desktop build.

```bash
. ./bin/activate-hermit
export CARGO_TARGET_DIR=/Users/malkovitc/.buzz/REPOS/.worktrees/voice2-local-broker-sidecar/desktop/src-tauri/target
cargo build -p buzz-broker-host --bin buzz-broker-host
PROOF_BIN_DIR="$(mktemp -d)"
install -m 700 "$CARGO_TARGET_DIR/debug/buzz-broker-host" "$PROOF_BIN_DIR/buzz-broker-host"
BUZZ_TEST_BROKER_HOST_BIN="$PROOF_BIN_DIR/buzz-broker-host" cargo test --manifest-path desktop/src-tauri/Cargo.toml --lib desktop_audio_reaches_local_relay_and_stops_before_fence -- --ignored
```
