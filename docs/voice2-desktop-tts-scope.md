# VOICE 2 Desktop TTS canary scope

Base: draft PR #52 (`c12999c`). This slice changes only TTS broadcast for the
one local managed agent selected by `BUZZ_DESKTOP_BROKER_AGENT_PUBKEY`.

## Decision

Desktop reads the selected runtime's exact broker listener, bearer, and
authority from its private generation files under a short runtime lock. It
releases that lock before network I/O. The existing owner key signs one fresh
kind `27236` challenge grant for the current ephemeral Huddle channel, parent
channel, and Huddle lifetime. Desktop then opens the broker's send-only audio
socket with the same bearer. The broker forwards only the relay `peer_index`
in its joined control frame so Desktop's existing echo suppression works.
Desktop rechecks the exact broker generation and current Huddle lifetime before
registering the publisher. Stale sockets are cancelled. Legacy agents retain
their direct NIP-42 TTS path.

Alternatives considered: retaining another bearer copy in `BrokerHost` adds a
long-lived in-memory credential; passing an agent key to the media client
bypasses broker authority. Reading the already durable mode-0600 credential
file avoids another credential owner.

## Gates

- A selected agent with no tracked live broker generation fails closed; it
  never falls through to direct nsec signing.
- Owner TTS toggle, active Huddle lifetime, exact canonical channel IDs,
  local bot membership, per-agent voice consent, and the existing TTS speaker
  lease remain required. Disabling one agent invalidates its queued text and
  cancels its publisher before a later enable can admit new speech.
Pubkeys are canonicalized at speech and voice-setting boundaries; case aliases
merge with disabled consent winning, including when a voice becomes unavailable.
- A broker generation fenced during challenge, grant, or audio handshake
  cannot register a publisher or emit a queued frame.
- The broker connection uses bounded frames and the existing Opus encoder,
  queue, cancellation, and echo-suppression path. A blocked audio send exits
  when the publisher is cancelled.

No provider adapter, cloud bridge, relay protocol change, deployment, or CI
configuration change is in this slice.

## Proof boundary

Focused proofs compose three seams: broker-host rejects old grants and stops
relay-observed ingress after quiesce; Desktop's real-host snapshot→stop test
refuses the final registration callback; an unregistered Desktop publisher
cancels its socket task on Drop. The fake broker test verifies Desktop's owner
signature, exact grant target, bearer WebSocket, and `peerIndex` handling.
The per-agent consent tests and existing speaker-generation test cover the
toggle-off predicate and queued-text invalidation separately; code orders
both with the Huddle lock. The blocked-send test holds a sink pending until
publisher cancellation.
Case-alias tests cover uppercase speech after a lowercase toggle-off and
preexisting mixed-case settings with an unavailable voice.

This draft does not contain a single Tauri `AppState` race harness or a real
Desktop→broker→relay Huddle observation. Those remain canary release gates,
along with the provider/latency run. VOICE 2 is not closed by this slice.
