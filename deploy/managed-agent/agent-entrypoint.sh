#!/bin/sh
set -eu

credential_file=/run/buzz/credential/bearer
authority_file=/run/buzz/authority.json
for input in "$credential_file" "$authority_file"; do
    if [ ! -f "$input" ] || [ ! -r "$input" ]; then
        echo "managed agent input is unavailable" >&2
        exit 1
    fi
done
if [ "$(stat -c '%a:%u' "$credential_file")" != '600:1000' ]; then
    echo "managed agent credential must be mode 0600 and owned by uid 1000" >&2
    exit 1
fi

unset BUZZ_PRIVATE_KEY BUZZ_ACP_PRIVATE_KEY NOSTR_PRIVATE_KEY BUZZ_AUTH_TAG BUZZ_RELAY_URL
BUZZ_BROKER_CREDENTIAL="$(cat "$credential_file")"
BUZZ_MANAGED_ACP_AUTHORITY="$(cat "$authority_file")"
if [ -z "$BUZZ_BROKER_CREDENTIAL" ] || [ -z "$BUZZ_MANAGED_ACP_AUTHORITY" ]; then
    echo "managed agent input is empty" >&2
    exit 1
fi
export BUZZ_BROKER_CREDENTIAL BUZZ_MANAGED_ACP_AUTHORITY
exec /usr/local/bin/sprig-entrypoint "$@"
