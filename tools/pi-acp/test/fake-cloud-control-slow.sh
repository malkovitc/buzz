#!/bin/sh
set -eu
cat >/dev/null
sleep 5
printf '%s\n' '{"status":"ok","content":"TOO_LATE"}'
