#!/bin/sh
set -eu

cat >/dev/null
if [ "${BUZZ_PRIVATE_KEY+x}" = x ]; then
  echo 'secret leaked to cloud control' >&2
  exit 2
fi
printf '%s\n' '{"status":"ok","content":"STATUS_LOCAL branch=cloud/handoff-test head=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}'
