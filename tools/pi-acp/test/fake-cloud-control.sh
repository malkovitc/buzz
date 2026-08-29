#!/bin/sh
set -eu

input=$(cat)
if [ "${BUZZ_PRIVATE_KEY+x}" = x ]; then
  echo 'secret leaked to cloud control' >&2
  exit 2
fi
operation='11111111-1111-4111-8111-111111111111'
case "$input" in
  *'"phase":"prepare"'*'"command":"-status"'*)
    printf '%s\n' '{"status":"noop","content":"STATUS_LOCAL branch=cloud/handoff-test head=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}'
    ;;
  *'"phase":"prepare"'*)
    printf '%s\n' "{\"status\":\"ok\",\"content\":\"CONTROL_PREPARED\",\"operationId\":\"$operation\"}"
    ;;
  *'"phase":"commit"'*'"operationId":"11111111-1111-4111-8111-111111111111"'*'"receiptEventId":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"'*)
    printf '%s\n' "{\"status\":\"ok\",\"content\":\"COMMIT_QUEUED\",\"operationId\":\"$operation\"}"
    ;;
  *)
    echo 'invalid cloud control phase binding' >&2
    exit 3
    ;;
esac
