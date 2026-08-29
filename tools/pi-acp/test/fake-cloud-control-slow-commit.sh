#!/bin/sh
set -eu
input=$(cat)
operation='11111111-1111-4111-8111-111111111111'
case "$input" in
  *'"phase":"prepare"'*)
    printf '%s\n' "{\"status\":\"ok\",\"content\":\"CONTROL_PREPARED\",\"operationId\":\"$operation\"}"
    ;;
  *'"phase":"commit"'*)
    sleep 1
    printf '%s\n' "{\"status\":\"noop\",\"content\":\"COMMIT_QUEUED\"}"
    ;;
  *) exit 3 ;;
esac
