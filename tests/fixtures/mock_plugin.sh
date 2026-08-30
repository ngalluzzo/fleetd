#!/bin/sh
mode="${1:-healthy}"

if [ "$mode" = 'sandbox-write' ]; then
  printf '%s\n' 'discarded' > /dev/null || exit 20
  printf '%s\n' 'allowed' > "$2" || exit 21
  if printf '%s\n' 'escaped' > "$3"; then
    exit 22
  fi
fi

if [ "$mode" = 'sandbox-write-scoped' ]; then
  test "$(cat "$5")" = 'runtime-readable' || exit 23
  printf '%s\n' 'workspace' > "$2" || exit 24
  printf '%s\n' 'state' > "$3" || exit 25
  printf '%s\n' 'temp' > "$4" || exit 26
  printf '%s\n' 'discarded' > /dev/null || exit 27
  /bin/sh -c '
    printf "%s\n" descendant > "$1" || exit 31
    if printf "%s\n" escaped > "$2"; then
      exit 32
    fi
  ' sh "$7" "$6" || exit 28
  /usr/bin/python3 -c '
import errno
import os
import sys

target, evidence = sys.argv[1:]
flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
try:
    descriptor = os.open(target, flags, 0o600)
except OSError as error:
    name = errno.errorcode.get(error.errno, "UNKNOWN")
    with open(evidence, "w", encoding="utf-8") as output:
        output.write(
            f"os.open flags=O_WRONLY|O_CREAT|O_TRUNC errno={error.errno} "
            f"name={name} message={error.strerror}\n"
        )
    if error.errno != errno.EPERM:
        raise
else:
    os.close(descriptor)
    raise SystemExit(33)
' "$6" "$8" || exit 30
  /usr/bin/nc -l 127.0.0.1 0 &
  listener_pid=$!
  /bin/sleep 0.1
  kill -0 "$listener_pid" || exit 29
  kill "$listener_pid"
  wait "$listener_pid" || true
fi

if [ "$mode" = 'never-read' ]; then
  /bin/sleep 5
  exit 13
fi

IFS= read -r initialize || exit 10

case "$mode" in
  hang)
    /bin/sleep 5
    exit 11
    ;;
  malformed)
    printf '%s\n' 'this is not json'
    exit 12
    ;;
  wrong-id)
    plugin_id='wrong.plugin'
    interfaces='[{"id":"fleetd.test.echo","version":"1.0.0"}]'
    ;;
  missing-interface)
    plugin_id='mock.plugin'
    interfaces='[{"id":"fleetd.test.other","version":"1.0.0"}]'
    ;;
  duplicate-interface)
    plugin_id='mock.plugin'
    interfaces='[{"id":"fleetd.test.echo","version":"1.0.0"},{"id":"fleetd.test.echo","version":"1.0.0"}]'
    ;;
  *)
    plugin_id='mock.plugin'
    interfaces='[{"id":"fleetd.test.echo","version":"1.0.0"}]'
    ;;
esac

protocol_version=1
if [ "$mode" = 'unsupported-protocol' ]; then
  protocol_version=2
fi

printf '{"jsonrpc":"2.0","id":1,"result":{"protocol_version":%s,"plugin":{"id":"%s","name":"Mock plugin","version":"0.1.0"},"interfaces":%s}}\n' "$protocol_version" "$plugin_id" "$interfaces"
if [ "$mode" = 'plugin-request' ]; then
  printf '%s\n' '{"jsonrpc":"2.0","id":99,"method":"plugin.request","params":{}}'
else
  printf '%s\n' '{"jsonrpc":"2.0","method":"mock.ready","params":{"ready":true}}'
fi
if [ "$mode" = 'descendant' ] || [ "$mode" = 'orphan-descendant' ]; then
  /bin/sleep 30 &
  descendant_pid=$!
  printf '{"jsonrpc":"2.0","method":"mock.descendant","params":{"pid":%s}}\n' "$descendant_pid"
fi

request_id=2
while IFS= read -r request; do
  case "$request" in
    *'"method":"fleetd.health"'*)
      if [ "$mode" = 'unhealthy' ]; then
        printf '{"jsonrpc":"2.0","id":%s,"result":{"status":"degraded"}}\n' "$request_id"
      else
        printf '{"jsonrpc":"2.0","id":%s,"result":{"status":"ok"}}\n' "$request_id"
      fi
      if [ "$mode" = 'crash-after-health' ]; then
        exit 17
      fi
      if [ "$mode" = 'orphan-descendant' ]; then
        printf '%s\n' '{"jsonrpc":"2.0","method":"mock.outer_exiting","params":{}}'
        exit 17
      fi
      ;;
    *'"method":"fleetd.shutdown"'*)
      printf '{"jsonrpc":"2.0","id":%s,"result":{"accepted":true}}\n' "$request_id"
      if [ "$mode" = 'force-shutdown' ]; then
        /bin/sleep 5
      fi
      exit 0
      ;;
    *)
      printf '{"jsonrpc":"2.0","id":%s,"error":{"code":-32601,"message":"method not found"}}\n' "$request_id"
      ;;
  esac
  request_id=$((request_id + 1))
done
