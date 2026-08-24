#!/bin/sh
mode="${1:-healthy}"

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
    capabilities='[{"name":"test.echo","version":1}]'
    ;;
  missing-capability)
    plugin_id='mock.plugin'
    capabilities='[]'
    ;;
  duplicate-capability)
    plugin_id='mock.plugin'
    capabilities='[{"name":"test.echo","version":1},{"name":"test.echo","version":1}]'
    ;;
  *)
    plugin_id='mock.plugin'
    capabilities='[{"name":"test.echo","version":1}]'
    ;;
esac

protocol_version=1
if [ "$mode" = 'unsupported-protocol' ]; then
  protocol_version=2
fi

printf '{"jsonrpc":"2.0","id":1,"result":{"protocol_version":%s,"plugin":{"id":"%s","name":"Mock plugin","version":"0.1.0"},"capabilities":%s}}\n' "$protocol_version" "$plugin_id" "$capabilities"
if [ "$mode" = 'plugin-request' ]; then
  printf '%s\n' '{"jsonrpc":"2.0","id":99,"method":"plugin.request","params":{}}'
else
  printf '%s\n' '{"jsonrpc":"2.0","method":"mock.ready","params":{"ready":true}}'
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
