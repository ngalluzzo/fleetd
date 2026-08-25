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
    offers='[{"implementation":{"package":"mock.plugin","name":"echo","version":"0.1.0"},"capability":{"package":"dev.fleetd.test","name":"echo","version":"1.0.0"},"implementation_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]'
    ;;
  missing-capability)
    plugin_id='mock.plugin'
    offers='[{"implementation":{"package":"mock.plugin","name":"other","version":"0.1.0"},"capability":{"package":"dev.fleetd.test","name":"other","version":"1.0.0"},"implementation_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]'
    ;;
  duplicate-capability)
    plugin_id='mock.plugin'
    offers='[{"implementation":{"package":"mock.plugin","name":"echo","version":"0.1.0"},"capability":{"package":"dev.fleetd.test","name":"echo","version":"1.0.0"},"implementation_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},{"implementation":{"package":"mock.plugin","name":"echo","version":"0.1.0"},"capability":{"package":"dev.fleetd.test","name":"echo","version":"1.0.0"},"implementation_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]'
    ;;
  *)
    plugin_id='mock.plugin'
    offers='[{"implementation":{"package":"mock.plugin","name":"echo","version":"0.1.0"},"capability":{"package":"dev.fleetd.test","name":"echo","version":"1.0.0"},"implementation_digest":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]'
    ;;
esac

protocol_version=1
if [ "$mode" = 'unsupported-protocol' ]; then
  protocol_version=2
fi

printf '{"jsonrpc":"2.0","id":1,"result":{"protocol_version":%s,"plugin":{"id":"%s","name":"Mock plugin","version":"0.1.0"},"capability_offers":{"protocol":"org.gooi.capability.offers/v1","package":{"package":"mock.plugin","name":"package","version":"0.1.0"},"offers":%s}}}\n' "$protocol_version" "$plugin_id" "$offers"
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
