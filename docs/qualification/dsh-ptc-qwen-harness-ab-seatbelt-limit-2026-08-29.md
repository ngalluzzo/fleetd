# DSH/PTC versus OpenCode: macOS Seatbelt bind limitation

Date: 2026-08-29  
Parent record: [blocked qualification](dsh-ptc-qwen-harness-ab-blocked-2026-08-29.md)  
Verdict: **stopped at prerequisite B; the required boundary is not expressible with this Seatbelt mechanism**

## Outcome

The resumed qualification stopped before source edits, runtime re-homing, harness
boots, work messages, or inference. On the deployed macOS, the narrowest accepted
Seatbelt TCP listener rule is not loopback-only or ephemeral-only:

```scheme
(allow network-bind (local tcp "localhost:*"))
(allow network-inbound (local tcp "localhost:*"))
```

It permits the intended `127.0.0.1:0` and `::1:0` listeners, but also permits
`0.0.0.0:0`, the machine's external-interface address at port `0`, and a fixed
loopback port. Replacing `localhost` with a numeric loopback address is rejected by
the Seatbelt profile parser. Replacing `*` with port `0` or a port range is also
rejected. Granting only `network-bind` does not suffice: `listen()` fails until the
matching `network-inbound` grant is present.

This is exactly the unsafe approximation the prerequisite prohibited. Fleetd did
not add a generic `network-bind`, raw SBPL, a misleading typed
`loopback_bind: ephemeral` switch, or an unsandboxed OpenCode exception.

Because the requested protocol said to stop if Seatbelt could not express the
address restriction, prerequisite A was not implemented after this fail-fast result.
The pinned DSH source/runtime, Fleetd sandbox source, qualification profiles, and
prior blocked evidence remain unchanged by this resumed attempt. The A/B is still
blocked and carries no DSH-versus-OpenCode model evidence.

## Platform identity

- macOS `26.5.2` (`25F84`)
- Darwin `25.5.0 arm64`
- `/usr/bin/sandbox-exec` SHA-256:
  `8290e4be7387a0df83cd1559e86afd880464f269450573d012795761fe298f16`
- `/usr/bin/nc` SHA-256:
  `427423db6d5d5e9f720c5e110a2c9b3cba39ea089dafed4ab936d04dd218bdac`
- tested external interface: `en0`, `192.168.1.184`

The test used the same deny-default process, filesystem, IPC, and signal baseline as
Fleetd's current `MacOsSeatbeltSandbox`, then varied only the listener rules.

## Empirical matrix

With the accepted `localhost:*` TCP bind and inbound rules above:

| Attempt | Expected contract result | Observed | Safe? |
| --- | --- | --- | --- |
| TCP `127.0.0.1:0` | allow | allowed | yes |
| TCP `::1:0` | allow where supported | allowed | yes |
| TCP `0.0.0.0:0` | deny | **allowed** | no |
| TCP `192.168.1.184:0` | deny | **allowed** | no |
| TCP `127.0.0.1:43123` | deny | **allowed** | no |
| UDP `127.0.0.1:0` | deny as unrelated inbound | denied | yes |

The listener probe starts `/usr/bin/nc` under `sandbox-exec`, waits 200 ms, and
records success only while the listener process remains alive. Allowed listeners are
then killed inside the same sandbox. The UDP negative control exited with
`Operation not permitted`.

With only:

```scheme
(allow network-bind (local tcp "localhost:*"))
```

the TCP listener failed with:

```text
nc: listen: Operation not permitted
```

Adding the matching `network-inbound` rule made it succeed. Therefore omitting
`network-inbound` cannot be used to regain restriction while still supporting
OpenCode's private ACP server.

## Parser limits

The deployed parser rejects exact numeric loopback hosts:

```text
(local tcp "127.0.0.1:*")
sandbox-exec: host must be * or localhost in network address

(local tcp "[::1]:*")
sandbox-exec: host must be * or localhost in network address
```

It also rejects port zero and ranges:

```text
(local tcp "localhost:0")
sandbox-exec: invalid port in network address

(local tcp "localhost:49152-65535")
sandbox-exec: invalid port in network address
```

Apple's installed profiles use the same coarse forms: exact fixed ports,
`"*:*"`, or unparameterized `(local ip)`. No installed example supplies an
address-and-ephemeral-port restriction that the parser accepts.

## Exact listener probe

The following is the exact shape used for the final matrix. `BASELINE` below stands
for Fleetd's existing deny-default process/filesystem rules plus a canonical private
temporary writable root; no repository directory is writable or newly readable.

```sh
profile="$BASELINE
(allow network-bind (local tcp \"localhost:*\"))
(allow network-inbound (local tcp \"localhost:*\"))"

probe_listener() {
  probe_label=$1
  shift
  (
    cd "$sbx_probe_dir" || exit 97
    /usr/bin/sandbox-exec -p "$profile" /bin/sh -c \
      '"$@" & probe_pid=$!; sleep 0.2;
       if kill -0 "$probe_pid"; then
         kill -9 "$probe_pid"; wait "$probe_pid"; exit 0;
       else
         exit 1;
       fi' sh "$@"
  )
  probe_exit=$?
  printf '%s=%s\n' "$probe_label" "$probe_exit"
}

probe_listener ipv4_loopback_ephemeral /usr/bin/nc -l 127.0.0.1 0
probe_listener ipv6_loopback_ephemeral /usr/bin/nc -6 -l ::1 0
probe_listener ipv4_wildcard_ephemeral /usr/bin/nc -l 0.0.0.0 0
probe_listener external_interface_ephemeral /usr/bin/nc -l 192.168.1.184 0
probe_listener ipv4_loopback_fixed /usr/bin/nc -l 127.0.0.1 43123
probe_listener unrelated_udp_inbound /usr/bin/nc -u -l 127.0.0.1 0
```

Observed exit codes:

```text
ipv4_loopback_ephemeral=0
ipv6_loopback_ephemeral=0
ipv4_wildcard_ephemeral=0
external_interface_ephemeral=0
ipv4_loopback_fixed=0
unrelated_udp_inbound=1
```

Every temporary probe directory and listener process was removed after the test.

## Containment and no-work evidence

- No Fleetd, DSH, OpenCode, GOOI, profile-catalog, or runtime-store source/config
  file was edited in this resumed attempt.
- Only this adjacent report and its JSON manifest were created.
- No content-addressed DSH re-home was attempted after prerequisite B failed.
- No qualification seat was restarted; the two prior seats remain stopped at
  revision 2.
- No model-free harness boot was attempted.
- No work message or invocation was created.
- No positive-control or seq 119 artifact was created.
- The shared MLX observer still reported `requests_started: 0`,
  `requests_completed: 0`, `prompt_tokens_total: 0`,
  `completion_tokens_total: 0`, and `latest: null` at the final checkpoint.

## What would be required to resume

The boundary must change mechanisms rather than rename this Seatbelt grant. Viable
design investigations include a Fleetd-owned pre-bound loopback socket passed by file
descriptor to a runtime that can adopt it, or a separately sandboxed loopback broker
whose public contract contains no arbitrary bind operation. Either requires explicit
runtime support and its own threat model, digest, and real negative tests. A VM or
another OS sandbox with enforceable address/port policy is also a distinct candidate.

Do not resume the DSH runtime re-home or the A/B until one mechanism can prove all of:

1. `127.0.0.1:0` and `::1:0` private listeners work;
2. wildcard and external-interface listeners fail;
3. fixed undeclared loopback ports fail;
4. unrelated inbound operations remain denied;
5. OpenCode 1.4.0 ACP initializes and shuts down with no model request.

The current Seatbelt filter cannot provide that proof, so no typed capability should
be exposed for it.
