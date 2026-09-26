# stormlb

The Storm stack's load balancer, in Rust. One binary with two independent
halves:

- **The VIP half** (`[vip]`): a health-checked L4 (TCP) proxy for the
  control-plane VIP. VRRP (L2) or BGP anycast (L3) presents the VIP to the
  network. It is the pre-cluster kube-api VIP: it has to exist before the
  cluster does, and Cilium can't provide it because Cilium runs as a workload
  and can't front its own apiserver. It fills the `keepalived + haproxy` role
  from OpenShift on-prem.
- **The router** (`[router]`): the L7 half of inbound. It's a Host-header
  demux over Gateway API HTTPRoutes, standing where the wildcard
  `*.storm1.<zone>` DNS name lands (stormpump `docs/routing.md`).

**What ships today is the router alone.** The stormcos golden is configured
with `[router]` and nothing else (see [How it ships](#how-it-ships)). The VIP
half is implemented and unit-tested, but no shipped node runs it yet. See
[Gaps](#gaps-known-and-filed) before relying on it.

**Scope split:** stormlb owns inbound: the control-plane VIP and the Host
demux in front of node and cluster HTTP services. Cilium owns the in-cluster
Service data plane (eBPF LB, LB-IPAM, BGP / L2 announcements). Don't use the
L4 half for Service traffic.

Design notes (L2 vs L3, stormlb vs a DNS LB, why the router lives here) are in
[docs/design.md](docs/design.md).
A short deck on its purpose and functionality (Marp Markdown; render with
`npx @marp-team/marp-cli docs/presentation.md`) is
[docs/presentation.md](docs/presentation.md).

## Running it

```
stormlb [--config <path>]
```

| Flag | Env | Default |
|---|---|---|
| `-c`, `--config` | `STORMLB_CONFIG` | `/etc/stormlb/stormlb.toml` |

Logging goes to stderr through `tracing`. The filter comes from `RUST_LOG`
(default `info`). Per-connection failures (client gone, backend down) log at
`debug`.

At start it:

1. Reads and parses the TOML. A missing file or a parse error exits non-zero
   and names the path.
2. Resolves every `[[backend]]` to `ip:port`. A bad address exits.
3. Runs **router only** if there is no `[vip]` and there is a `[router]`.
   If neither is present it exits with `nothing to do`.
4. Otherwise it parses `vip.address` as IPv4 (exits if it can't), then starts
   the health loop, VRRP (if enabled), BGP (if enabled and its preflight
   passes), and the router (if present). Finally it runs the L4 balancer in
   the foreground.

## Configuration

TOML. Unknown keys are ignored. A fuller example is in
[`examples/stormlb.toml`](examples/stormlb.toml).

### `[router]`: the L7 Host router (present = enabled)

| Key | Default | Meaning |
|---|---|---|
| `listen` | `"0.0.0.0:80"` | Address to listen on. `"auto:<port>"` binds this node's routable IPv4 and `127.0.0.1` on `<port>`; see below. |
| `apiserver` | `"https://127.0.0.1:6443"` | Where HTTPRoutes and Services are read from. |
| `poll_secs` | `5` (min 1) | Seconds between route-table refreshes. |
| `insecure` | `true` | Accept the apiserver's certificate without a trusted CA. stormcert's CA is not in a trust store yet. |

`auto:<port>` picks the node's address by asking the routing table (a
connected UDP socket to `203.0.113.1`, so nothing is sent). It binds that
address plus loopback, and skips anything in `127.*` or `169.254.*`. It
exists because `0.0.0.0:80` includes `169.254.169.254:80`, which stormimds
(the metadata service) holds. Whichever started second crash-looped on
`EADDRINUSE`. If no address can be found it warns and falls back to
`0.0.0.0:<port>`.

### `[vip]`: the L4 balancer (present = enabled)

| Key | Default | Meaning |
|---|---|---|
| `address` | required | The VIP, IPv4. VRRP claims it and BGP advertises it. |
| `port` | required | Port the L4 proxy listens on (e.g. `6443`). |
| `bind` | `"0.0.0.0"` | Address the L4 proxy binds. The wildcard means it accepts whether or not this node currently holds the VIP. |

The next four sections are only read when `[vip]` is present.

### `[[backend]]`: the pool behind the VIP

| Key | Default | Meaning |
|---|---|---|
| `address` | required | IP literal. Hostnames aren't resolved and fail at start. |
| `port` | required | |

With no backends it starts, warns, and drops every connection.

### `[health]`

| Key | Default | Meaning |
|---|---|---|
| `mode` | `"tcp"` | `tcp`: a connect within the timeout passes. `http` / `https`: `GET <path>` and the status must be in `expect_status`. HTTPS doesn't verify certificates. |
| `path` | `"/readyz"` | A leading `/` is added if missing. |
| `interval_secs` | `2` (min 1) | Sleep between rounds. Backends are checked one after another within a round. |
| `timeout_secs` | `2` (min 1) | Per check. |
| `expect_status` | `[200]` | |

### `[vrrp]`: L2 VIP ownership

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `false` | |
| `interface` | `""` | Interface to advertise on and to hold the VIP on. Its first IPv4 (from `ip -o -4 addr show dev`) is the source address. |
| `vrid` | `51` | Virtual router ID. Must match across the nodes sharing the VIP. |
| `priority` | `100` | 1–255. `255` starts as Master (address owner). Everyone else starts Backup. |
| `advert_interval_secs` | `1` (min 1) | Master advertisement interval. Master-down = 3 × interval + skew. |

### `[bgp]`: L3 anycast advertisement

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `false` | |
| `local_asn` | `0` | Must be 1–65535. 2-byte ASNs only. |
| `router_id` | `""` | This node's IPv4. It is the BGP identifier **and** the next-hop advertised for the VIP. |
| `[[bgp.peers]]` `address`, `asn` | none | IPv4 peers. stormlb dials each on TCP 179. `asn` is logged, not checked against the peer's OPEN. |

If the preflight fails (bad ASN, no peers, `router_id` not IPv4), it logs
`bgp disabled: …` and everything else keeps running.

## What each half does

### Router

- Reads **one request head** (limit 16 KiB), takes the `Host` header
  (lowercased, port stripped) and looks it up. It then replays the head to
  the backend and splices bytes both ways. After the head it is just a wire,
  so keep-alive, chunked bodies, websockets and SSE work. Routing is
  **per connection**: a keep-alive connection that later names another host
  stays on the first backend.
- **The route table** is `GET {apiserver}/apis/gateway.networking.k8s.io/v1/httproutes`
  (all namespaces, no credentials sent), polled every `poll_secs` with a 10 s
  timeout. On error it keeps the last good table. Every `spec.hostnames`
  entry maps to one backend:
  1. the `storm.io/backend` annotation, `host:port` verbatim. This is how a
     node service routes: `127.0.0.1:9094` is "this node's console" on every
     node.
  2. otherwise `spec.rules[0].backendRefs[0]`, resolved via
     `GET /api/v1/namespaces/{ns}/services/{name}` to `clusterIP:port` (port
     default 80, namespace default = the route's). A headless or missing
     Service skips the route with a warning.
- Matching is **exact hostname only**. There are no wildcard hostnames, no
  path/header matches, and no rules or backendRefs beyond the first.
- Responses of its own:
  - `400` if there is no `Host`.
  - `404` `no route for host <h>` for a host no route claims.
  - `200` `router alive` for `GET /healthz` on any host no route claims.
    A host a route does claim gets its `/healthz` proxied to the backend.
  - If a backend can't be dialled, the connection closes with no response
    (logged at `debug`). There is no 502.

### L4 balancer

- Accepts on `bind:port` and picks a backend round-robin among the healthy
  ones. It then splices (TCP_NODELAY both ways).
- Backends **start unhealthy**; the first passing check admits them.
- With no healthy backend, or if the chosen backend refuses the connection,
  the client connection is closed. There is no retry on another backend.

### VRRP (L2)

VRRP v3 (RFC 5798) over a raw `IPPROTO_VRRP` (112) socket joined to
`224.0.0.18` on `interface`, TTL 255. It runs on its own OS thread.

- **Master:** sends an advertisement every interval. A peer with a higher
  priority, or equal priority and a higher address, demotes it to Backup,
  and it releases the VIP.
- **Backup:** if no advertisement arrives within master-down, it becomes
  Master and claims the VIP.

Claim is `ip addr add <vip>/32 dev <iface>` plus a best-effort
`arping -c 3 -A` (gratuitous ARP). Release is `ip addr del`. Both are
idempotent. It needs `CAP_NET_ADMIN` and `CAP_NET_RAW`. Known deviations from
the RFC are in [#7](https://github.com/glennswest/stormlb/issues/7).

### BGP (L3)

A minimal speaker: one outbound session per peer, reconnecting every 5 s
after failure. It sends OPEN (version 4, hold 180 s, no optional parameters)
and KEEPALIVE every 60 s. It announces `<vip>/32` (ORIGIN IGP, AS_PATH
`[local_asn]`, NEXT_HOP `router_id`) while this node has at least one healthy
backend, and withdraws it otherwise. Received messages are read and
discarded; a NOTIFICATION ends the session. IPv4 unicast only. Every healthy
node advertises the same /32, so the upstream router ECMP-hashes across them.
Announce/withdraw latency is up to 60 s today
([#6](https://github.com/glennswest/stormlb/issues/6)).

## Ports and endpoints

| Port | Proto | Who | When |
|---|---|---|---|
| `[router] listen`: **80** in the golden | TCP, HTTP/1.x | clients via `*.storm1.<zone>`; stormd's liveness probe on `127.0.0.1:80/healthz` | `[router]` present |
| `[vip] port`, e.g. 6443 | TCP | kube-api clients via the VIP | `[vip]` present |
| n/a | IP proto 112 to `224.0.0.18` | VRRP peers | `[vrrp] enabled` |
| 179 (outbound only) | TCP | BGP peers | `[bgp] enabled` |
| 180 in the golden | HTTP | **stormd's** API, not stormlb's (port + 100, stormcentral's convention) | golden |

**Health:** the router's `/healthz` (above) is the only endpoint. The L4
half has no health endpoint of its own; a node's VIP state is visible in
the logs and with `ip addr`.

**Metrics:** none. stormlb exports no metrics endpoint. stormd's API
reports the process's restarts and liveness failures.

## How it ships

stormlb is a stormcentral component (`components/stormcos.toml` in
stormcentral):

- `kind = "service"`: a `stormlb` golden (32 MiB, pallet `system1`) with the
  static musl binary at `/usr/sbin/stormlb` in a stormd base. stormd runs it
  as a process with `--config /etc/stormlb/stormlb.toml`, restarts it on
  exit, and probes `http://127.0.0.1:80/healthz`. There are also
  `stormlb-data` (64 MiB, `data1`, at `/var/lib/stormlb`) and `stormlb-logs`
  (64 MiB, `system1`, at `/var/log/stormd`).
- The config baked into the golden is **router only**:

  ```toml
  [router]
  listen = "auto:80"
  ```

- stormcentral builds the golden from an exact pushed commit with
  `cargo build --release --locked --target x86_64-unknown-linux-musl` on
  `dev.g8.lo`, as an unprivileged build user. That's why `Cargo.lock` is
  committed.
- On a stormcos node there is no systemd: stormpump is PID 1. stormcos's
  `deploy/build-goldens.sh` writes a `spec stormlb` stanza into
  `/etc/stormpump/boot.d/40-services`. It is a container on the host network
  profile, sharing UTS, with its data and log volumes. On the `sno`
  profile (the default) it also writes `start stormlb`. A node runs the
  router because it has that `start` line; the `node` and `storage`
  profiles get the spec without it.
  stormcos `deploy/image.toml` places the three goldens.

## Build and test

Nothing builds on the session VM, and nothing needs root. Push, then:

```
sc-build                                        # cargo build && cargo test on dev.g8.lo
sc-build 'cargo build --release --locked && cargo test --locked'
```

`sc-build` fetches the pushed commit into a scratch directory on `dev.g8.lo`
as `stormbuild`, builds it and deletes it. A failure files a `build-failure`
issue here. `Cargo.lock` is committed, and `cargo update` is a deliberate
commit of its own. A new golden is requested with
`stormcentral component build stormlb --url http://stormcentral.g8.lo`.

Tests (25): config parsing and defaults, pool round-robin and health
filtering, TCP health check, the VRRP state machine and advertisement
encode/parse/checksum, BGP OPEN/UPDATE/withdraw encoding, router header
parsing, the router's own `/healthz` bytes on a real socket, and an
integration test (`tests/balancer.rs`) for round-robin plus failover through
the real L4 proxy. VRRP and BGP on the wire (raw socket,
iproute2, a real peer) are not covered by tests.

### Tests on a node: the test container

[`test/`](test/) is stormlb's test container, per stormcentral
`docs/test-standard.md`. It tests what a node runs, which is the router,
from outside, through the apiserver and port 80. It is a crate of its own
(own workspace and `Cargo.lock`, never part of the golden's build), built
into `stormlb-test-<suite>` by `test/Containerfile` (`FROM scratch`, static
musl) and run as a Job by [`test/stormlb-test.yaml`](test/stormlb-test.yaml).

| Suite | Budget | What it proves |
|---|---|---|
| `short` | < 2 min | `/healthz` answers; stormd (`:180/metrics`) reports stormlb running; an HTTPRoute's hostname reaches its backend, and is a 404 again once deleted. |
| `medium` | < 30 min | 400 without a Host, the 404 that names the host, `/healthz` on unclaimed and claimed hosts and its CRLF line endings, Host case and port, per-connection routing, a streamed response not held back, an Upgrade as a two-way pipe, an 8 MiB body, the 16 KiB head limit, a dead backend closing with no response, a route update, a backendRef through a Service (skip without a Service data plane), a headless Service's route skipped, 50 hosts under concurrent load, deleted routes back to 404, no restart or crash under stormd. The VIP half is reported skip: it is not shipped. |
| `long` | the night window | Waves sized from the node's allocatable CPU (read from the API) and the container's open-file limit. Each wave creates routes, holds connections at that size, drains, and checks residue. Each `wave-<n>` line carries route-programming time, request p50/p99, requests/s, drain time, leftovers, restarts and idle latency. `trend` fails on the first wave that is slower than the first wave of its size. |

- **Backends** are listeners in the test pod. The Job is `hostNetwork`, and
  the routes name the pod's backends in `storm.io/backend`, the same path a
  node service uses. The suites create only HTTPRoutes, Services and
  Endpoints, all in the run's namespace and labelled `storm.io/test-run`,
  and delete them at the end (`cleanup`).
- **Environment:** the standard's `STORM_*` variables. The router defaults to
  `STORM_NODE:80` and stormd to `STORM_NODE:180`; `STORMLB_ROUTER` and
  `STORMLB_STORMD` (`none` for no stormd) override them. `STORMLB_ROUTE_WAIT`
  (default 30 s) is how long a route change may take, and `STORMLB_SETTLE`
  (12 s) is how long to wait before calling something *not* routed.
- **Where stormlb isn't started:** stormcos starts it on the `sno` profile
  only. When neither the router nor its stormd answers, a suite reports one
  `stormlb-started` skip, never a pass. If stormd answers and the router
  doesn't, that's a failure.
- **Not observable yet:** the router's own memory and file descriptors.
  stormd's open `/metrics` reports stormd's, not the process it supervises.
- **The harness:** `test/tests/harness.rs` runs the three suites against the
  real router (`stormlb::router::run`) and a small in-memory apiserver, all
  on loopback. That's how the container's own code is tested:

  ```
  sc-build 'cd test && cargo test --locked'
  ```

  stormcentral does not run these Jobs yet: the runner is stormcentral#41.

## Gaps (known and filed)

What the code does not do yet, which older docs implied it did:

- [#6](https://github.com/glennswest/stormlb/issues/6): BGP reconciles
  announce/withdraw only on the 60 s keepalive tick. It doesn't wait for
  Established or enforce a hold timer.
- [#7](https://github.com/glennswest/stormlb/issues/7): a VRRP Backup never
  preempts a lower-priority Master, and priority 0 isn't handled. VIP
  ownership ignores backend health. It relies on `ip` and `arping` binaries.
- Earlier follow-ups: 4-octet ASNs and multiprotocol BGP, a priority-0 VRRP
  resign on shutdown, and netlink-native VIP control.
