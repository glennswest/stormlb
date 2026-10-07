# stormlb

The Storm stack's load balancer, in Rust. One binary with two independent
halves:

- **The VIP half** (`[vip]`, and the VIP API `[api]`): a health-checked L4
  (TCP) proxy for the control-plane VIP. VRRP (L2) or BGP anycast (L3)
  presents the VIP to the network. VIPs can also be made and changed at
  runtime through a small HTTP API, which is how stormcluster keeps a
  cluster's API VIP in front of whichever nodes are masters
  ([VIP API](#vip-api)). It is the pre-cluster kube-api VIP: it has to exist before the
  cluster does, and Cilium can't provide it because Cilium runs as a workload
  and can't front its own apiserver. It fills the `keepalived + haproxy` role
  from OpenShift on-prem.
- **The router** (`[router]`): the L7 half of inbound. It's a Host-header
  demux over Gateway API HTTPRoutes, standing where the wildcard
  `*.storm1.<zone>` DNS name lands (stormpump `docs/routing.md`).

**What ships today is the router and the VIP API.** The stormcos golden is
configured with `[router]` and `[api]` (see [How it ships](#how-it-ships)), so
a node serves no VIP until stormcluster makes one through the API. The VIP
half is implemented and tested, but no shipped node serves a VIP yet. See
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
2. Exits with `nothing to do` if none of `[vip]`, `[router]` and `[api]` is
   present.
3. Starts `[vip]` as the VIP named `default`: its `[[backend]]` pool, the
   health loop, the L4 listener and VRRP (if enabled). A bad address, a bad
   `ca_file` or a listener that can't bind exits non-zero. Then BGP (if
   enabled and its preflight passes).
4. With `[api]`: starts the VIPs saved in `state_file` (one that no longer
   starts is logged and kept in the file), then the API.
5. Runs the router (if present). With the router alone, its exit is the
   process's; beside a VIP or the API, a router failure is only logged.

## Configuration

TOML. Unknown keys are ignored. A fuller example is in
[`examples/stormlb.toml`](examples/stormlb.toml).

### `[router]`: the L7 Host router (present = enabled)

| Key | Default | Meaning |
|---|---|---|
| `listen` | `"0.0.0.0:80"` | Address to listen on. `"auto:<port>"` binds this node's routable IPv4 and `127.0.0.1` on `<port>`; see below. |
| `apiserver` | `"https://127.0.0.1:6443"` | Where HTTPRoutes and Services are read from. |
| `poll_secs` | `5` (min 1) | Seconds between route-table refreshes. |
| `backend_ca_file` | none | The CA (PEM) a route's backend must chain to when the route says `storm.io/backend-protocol: https`; on a node, the cluster CA `/data/stormcert/ca.crt`. Only these certificates are trusted, not the public roots. Re-read on the route poll when it changes, so it may appear after the router starts. Unset or not loadable: https backends' connections fail closed. |
| `token_file` | none | The router's identity (#9): a file holding a bearer token, its ServiceAccount's, which stormcos mints with get/list/watch on HTTPRoutes and Services (stormcos#76 step 2). It's sent as `Authorization: Bearer` and re-read on every poll, so it can be rotated in place. Set but missing or empty: no refresh is made, and the last table keeps serving. Unset: anonymous, as before. |
| `ca_file` | none | The CA (PEM) the apiserver's certificate must chain to, on a node `/data/stormcert/ca.crt` (#10). Only it is trusted, not public roots. Re-read when it changes, so it may appear after the router starts. When set, `insecure` is ignored. |
| `insecure` | `true` | Without `ca_file`: accept the apiserver's certificate unverified. `false` without `ca_file` trusts only the public roots compiled in, which refuses stormcert's certificate. |

`auto:<port>` picks the node's address by asking the routing table (a
connected UDP socket to `203.0.113.1`, so nothing is sent). It binds that
address plus loopback, and skips anything in `127.*` or `169.254.*`. It
exists because `0.0.0.0:80` includes `169.254.169.254:80`, which stormimds
(the metadata service) holds. Whichever started second crash-looped on
`EADDRINUSE`. If no address can be found it warns and falls back to
`0.0.0.0:<port>`.

### `[router.tls]`: TLS for routed hosts (present = enabled)

| Key | Default | Meaning |
|---|---|---|
| `listen` | `"auto:443"` | Where the TLS listener binds, as for `[router] listen`. If it can't be bound, the router logs it and serves plain HTTP only (no crash loop that would take `:80` and the health probe with it). |
| `certs` | `[]` | `[{ cert_file = "…", key_file = "…" }, …]`: PEM pairs, the chain leaf first. Each handshake gets the first pair whose certificate is valid for the SNI name (webpki's check: a wildcard covers exactly one label); with no SNI, or a name none covers, the first pair. |
| `redirect` | `true` | Once a certificate is loaded, plain HTTP gets `308` to `https://<host>[:<port>]<path?query>`, except `/healthz`. With nothing loaded, plain HTTP is served as before. |
| `reload_secs` | `30` (min 1) | How often the files are checked. A pair whose files changed is re-read, so a renewal is served without a restart. One that fails to load (unreadable, not PEM, a key that isn't the certificate's) keeps its last good version and is logged. |

### `[api]`: the VIP API (present = enabled)

| Key | Default | Meaning |
|---|---|---|
| `listen` | `"127.0.0.1:9103"` | Where the [VIP API](#vip-api) listens. A non-loopback address needs `token_file`, or stormlb refuses to start: whoever reaches the API can point a VIP anywhere. |
| `token_file` | none | A file holding the bearer token every `/api/v1` request must send (`Authorization: Bearer <token>`). It is re-read on every request, so it can be rotated in place. `/healthz` needs no token. |
| `state_file` | none | Where VIPs made through the API are saved (JSON, written atomically on every change) and read back at start, so they serve again after a restart before anyone re-applies them. Unset: they live in memory only. |

### `[metrics]`: Prometheus metrics (present = enabled)

| Key | Default | Meaning |
|---|---|---|
| `listen` | `"auto:9104"` | Where `GET /metrics` is served (also `GET /healthz`; anything else is a 404, a non-GET a 405). `auto:<port>` binds the node's address and loopback, as for `[router] listen`: the node's ironprom scrapes loopback, and stormcos's `check-metrics.sh` probes from off the node. Plain HTTP, read-only, no token, like the other node metrics listeners. If it can't be bound it is logged and everything else runs. |

The series are in [Metrics](#metrics).

### `[vip]`: the L4 balancer (present = enabled)

| Key | Default | Meaning |
|---|---|---|
| `address` | required | The VIP. VRRP and BGP need IPv4. |
| `port` | required | Port the L4 proxy listens on (e.g. `6443`). |
| `bind` | `"0.0.0.0"` | Address the L4 proxy binds. The wildcard means it accepts whether or not this node currently holds the VIP. The VIP's own address works too: the listener sets `IP_FREEBIND`, so it binds before the address arrives. |

It runs as the VIP named `default`, which the API shows but can't change or
remove. The next four sections are only read when `[vip]` is present.

### `[[backend]]`: the pool behind the VIP

| Key | Default | Meaning |
|---|---|---|
| `address` | required | IP literal. Hostnames aren't resolved and fail at start. |
| `port` | required | |

With no backends it starts, warns, and drops every connection.

### `[health]`

| Key | Default | Meaning |
|---|---|---|
| `mode` | `"tcp"` | `tcp`: a connect within the timeout passes. `http` / `https`: `GET <path>` and the status must be in `expect_status`. |
| `path` | `"/readyz"` | A leading `/` is added if missing. |
| `interval_secs` | `2` (min 1) | Sleep between rounds. Backends are checked one after another within a round. |
| `timeout_secs` | `2` (min 1) | Per check. |
| `expect_status` | `[200]` | |
| `ca_file` | none | For `https`: a PEM CA (or bundle) the backend's certificate must chain to, for the address dialled (an apiserver's: the cluster CA, `/data/stormcert/ca.crt`). Only these CAs are trusted, not the public roots. Unset: any certificate is accepted. A file that can't be read or holds no certificate is refused at start (or by the API). |

Within a round, backends are checked one after another. A change to the
members or the spec through the API wakes the loop, so a new backend is
probed at once rather than after an interval.

### `[vrrp]`: L2 VIP ownership

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `false` | |
| `interface` | `""` | Interface to advertise on and to hold the VIP on. Its own IPv4 (the first that is not a /32, read over rtnetlink) is the source address. |
| `vrid` | `51` | Virtual router ID. Must match across the nodes sharing the VIP. |
| `priority` | `100` | 1–255. `255` starts as Master (address owner) if it has a healthy backend. Everyone else starts Backup. |
| `preempt` | `true` | A Backup takes over from a lower-priority Master (RFC 5798 Preempt_Mode). Off: whoever is Master stays until it goes. |
| `advert_interval_secs` | `1` (min 1) | Master advertisement interval. Master-down = 3 × interval + skew. |

### `[bgp]`: L3 anycast advertisement

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `false` | |
| `local_asn` | `0` | Must be 1–65535. 2-byte ASNs only. |
| `router_id` | `""` | This node's IPv4. It is the BGP identifier **and** the next-hop advertised for the VIP. |
| `[[bgp.peers]]` `address`, `asn`, `port` | none; `port` 179 | IPv4 peers. stormlb dials each on `port`. The peer's OPEN must carry `asn`, or the session is refused with a Bad Peer AS NOTIFICATION. |

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
  (all namespaces), polled every `poll_secs` with a 10 s timeout. On error it
  keeps the last good table (a refusal is logged once per distinct error,
  and a 401/403 says the router needs get/list on HTTPRoutes and get on
  Services). With `token_file` it reads as its own ServiceAccount (#9);
  without it, anonymously, which works only where anonymous may list
  HTTPRoutes, i.e. the stormcos `sno` and `bastion` apiservers with
  `--dev-anonymous-admin`. With `ca_file` the apiserver is verified against
  the cluster CA (#10). The golden sets neither yet: the token and the mount
  of `/data/stormcert` are stormcos's (stormcos#76, #363; stormlb#21).
  Every `spec.hostnames` entry maps to one backend:
  1. the `storm.io/backend` annotation, `host:port` verbatim. This is how a
     node service routes: `127.0.0.1:9094` is "this node's console" on every
     node.
  2. otherwise `spec.rules[0].backendRefs[0]`, resolved via
     `GET /api/v1/namespaces/{ns}/services/{name}` to `clusterIP:port` (port
     default 80, namespace default = the route's). A headless or missing
     Service skips the route with a warning.

  Either way, `storm.io/backend-protocol` says how to reach it: `http`
  (the default) or `https`. With `https` the router dials TLS (ALPN
  `http/1.1`) and verifies the backend against `backend_ca_file`, for the
  address's IP (an IP SAN) or for `storm.io/backend-server-name` if the
  route sets it (also sent as SNI). Any other value skips the route with a
  warning.
- Matching is **exact hostname only**. There are no wildcard hostnames, no
  path/header matches, and no rules or backendRefs beyond the first.
- Responses of its own:
  - `400` if there is no `Host`.
  - `404` `no route for host <h>` for a host no route claims.
  - `200` `router alive` for `GET /healthz` on any host no route claims.
    A host a route does claim gets its `/healthz` proxied to the backend.
  - If a backend can't be dialled, the connection closes with no response
    (logged at `debug`). There is no 502.
- **TLS on either side.** With `[router.tls]`, a second listener
  terminates TLS (ALPN `http/1.1`) and feeds the same demux. The head sent
  to the backend gains `X-Forwarded-Proto: https`, and any
  `X-Forwarded-Proto` the client sent is dropped. Plain-HTTP requests carry
  the head unchanged. Toward the backend, a route marked
  `storm.io/backend-protocol: https` gets TLS verified against
  `backend_ca_file` (#13), so an HTTPS-only backend (the apiserver,
  cadvisor under stormcos#81) can sit behind a route. Verification fails
  closed: with no CA loaded, a name the certificate lacks, or another CA's
  certificate, nothing is sent and the client's connection closes
  (`stormlb_router_upstream_errors_total{kind="tls"}`). A client's
  `Authorization` header is passed through untouched, so bearer-guarded
  backends need nothing more. The router presents no client certificate.

### VIP API

`[api]` serves plain HTTP/1.1, one request per connection, JSON both ways.
stormcluster calls it as the masters of a cluster change (form, join as
master, promote, demote, split): it `GET`s the VIP to see where it stands,
then `PUT`s the masters it wants behind it.

| Request | Answer |
|---|---|
| `GET /api/v1/vips` | `{"vips": [<vip>, …]}` |
| `GET /api/v1/vips/{name}` | `<vip>`, or 404 |
| `PUT /api/v1/vips/{name}` with a spec | 201 created or 200 replaced, with `<vip>` |
| `DELETE /api/v1/vips/{name}` | 200 `{"deleted": name}`, or 404 |
| `GET /healthz` | 200 `{"status": "ok"}`, no token needed |

A name is 1–63 of `[a-z0-9-]`. The spec (unknown keys are refused):

```json
{
  "address": "192.168.8.50",
  "port": 7443,
  "bind": "192.168.8.50",
  "backends": [{"address": "192.168.8.51", "port": 6443},
               {"address": "192.168.8.52", "port": 6443}],
  "health": {"mode": "https", "path": "/readyz", "ca_file": "/data/stormcert/ca.crt",
             "interval_secs": 2, "timeout_secs": 2, "expect_status": [200]},
  "vrrp": {"interface": "eth0", "vrid": 51, "priority": 100, "advert_interval_secs": 1}
}
```

`address`, `port` and backend `address`/`port` are required. Addresses are
IP literals, never names. `bind` defaults to `address`. `health` takes the
`[health]` keys with the same defaults. `vrrp` (IPv4 only) takes the
`[vrrp]` keys without `enabled`; leave it out when something else puts the
address on the node. `<vip>` is the spec plus `name` and `status`:

```json
"status": {"listening": "192.168.8.50:7443", "source": "api", "healthy": 2,
           "backends": [{"address": "192.168.8.51", "port": 6443, "healthy": true}, …],
           "vrrp": "master"}
```

`source` is `api`, or `config` for `default` (the TOML `[vip]`, read-only
here: a `PUT` or `DELETE` of it is a 409). `vrrp` is `master`, `backup` or
`stopped`, and is absent without VRRP.

What a `PUT` does to a running VIP:

- **Validated first.** A bad spec is a 400, and a listener that can't bind
  (in use, or another VIP's) is a 409. Either way the VIP keeps serving as
  it was.
- **Backends that stay keep their health,** so re-applying the same masters
  never takes the VIP down. New ones start unhealthy and are probed at once.
  A removed one gets no new connections. **Connections already proxied are
  never cut,** whatever changes.
- A new `bind`/`port` binds the new listener before the old one closes.
- A changed `vrrp` or `address` stops the VIP's VRRP instance (releasing the
  address if it was Master) and starts a new one.

`DELETE` closes the listener and stops VRRP; connections already proxied
run on. With `state_file`, every change is saved: if the save fails, the
change is still in effect, and the answer is a 500 that says so.

Errors are `{"error": "…"}`: 400, 401 (no or wrong token), 404, 405, 409,
411 (chunked body), 413 (body over 1 MiB), 431 (head over 16 KiB), 500.

**On a master, the VIP can't be on 6443.** The apiserver binds
`0.0.0.0:6443`, and Linux won't bind `<vip>:6443` beside a wildcard
listener, so the PUT is a 409. Which port the cluster's VIP uses is
stormcluster's (glennswest/stormcluster#35).

### L4 balancer

- Accepts on `bind:port` (bound with `SO_REUSEADDR` and `IP_FREEBIND`) and
  picks a backend round-robin among the healthy ones. It then splices (TCP_NODELAY both ways).
- Backends **start unhealthy**; the first passing check admits them.
- With no healthy backend, or if the chosen backend refuses the connection,
  the client connection is closed. There is no retry on another backend.

### VRRP (L2)

VRRP v3 (RFC 5798) over a raw `IPPROTO_VRRP` (112) socket joined to
`224.0.0.18` on `interface`, TTL 255. Each VIP with VRRP (`[vrrp]` for
`default`, `vrrp` in an API spec) runs its own instance on its own OS
thread. An instance stops within half a second when its VIP is removed or
its VRRP changes, and releases the address if it was Master.

RFC 5798 §6.4, as specified:

- **Master:** sends an advertisement every interval. A peer with a higher
  priority, or equal priority and a higher address, demotes it to Backup,
  and it releases the VIP. A peer's priority-0 advertisement is answered
  with one at once.
- **Backup:** restarts its master-down timer on an advertisement from a
  Master of equal or higher priority, and learns that Master's interval.
  With `preempt` (the default) it **discards** a lower-priority Master's
  advertisements, so its timer runs out and it takes over. A priority-0
  advertisement (a Master resigning) cuts the wait to Skew_Time. When
  master-down passes, it becomes Master and claims the VIP.
- **Stopping** (the VIP removed, its VRRP changed, the process told to
  stop): a Master sends priority 0, then releases the VIP.
- Advertisements carry the RFC checksum over the IPv4 pseudo-header. Ones
  received with an IP TTL other than 255 or a bad checksum are dropped.

**Ownership follows backend health** (beyond the RFC): a Master whose VIP has
no healthy backend resigns (priority 0, release), and a Backup with none
never takes over. The VIP sits on a node that can serve it. If no node has a
healthy backend, no node holds it. The address owner (255) starts as Backup
until it has one.

Claim adds `<vip>/32` to the interface over rtnetlink, then broadcasts three
gratuitous ARP replies, 0.5 s apart, on an `AF_PACKET` socket. Release
deletes the address. Both are idempotent. No `ip` or `arping` binary is
used, so the golden needs none. It needs `CAP_NET_ADMIN` (the address) and
`CAP_NET_RAW` (the VRRP and ARP sockets).

### BGP (L3)

A minimal speaker: one outbound session per peer, reconnecting every 5 s
after failure. The session follows RFC 4271's FSM as far as a speaker that
originates one route needs (#6):

1. It sends OPEN (version 4, hold 180 s, no optional parameters).
2. It reads the peer's OPEN, within 4 minutes, and checks it. The version
   must be 4, the AS must be the configured `asn`, and the hold time must be
   0 or at least 3 s. Otherwise it sends the matching OPEN Message Error
   NOTIFICATION (subcode 1, 2 or 6) and drops the session.
3. It answers with a KEEPALIVE and waits for the peer's. Only then is the
   session Established, and only then is any UPDATE sent.
4. The hold time is the smaller of 180 s and the peer's. KEEPALIVEs go
   every third of it, and a peer that sends nothing for a whole hold time
   gets a Hold Timer Expired NOTIFICATION and the session is dropped.
   Hold 0 means neither.

It announces `<vip>/32` (ORIGIN IGP, AS_PATH `[local_asn]`, NEXT_HOP
`router_id`) while the config VIP has at least one healthy backend, and
withdraws it otherwise. Health is checked every 250 ms, and a change is sent
at once, so a withdraw follows a failed health check within a quarter
second (it waited for the 60 s keepalive tick before #6). On Established it
announces right away if healthy. Received UPDATEs are read and ignored; a
NOTIFICATION ends the session. IPv4 unicast only. Every healthy node
advertises the same /32, so the upstream router ECMP-hashes across them.

## Ports and endpoints

| Port | Proto | Who | When |
|---|---|---|---|
| `[router] listen`: **80** in the golden | TCP, plain HTTP/1.x, no auth | clients via `*.storm1.<zone>`; stormd's liveness probe on `127.0.0.1:80/healthz` | `[router]` present |
| `[router.tls] listen`: **443** | TLS 1.2/1.3, then HTTP/1.x | clients via `*.storm1.<zone>` | `[router.tls]` present (not in the golden yet: stormcos#363) |
| `[vip] port`, e.g. 6443, and each API VIP's `port` | TCP | kube-api clients via the VIP | `[vip]` present, or a VIP made through the API |
| `[metrics] listen`: **9104** (`auto`: node address and loopback) | plain HTTP, read-only, no token | ironprom on the node; stormcos `check-metrics.sh` | `[metrics]` present |
| `[api] listen`: **127.0.0.1:9103** by default | plain HTTP, JSON; a bearer token with `token_file` | stormcluster, on the same node | `[api]` present |
| n/a | IP proto 112 to `224.0.0.18` | VRRP peers | `[vrrp] enabled` |
| 179 (outbound only) | TCP | BGP peers | `[bgp] enabled` |
| 180 in the golden | plain HTTP, no auth | **stormd's** API, not stormlb's (port + 100, stormcentral's convention). Its TLS and auth are stormd#32. | golden |

stormcos#81 requires every listener on a node to be TLS with a stormcert
certificate and to authenticate, health probes excepted. The router can
serve TLS (`[router.tls]`, #14), but no node has its certificate yet:
minting `*.storm1.<zone>` and mounting it into stormlb's container is
stormcos#363. Until then the golden serves plain `:80` only, which
stormcos `docs/SECURITY.md` lists as failing the rule. A routed host's own
authentication is its backend's.

**Health:** the router's `/healthz` (above), the API's `/healthz`, and the
metrics listener's `/healthz`.
Each VIP's backends' health and VRRP state are in `GET /api/v1/vips`; a VIP
from `[vip]` without `[api]` shows them only in the logs (and `ip addr`).

### Metrics

`[metrics]` serves Prometheus text (format 0.0.4) on `:9104/metrics` (#12,
for stormcos#64). `GET /metrics` on `:80` is still routing: the 404 for an
unclaimed host, or the backend of a claimed one. Only series that have
happened are printed.

| Series | Type | Labels | What |
|---|---|---|---|
| `stormlb_router_requests_total` | counter | `host`, `code` | Requests: the first of each connection, which is what the router routes. `host` is a route's hostname, or `unrouted` for the router's own answers (a client's Host header never becomes a label). `code` is the backend's status, the router's own (`400`, `404`, `308`, `200` for `/healthz`), or `error` when the backend gave no response. |
| `stormlb_router_request_duration_seconds` | histogram | `host` | From the request reaching the backend to its first response byte. Buckets 5 ms to 10 s. |
| `stormlb_router_upstream_errors_total` | counter | `host`, `kind` | `connect` (refused, unreachable), `tls` (an https backend: no CA loaded, verification or handshake failed) or `no_response` (closed before a byte). |
| `stormlb_router_connections_total`, `stormlb_router_connections_active` | counter, gauge | `listener` (`http`, `https`) | Connections accepted, and open now. |
| `stormlb_router_tls_handshake_errors_total` | counter | | Failed or timed-out TLS handshakes. |
| `stormlb_router_routes` | gauge | | Hostnames in the route table. |
| `stormlb_router_route_refreshes_total` | counter | `result` (`ok`, `error`) | Route-table reloads from the apiserver. |
| `stormlb_router_tls_certificates_loaded`, `stormlb_router_tls_reloads_total` | gauge, counter | `result` | Certificate pairs loaded, and (re)loads from changed files. |
| `stormlb_vip_connections_total`, `stormlb_vip_connections_active` | counter, gauge | `vip` | Connections through each VIP's L4 listener. |
| `stormlb_vip_no_healthy_backend_total`, `stormlb_vip_upstream_connect_errors_total` | counter | `vip` | Connections dropped: no healthy backend, or the chosen one failed. |
| `stormlb_vip_backend_healthy` | gauge | `vip`, `backend` | 1 or 0, read at scrape time. |
| `stormlb_vip_vrrp_master` | gauge | `vip` | 1 while this node is Master (absent without VRRP). |
| `process_resident_memory_bytes`, `process_open_fds`, `process_max_fds`, `process_cpu_seconds_total`, `process_start_time_seconds` | gauge, counter | | stormlb's own process, from `/proc/self`. stormd's `:180/metrics` describes stormd, not the process it supervises (stormd#33). |
| `stormlb_build_info` | gauge | `version` | Always 1. |

**Per connection, not per request:** a keep-alive connection's later
requests are spliced, not parsed, so they aren't counted. That's the price
of routing per connection (see [Router](#router)).

stormd's own `/metrics` on `127.0.0.1:180` still reports the process's
state, restarts and crashes.

## How it ships

stormlb is a stormcentral component. Its entry (golden, port, health,
argv, the config text below) lives in stormcentral's database: read it with
`stormcentral component export`, change it with `stormcentral component edit
stormlb --set key=value` (stormcentral#185). stormcentral's
`components/stormcos.toml` was only the seed, imported once and not read
again.

- `kind = "service"`: a `stormlb` golden (32 MiB, pallet `system1`) with the
  static musl binary at `/usr/sbin/stormlb` in a stormd base. stormd runs it
  as a process with `--config /etc/stormlb/stormlb.toml`, restarts it on
  exit, and probes `http://127.0.0.1:80/healthz`. There are also
  `stormlb-data` (64 MiB, `data1`, at `/var/lib/stormlb`) and `stormlb-logs`
  (64 MiB, `system1`, at `/var/log/stormd`).
- The config baked into the golden is the **router, the VIP API** (on
  loopback, saving its VIPs on the data volume) **and metrics** on `:9104`:

  ```toml
  [router]
  listen = "auto:80"

  [api]
  state_file = "/var/lib/stormlb/vips.json"

  [metrics]
  listen = "auto:9104"
  ```

- stormcentral builds the golden from an exact pushed commit with
  `cargo build --release --locked --target x86_64-unknown-linux-musl` on a
  build machine, as an unprivileged build user. That's why `Cargo.lock` is
  committed.
- On a stormcos node there is no systemd: stormpump is PID 1. stormcos's
  `deploy/build-goldens.sh` writes a `spec stormlb` stanza into
  `/etc/stormpump/boot.d/40-services`. It is a container on the host network
  profile, sharing UTS, with its data and log volumes. On the `sno`
  and `bastion` profiles it also writes `start stormlb` (`sno` is the
  default). A node runs the router because it has that `start` line; the
  `node` and `storage` profiles get the spec without it.
  stormcos `deploy/image.toml` places the three goldens.

## Build and test

Nothing builds on the session VM, and nothing needs root. Push, then:

```
sc-build                                        # cargo build && cargo test on a build VM
sc-build 'cargo build --release --locked && cargo test --locked'
```

`sc-build` fetches the pushed commit onto a fresh build VM, builds it there
and deletes it (`SC_BUILD_VM=1` forces the VM until stormcentral switches to
it by default; the old build box, `dev.g8.lo`, was retired on 2026-10-07). A failure files a `build-failure`
issue here. `Cargo.lock` is committed, and `cargo update` is a deliberate
commit of its own. A new golden is requested with
`stormcentral component build stormlb --url http://stormcentral.g8.lo`.

Tests: config parsing and defaults, pool round-robin, health filtering and
member replacement, TCP health check, CA-file validation, the VRRP state machine and advertisement
encode/parse/checksum, BGP OPEN/UPDATE/withdraw encoding, router header
parsing, the router's own `/healthz` bytes on a real socket, and two
integration tests. `tests/balancer.rs` drives round-robin plus failover
through the real L4 proxy. `tests/vips.rs` drives the VIP API over a real
socket: create, change backends (new members probed at once, members that
stay keep their health, a proxied connection survives), move the listener,
remove, refused changes leaving the VIP serving, the token, a restart from
`state_file`, the config VIP read-only, and https health against a CA file
(certificates made with `openssl` at test time; skipped without it).
`tests/router_tls.rs` runs the real router against a fake apiserver with one
route, and certificates made with `openssl`: plain HTTP served while no
certificate exists, the files picked up when they appear, SNI choosing the
wildcard, the exact pair, or the first (no SNI), names no pair covers
refused by the client (`a.b.` under a one-label wildcard included), the 308
with path and query, `/healthz` still plain, `X-Forwarded-Proto: https` at
the backend with the client's own dropped, a renewal served without a
restart, a broken file keeping the last good one, and a mismatched key
refused.

VRRP on a real wire, with no root: `tests/vrrp-netns.sh` runs two stormlb
processes in two network namespaces joined by a veth, inside an unprivileged
user namespace (`unshare -rn`). Each makes the same VIP through its API with
VRRP on the veth. Checked against the kernel's `ip addr` and `ip neigh`:
takeover after master-down, preemption by the higher priority, the
gratuitous ARP updating the peer's neighbour entry, a resign on no healthy
backend, preempting back, and priority 0 on removal cutting the takeover to
about a second.

```
sc-build 'cargo build --release --locked && tests/vrrp-netns.sh'
```

BGP against a real peer is not covered.

### Tests on a node: the test container

[`test/`](test/) is stormlb's test container, per stormcentral
`docs/test-standard.md`. It tests what a node runs, which is the router,
from outside, through the apiserver and port 80. It is a crate of its own
(own workspace and `Cargo.lock`, never part of the golden's build). It is
built in two steps: `test/build.sh` compiles the static (musl) binary into
`test/out/test`, and `test/Containerfile` (`FROM scratch`) copies it in as
`/test`. stormcentral runs it with
`stormcentral test run stormlb <short|medium|long>`: it runs `test/build.sh`
on the build box, builds the image with the repo root as the context, and
starts `/test <suite>` with the standard's `STORM_*` environment in its own
Job. Results are the JSON lines on stdout, read from the pod log. Each line
is compact JSON with every space in a string written `\u0020`, so no line
has a literal space: rustkube-node's `/log` cuts the first three words off
any line with three or more (rustkube-node#136).

What the suites need of the node is declared per suite in
[`test/requires.toml`](test/requires.toml), which the runner reads at the
commit it tests (stormcentral#74, #55):

- **`host_network = true`** on all three. The router runs on the host
  network and dials the test's backends at the node's address, the same
  path as a node service. Without it they would be pod IPs, which the host
  may not route to.
- **`cluster_read` of `nodes`** on `long`, which sizes its waves from the
  node's allocatable CPU. If the read fails anyway, it falls back to the
  container's CPUs and its `capacity` line says so.

The runner's Role already covers the run's namespace, which is all the
suites write to. There is no Job template of stormlb's own: the runner
makes the Job.

| Suite | Budget | What it proves |
|---|---|---|
| `short` | < 2 min | `/healthz` answers; stormd (`:180/metrics`) reports stormlb running; an HTTPRoute's hostname reaches its backend, and is a 404 again once deleted. |
| `medium` | < 30 min | 400 without a Host, the 404 that names the host, `/healthz` on unclaimed and claimed hosts and its CRLF line endings, Host case and port, per-connection routing, a streamed response not held back, an Upgrade as a two-way pipe, an 8 MiB body, the 16 KiB head limit, a dead backend closing with no response, a route update, a backendRef through a Service (skip without a Service data plane), a headless Service's route skipped, 50 hosts under concurrent load, deleted routes back to 404, no restart or crash under stormd. The VIP half is reported skip: it is not shipped. |
| `long` | the night window | Waves sized from the node's allocatable CPU (read from the API) and the container's open-file limit. Each wave creates routes, holds connections at that size, drains, and checks residue. Each `wave-<n>` line carries route-programming time, request p50/p99, requests/s, drain time, leftovers, restarts and idle latency. `trend` fails on the first wave that is slower than the first wave of its size. |

- **Backends** are listeners in the test pod. They are reached on the node's
  address (`host_network`, above): the routes name them in
  `storm.io/backend`, the same path a node service uses. The suites create only HTTPRoutes, Services and
  Endpoints, all in the run's namespace and labelled `storm.io/test-run`,
  and delete them at the end (`cleanup`).
- **Environment:** the standard's `STORM_*` variables. The router defaults to
  `STORM_NODE:80` and stormd to `STORM_NODE:180`; `STORMLB_ROUTER` and
  `STORMLB_STORMD` (`none` for no stormd) override them. `STORMLB_ROUTE_WAIT`
  (default 30 s) is how long a route change may take, and `STORMLB_SETTLE`
  (12 s) is how long to wait before calling something *not* routed.
- **Where stormlb isn't started:** stormcos starts it on the `sno` and
  `bastion` profiles only. When neither the router nor its stormd answers, a suite reports one
  `stormlb-started` skip, never a pass. If stormd answers and the router
  doesn't, that's a failure.
- **Not read by the suites yet:** the router's own memory and file
  descriptors. stormd's open `/metrics` reports stormd's, not the process it
  supervises (stormd#33). Since #12 the router reports its own `process_*`
  on `:9104/metrics`, but the long suite's residue check doesn't read them
  yet.
- **The harness:** `test/tests/harness.rs` runs the three suites against the
  real router (`stormlb::router::run`) and a small in-memory apiserver, all
  on loopback. That's how the container's own code is tested:

  ```
  sc-build 'cd test && cargo test --locked'
  ```

  None of the three suites has been run on a test machine yet.

## Gaps (known and filed)

What the code does not do yet, which older docs implied it did:

- [#21](https://github.com/glennswest/stormlb/issues/21): the golden's
  config doesn't set `token_file`, `ca_file`, `backend_ca_file` or
  `[router.tls]` yet. They need stormlb's ServiceAccount token
  (stormcos#76 step 2) and `/data/stormcert` mounted into its container
  (stormcos#363), so the shipped router still reads anonymously, against
  sno and bastion only.
- [stormcluster#35](https://github.com/glennswest/stormcluster/issues/35):
  a cluster's API VIP can't listen on `:6443` on a master, because the
  apiserver binds `0.0.0.0:6443` there. The VIP's port is stormcluster's
  choice.
- The API is plain HTTP on loopback, with an optional bearer token: no TLS
  (stormcos#81 allows nothing else off-node, which is why it is loopback).
- Metrics count the first request of each keep-alive connection only (the
  router splices after the first head).
- `backend_ca_file` isn't set in the golden: stormlb's container doesn't
  mount `/data/stormcert` yet (stormcos#363), so an https route fails closed
  on a node until it does.
- [stormcos#363](https://github.com/glennswest/stormcos/issues/363): the
  router can terminate TLS, but no node mints its certificate or mounts it
  into stormlb's container yet, so the golden serves plain `:80` only.
- Earlier follow-ups: 4-octet ASNs and multiprotocol BGP. VRRP over IPv6
  is not implemented. VRRP's `CAP_NET_ADMIN` and `CAP_NET_RAW` come from
  stormpump keeping every capability for a container today. Once stormpump#47
  sets a default, stormlb's spec must ask for both.
