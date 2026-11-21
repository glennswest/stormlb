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
(default `info`). Per-connection and per-request failures (client gone, backend down) log at
`debug`; a backend's health changing logs at `info`/`warn`.

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
| `apiserver` | `"https://127.0.0.1:6443"` | Where HTTPRoutes, Services, Endpoints, EndpointSlices and ReferenceGrants are read from, and route status is written. |
| `poll_secs` | `5` (min 1) | Seconds between route-table refreshes. |
| `backend_ca_file` | none | The CA (PEM) a route's backend must chain to when the route says `storm.io/backend-protocol: https`; on a node, the cluster CA `/data/stormcert/ca.crt`. Only these certificates are trusted, not the public roots. Re-read on the route poll when it changes, so it may appear after the router starts. Unset or not loadable: https backends' connections fail closed. |
| `token_file` | none | The router's identity (#9): a file holding a bearer token, its ServiceAccount's, which stormcos mints (stormcos#395, step 2 of stormcos#76) with list on HTTPRoutes, Services, Endpoints, EndpointSlices and ReferenceGrants and patch on `httproutes/status` (#37). It's sent as `Authorization: Bearer` and re-read on every poll, so it can be rotated in place. Set but missing or empty: no refresh is made, and the last table keeps serving (never a fallback to anonymous). Unset: anonymous, as before. The golden sets `/data/stormcert/stormlb.token` (#33). |
| `ca_file` | none | The CA (PEM) the apiserver's certificate must chain to, on a node `/data/stormcert/ca.crt` (#10). Only it is trusted, not public roots. Re-read when it changes, so it may appear after the router starts. When set, `insecure` is ignored. |
| `insecure` | `true` | Without `ca_file`: accept the apiserver's certificate unverified. `false` without `ca_file` trusts only the public roots compiled in, which refuses stormcert's certificate. |

`auto:<port>` picks the node's address by asking the routing table (a
connected UDP socket to `203.0.113.1`, so nothing is sent). It binds that
address plus loopback, and skips anything in `127.*` or `169.254.*`. It
exists because `0.0.0.0:80` includes `169.254.169.254:80`, which stormimds
(the metadata service) holds. Whichever started second crash-looped on
`EADDRINUSE`. Both addresses must bind: if either fails (a port collision
on the node address, say) the bind fails naming that address, rather than
a router whose loopback health probe passes while nothing off the node can
reach it (#43). For `[router] listen` that ends the router (stormd restarts
it, so the restart count shows it); `[router.tls]` and `[metrics]` log it
and bind nothing. If no address can be found it warns and falls back to
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
| `listen` | `"127.0.0.1:9109"` | Where the [VIP API](#vip-api) listens. A non-loopback address needs `token_file`, or stormlb refuses to start: whoever reaches the API can point a VIP anywhere. |
| `token_file` | none | A file holding the bearer token every `/api/v1` request must send (`Authorization: Bearer <token>`). It is re-read on every request, so it can be rotated in place. `/healthz` needs no token. |
| `state_file` | none | Where VIPs made through the API are saved (JSON, written atomically on every change) and read back at start, so they serve again after a restart before anyone re-applies them. Unset: they live in memory only. |

### `[metrics]`: Prometheus metrics (present = enabled)

| Key | Default | Meaning |
|---|---|---|
| `listen` | `"auto:9111"` | Where `GET /metrics` is served (also `GET /healthz`; anything else is a 404, a non-GET a 405). `auto:<port>` binds the node's address and loopback, as for `[router] listen`: the node's ironprom scrapes loopback, and stormcos's `check-metrics.sh` probes from off the node. Plain HTTP, read-only, no token, like the other node metrics listeners. If it can't be bound it is logged and everything else runs. |

The series are in [Metrics](#metrics).

### `[vip]`: the L4 balancer (present = enabled)

| Key | Default | Meaning |
|---|---|---|
| `address` | required | The VIP, IPv4 or IPv6. VRRP owns either (#39); BGP announces either (#40). |
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
| `interface` | `""` | Interface to advertise on and to hold the VIP on. The source address, read over rtnetlink, is its own IPv4 (the first that is not a /32) for an IPv4 VIP, or its IPv6 link-local (one that has finished DAD; waited for up to 10 s) for an IPv6 VIP. |
| `vrid` | `51` | Virtual router ID. Must match across the nodes sharing the VIP. |
| `priority` | `100` | 1–255. `255` starts as Master (address owner) if it has a healthy backend. Everyone else starts Backup. |
| `preempt` | `true` | A Backup takes over from a lower-priority Master (RFC 5798 Preempt_Mode). Off: whoever is Master stays until it goes. |
| `advert_interval_secs` | `1` (min 1) | Master advertisement interval. Master-down = 3 × interval + skew. |

### `[bgp]`: L3 anycast advertisement

| Key | Default | Meaning |
|---|---|---|
| `enabled` | `false` | |
| `local_asn` | `0` | 1–4294967294, not 23456 (AS_TRANS). 4-octet ASNs work (#40). |
| `router_id` | `""` | A dotted quad: the BGP identifier, and the IPv4 VIP's next-hop unless `next_hop` is set. Usually this node's IPv4. |
| `next_hop` | `router_id` | The next-hop advertised for the VIP, in the VIP's family. An IPv6 VIP needs it: this node's global IPv6 (link-local is refused). |
| `[[bgp.peers]]` `address`, `asn`, `port` | none; `port` 179 | IPv4 or IPv6 peers. stormlb dials each on `port`. The peer's OPEN must carry `asn` (its 4-octet AS capability's, when it offers one), or the session is refused with a Bad Peer AS NOTIFICATION. Same ASN rules as `local_asn`. |

If the preflight fails (a reserved ASN, no peers, a peer address that is not
an IP, `router_id` not a dotted quad, a `next_hop` not in the VIP's family),
it logs `bgp disabled: …` and everything else keeps running.

## What each half does

### Router

An HTTP/1.1 reverse proxy over Gateway API HTTPRoutes (#37), hyper on both
sides. **Every request is routed on its own** (#26), including several on
one keep-alive connection. It is matched on host, path, method, headers and
query against the routes' rules, and sent to an endpoint of the backend
that the rule's weights pick.

- **The table** is built every `poll_secs` (10 s timeout) from cluster-wide
  lists: `GET {apiserver}/apis/gateway.networking.k8s.io/v1/httproutes`,
  and only when some route has a `backendRef`, `/api/v1/services`,
  `/apis/discovery.k8s.io/v1/endpointslices` and `/api/v1/endpoints`, and
  only when some backendRef names another namespace,
  `/apis/gateway.networking.k8s.io/v1beta1/referencegrants` (`v1` if only
  that is served). That is four or five requests a poll, however many
  routes. A list the apiserver does not serve (404) is treated as empty. Any
  other failure keeps the last good table, logged once per distinct error;
  a 401/403 names the verbs the router needs (list on those five, patch on
  `httproutes/status`). The ReferenceGrants are the exception: when they
  cannot be read, the table still updates and every cross-namespace ref is
  refused.
  - With `token_file` the router reads as its own ServiceAccount (#9).
    Without it, it reads anonymously, which works only where anonymous may
    list, i.e. the stormcos `sno` and `bastion` apiservers with
    `--dev-anonymous-admin`.
  - With `ca_file` the apiserver is verified against the cluster CA (#10).
    The golden sets `ca_file` (`/data/stormcert/ca.crt`, #21) and
    `token_file = "/data/stormcert/stormlb.token"` (#33): the token of
    `storm-lb/stormlb`, which the apiserver golden mints
    (`stormcert-sa-stormlb`, stormcos#395) with exactly the verbs above
    (stormcos `deploy/manifests/85-stormlb.yaml`). Until the file exists
    (a first boot, before the mint has run) no refresh is made; the router
    logs that once and loads routes on the poll after the file appears. The
    first refresh logs which identity it used (`route table read with the
    router's token (…)`, or `… anonymously`).
- **Hostnames**: exact names first, then wildcards (`*.example.com` covers
  `a.example.com` and `a.b.example.com`, not `example.com`). Among wildcards
  the longest suffix wins. Only the rules under the most specific matching
  hostname are considered. Host is lowercased and its port dropped. A route
  with no `hostnames` serves nothing.
- **Matches**:
  - path `Exact` or `PathPrefix`, element-wise (`/api` matches `/api` and
    `/api/x`, not `/apix`), compared undecoded;
  - `method`;
  - `headers` and `queryParams` of type `Exact`.

  A rule without `matches` is `PathPrefix /`. Precedence is the Gateway
  API's: an exact path, then the longest prefix, then a method, then the most
  headers, then the most query params. Ties go to the oldest route, then
  `namespace/name`, then rule order. So `/api/` beats `/` whichever is
  listed first.
- **Backends**: each `backendRef` (kind `Service`, port as the Service's
  `port`) is a pool of that Service's endpoints:
  - taken from its EndpointSlices (`kubernetes.io/service-name`), else its
    Endpoints, else, when neither object exists, its `clusterIP:port`;
  - the Service port's name picks the endpoints' port;
  - a headless Service works like any other.

  `weight` (default 1) splits the rule between its refs by smooth weighted
  round-robin (3:1 is exactly a, a, b, a; a refresh restarts the
  rotation). Inside a ref, the router uses round-robin, or the endpoint with
  the fewest requests waiting for a response head when the Service says
  `storm.io/balance: least-conn`.
- **Other namespaces** (#42): a backendRef whose `namespace` is not the
  route's is followed only when that namespace holds a Gateway API
  `ReferenceGrant` whose `from` has `group: gateway.networking.k8s.io`,
  `kind: HTTPRoute` and the route's namespace, and whose `to` has
  `group: ""`, `kind: Service` and either no `name` or the Service's.
  Otherwise the ref is unresolved: `ResolvedRefs: False`, reason
  `RefNotPermitted`, and its share of the rule's requests answers 500.
  rustkube serves no ReferenceGrants today ([rustkube#351](https://github.com/glennswest/rustkube/issues/351)), so there every
  cross-namespace ref is refused.
- **Health**: an endpoint takes requests while all three hold:
  - it is ready: EndpointSlice `conditions.ready`, or Endpoints `addresses`
    as opposed to `notReadyAddresses`;
  - it passes the Service's active check, if the Service asks for one;
  - it has not refused a connection in the last 5 s (passive ejection).

  The active check is a `GET` on `storm.io/health-check-path` every
  `storm.io/health-check-interval` (default `5s`), with
  `storm.io/health-check-timeout` (default the interval, at most `2s`). It
  passes on 2xx/3xx. `storm.io/health-check-rise` (default 2) passes in a
  row bring the endpoint back, and `-fall` (default 3) failures take it out.
  `-host` sets the Host header, defaulting to the endpoint's address. The
  check runs over TLS when the route's backend is https. A ref with no such
  endpoint drops out of the weighting. If its endpoints are only ejected,
  they are tried anyway rather than answering 503. Endpoint state survives
  table refreshes.
- **Retries and timeouts**:
  - A connection refused or failed moves on to another endpoint, up to 3
    tries, whatever the request.
  - A request without a body whose backend closed without answering is also
    retried, as are the rule's `retry.codes` (`retry.attempts` times,
    `retry.backoff` apart; Gateway API experimental).
  - `timeouts.request` bounds the time to the response head, retries
    included. `timeouts.backendRequest` bounds one attempt. Both answer 504.
  - Once the head is sent, the body streams with no timeout.
- **`storm.io/backend: host:port`** on a route overrides its rules: one
  static backend for every path. This is how a node service routes:
  `127.0.0.1:9094` is "this node's console" on every node.
- **Refused, not ignored**: a rule with `filters` (route or backendRef),
  `sessionPersistence`, or a `RegularExpression` path, header or query match
  is dropped. The route's status says why (`PartiallyInvalid`, or `Accepted:
  False` when nothing is left). A bad `storm.io/backend-protocol` drops the
  route; a bad `storm.io/balance` or check annotation makes that ref
  unresolved (500).
- **Status**: after each poll the router merge-patches
  `.../httproutes/{name}/status`, only when it changed, with its own entry in
  `status.parents`. That entry has `parentRef` `storm.io/Router stormlb`,
  `controllerName: storm.io/stormlb`, and these conditions:
  - `Accepted`;
  - `PartiallyInvalid` (only when a rule was dropped);
  - `ResolvedRefs`, False with `BackendNotFound`, `InvalidKind` or
    `UnsupportedValue`;
  - `storm.io/BackendsHealthy`, whose message is `ns/svc:port 3/3 healthy;
    …`.

  Other controllers' entries are kept. A refused write is logged once and
  retried after 60 s; routing never waits on it. Every node's router writes
  the same entry, so nodes that disagree on health would take turns. The
  rustkube apiserver currently answers 404 for HTTPRoute `/status`
  (rustkube#348), so no status appears there yet. The same table, with
  every endpoint's state, is `GET /api/v1/routes` on the metrics listener.
- **Proxying**:
  - The request goes to the backend as HTTP/1.1, origin-form, with
    hop-by-hop headers removed (those `Connection` names, `Keep-Alive`, `TE`,
    `Trailer`, `Transfer-Encoding`, `Upgrade`).
  - The router sets `X-Forwarded-For` (appended), `X-Forwarded-Host` (if
    absent) and `X-Forwarded-Proto` (the scheme the client used; a client's
    own value is replaced). Header names go out Title-Case.
  - Bodies stream both ways.
  - An `Upgrade` request (websockets) gets the backend's `101`, and then the
    two connections are spliced.
  - Backend connections are kept alive and reused: up to 8 idle per
    endpoint and 64 in all, each for 15 s.
  - A backend response with `connection: close` closes the client's
    connection after it.
  - The request head limit is 16 KiB and 256 headers (beyond that: `431`,
    then close); a client that sends no head in 30 s is closed.
- **Its own answers** (text, `connection: close`, naming the host):
  - `400` with no `Host`;
  - `404 no route for host <h>`, or `no rule of the routes for host <h>
    matches <method> <path>`;
  - `200 router alive` for `GET /healthz` on any host no route claims (a
    host a route does claim gets `/healthz` proxied);
  - `500` for an unresolved backendRef, or a rule with no weighted backend;
  - `503` when no endpoint is healthy;
  - `502` when no endpoint could be reached or the backend gave no response;
  - `504` on a timeout.
- **TLS on either side.**
  - With `[router.tls]`, a second listener terminates TLS (ALPN `http/1.1`)
    and feeds the same router. Requests through it carry
    `X-Forwarded-Proto: https`.
  - A route marked `storm.io/backend-protocol: https` is dialled over TLS
    (ALPN `http/1.1`), verified against `backend_ca_file` only (#13). The
    name checked is each endpoint's IP (an IP SAN), or
    `storm.io/backend-server-name` if set (also sent as SNI). So an
    HTTPS-only backend (the apiserver, cadvisor under stormcos#81) can sit
    behind a route.
  - Verification fails closed: with no CA loaded, a name the certificate
    lacks, or another CA's certificate, nothing is sent and the client gets
    `502` (`stormlb_router_upstream_errors_total{kind="tls"}`).
  - A client's `Authorization` header is passed through untouched. The
    router presents no client certificate.

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
`[health]` keys with the same defaults. `vrrp` (IPv4 or IPv6) takes the
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
  address if it was Master) and starts a new one. The new one runs only once
  the old one has finished (within about half a second), so the old
  instance's release can never remove the address the new one claimed (#44).

`DELETE` closes the listener and stops VRRP; connections already proxied
run on. With `state_file`, every change is saved: if the save fails, the
change is still in effect, and the answer is a 500 that says so. Changes
are made one at a time, each saved before the next starts, so concurrent
requests leave the file equal to what is served; each save writes a
temporary file of its own and renames it into place (#44).

Errors are `{"error": "…"}`: 400, 401 (no or wrong token), 404, 405, 409,
411 (chunked body), 413 (body over 1 MiB), 431 (head over 16 KiB), 500.

**On a master, the VIP can't be on 6443.** The apiserver binds
`0.0.0.0:6443`, and Linux won't bind `<vip>:6443` beside a wildcard
listener, so the PUT is a 409. stormcluster (the API's client, stormcluster#10)
puts the cluster's VIP on its `endpoint.port` (default **7443**, never the
apiserver's port) and fronts each master's `:6443`; the apiserver certificate
needs the VIP among its SANs (stormcos#362).

### L4 balancer

- Accepts on `bind:port` (bound with `SO_REUSEADDR` and `IP_FREEBIND`) and
  picks a backend round-robin among the healthy ones. It then splices (TCP_NODELAY both ways).
- Backends **start unhealthy**; the first passing check admits them.
- With no healthy backend, or if the chosen backend refuses the connection,
  the client connection is closed. There is no retry on another backend.

### VRRP (L2)

VRRP v3 (RFC 5798) over a raw `IPPROTO_VRRP` (112) socket joined to
`224.0.0.18` on `interface`, TTL 255, for an IPv4 VIP; for an IPv6 VIP
(#39), joined to `ff02::12`, hop limit 255, and bound to the interface's
link-local address, which is the source and the tie-break. Each VIP with VRRP (`[vrrp]` for
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
- **A failed claim** (at start as the owner, or on takeover) is a failed
  transition (#45): the instance goes back to Backup, so it never
  advertises Master for an address it does not hold, removes whatever part
  of the claim happened, and logs a warning naming how many claims in a row
  have failed. It retries when its master-down timer next runs out with no
  Master heard: after the master-down interval, then twice that per further
  failure, at most 30 s.
- **Stopping** (the VIP removed, its VRRP changed, the process told to
  stop): a Master sends priority 0, then releases the VIP.
- Advertisements carry the RFC checksum over the IPv4 or IPv6
  pseudo-header. Ones received with an IP TTL / hop limit other than 255
  (IPv6: from `IPV6_HOPLIMIT`) or a bad checksum are dropped. So is one
  whose address list is not exactly this VIP (count 1, that address, no
  bytes after it), as RFC 5798 §7.1 allows: another virtual router on the
  link that reuses the VRID for another address does not reset our timer
  or demote our Master (#45).
- No virtual MAC: the node's own MAC answers for the VIP, announced on
  takeover. For IPv6 that also means no virtual link-local address, so an
  advertisement's address list carries the VIP itself rather than the
  virtual router's link-local first (RFC 5798 §5.2.9). Peers that are all
  stormlb agree on that; a third-party VRRPv3 router on the same VRID may
  not.

**Ownership follows backend health** (beyond the RFC): a Master whose VIP has
no healthy backend resigns (priority 0, release), and a Backup with none
never takes over. The VIP sits on a node that can serve it. If no node has a
healthy backend, no node holds it. The address owner (255) starts as Backup
until it has one.

Claim adds `<vip>/32` to the interface over rtnetlink, then broadcasts three
gratuitous ARP replies, 0.5 s apart, on an `AF_PACKET` socket. For IPv6 it
adds `<vip>/128` with `nodad` (usable at once: the election already decided
the owner) and sends three unsolicited Neighbor Advertisements (Override
set, target link-layer address = the interface MAC) to `ff02::1`, hop limit
255, on a raw ICMPv6 socket. A neighbour that has an entry for the VIP moves
it at once; one without an entry learns it by asking (Linux creates an
entry from an untracked NA only on a router with `accept_untracked_na`,
RFC 9131). Release deletes the address. Both are idempotent. No `ip`,
`arping` or `ndsend` binary is used, so the golden needs none. It needs
`CAP_NET_ADMIN` (the address) and `CAP_NET_RAW` (the VRRP, ARP and ICMPv6
sockets).

### BGP (L3)

A minimal speaker: one outbound session per peer, reconnecting every 5 s
after failure. The session follows RFC 4271's FSM as far as a speaker that
originates one route needs (#6):

1. It sends OPEN (version 4, hold 180 s) with two capabilities (RFC 5492):
   Multiprotocol (RFC 4760) for the VIP's family, unicast (AFI 1 or 2,
   SAFI 1), and 4-octet AS (RFC 6793) with `local_asn`. My AS is
   `local_asn`, or AS_TRANS (23456) when it is above 65535.
2. It reads the peer's OPEN, within 4 minutes, and checks it. The version
   must be 4, the AS must be the configured `asn` (the 4-octet AS
   capability's when offered; a peer configured above 65535 must offer it),
   the hold time must be 0 or at least 3 s, and the VIP's family must be
   negotiated (a peer offering no Multiprotocol capability has IPv4 unicast
   only). Otherwise it sends the matching OPEN Message Error NOTIFICATION
   (subcode 1, 2, 6, or 7 Unsupported Capability with the Multiprotocol
   capability it needs) and drops the session. Optional parameters are read
   in the RFC 4271 and the RFC 9072 extended layout.
3. It answers with a KEEPALIVE and waits for the peer's. Only then is the
   session Established, and only then is any UPDATE sent.
4. The hold time is the smaller of 180 s and the peer's. KEEPALIVEs go
   every third of it, and a peer that sends nothing for a whole hold time
   gets a Hold Timer Expired NOTIFICATION and the session is dropped.
   Hold 0 means neither.

It announces the VIP's host route while the config VIP has at least one
healthy backend, and withdraws it otherwise:

- IPv4: `<vip>/32` in the classic NLRI, ORIGIN IGP, AS_PATH `[local_asn]`,
  NEXT_HOP `next_hop` (default `router_id`); withdrawn in Withdrawn Routes.
- IPv6: `<vip>/128` in MP_REACH_NLRI (AFI 2, SAFI 1, the 16-byte
  `next_hop`), ORIGIN IGP, AS_PATH `[local_asn]`; withdrawn in
  MP_UNREACH_NLRI.
- AS_PATH is 4-byte to a peer that offered the 4-octet AS capability. To
  one that did not, it is 2-byte; with a `local_asn` above 65535 it carries
  AS_TRANS and an AS4_PATH (optional transitive) with the real ASN.

 Health is checked every 250 ms, and a change is sent
at once, so a withdraw follows a failed health check within a quarter
second (it waited for the 60 s keepalive tick before #6). On Established it
announces right away if healthy. Received UPDATEs are read and ignored; a
NOTIFICATION ends the session. Every healthy node advertises the same
route, so the upstream router ECMP-hashes across them. The session's
transport may be IPv4 or IPv6 whatever the VIP's family.

## Ports and endpoints

| Port | Proto | Who | When |
|---|---|---|---|
| `[router] listen`: **80** in the golden | TCP, plain HTTP/1.x, no auth; 308 to https once a `[router.tls]` pair loads (the golden) | clients via `*.storm1.<zone>`; the pod's liveness probe and stormd's `router` API probe on `127.0.0.1:80/healthz` | `[router]` present |
| `[router.tls] listen`: **443** | TLS 1.2/1.3, then HTTP/1.x | clients via `*.storm1.<zone>` | `[router.tls]` present (the golden: `auto:443`, the router's pair from stormcos#363) |
| `[vip] port`, e.g. 6443, and each API VIP's `port` | TCP | kube-api clients via the VIP | `[vip]` present, or a VIP made through the API |
| `[metrics] listen`: **9111** (`auto`: node address and loopback) | plain HTTP, read-only, no token: `/metrics`, `/api/v1/routes` (the router's table as JSON), `/healthz` | ironprom on the node; stormcos `check-metrics.sh`; the console | `[metrics]` present |
| `[api] listen`: **127.0.0.1:9109** by default | plain HTTP, JSON; a bearer token with `token_file` | stormcluster, on the same node | `[api]` present |
| n/a | IP proto 112 to `224.0.0.18` (IPv6: `ff02::12`); ICMPv6 NA to `ff02::1` on an IPv6 takeover | VRRP peers | `[vrrp] enabled`, or `vrrp` in a VIP |
| 179 (outbound only) | TCP | BGP peers | `[bgp] enabled` |
| 180 in the golden | plain HTTP, no auth | **stormd's** API, not stormlb's (port + 100, stormcentral's convention). Its TLS and auth are stormd#32. | golden |

9109 and 9111 are free in stormcentral's registry (#30): until 2026-11-10
the API defaulted to 9103 and metrics to 9104, which are sectionsystems' and
stormblock-csi's registered ports. Both bind the wildcard on the host network,
so whichever started second crash-looped on EADDRINUSE. 9110 is skipped: it is
stormcentral's own API port. **9109 is also stormfn's default** (`0.0.0.0:9109`,
not yet a node service): once it is, whichever binds second fails (#34, open).

stormcos#81 requires every listener on a node to be TLS with a stormcert
certificate and to authenticate, health probes excepted. The router can
serve TLS (`[router.tls]`, #14), and the golden turns it on (#21): stormcos
mints `/data/stormcert/router.{crt,key}` for `*.storm1.<zone>`, the node's
name and address from the node CA, and mounts tier-0 read-only into
stormlb's container (stormcos#363). Once the pair loads, `:80` answers 308
to `https` (`/healthz` excepted, for stormd's probe). A routed host's own
authentication is its backend's.

**Health:** the router's `/healthz` (above), the API's `/healthz`, and the
metrics listener's `/healthz`.

**API health** (#32, stormcentral#596): the registry declares three APIs,
and stormd probes each inside the container every 15 s, sorting it into
healthy, slow (over budget), stalled (no answer within the timeout) or down
(refused, or a status of 400 or more). The node's summary
(`/run/stormpump/health.d/stormlb.json`) carries them, and a test run on a
node where one is stalled or down reports infrastructure, not a failure.

| API | Probe | p50 / p99 | Timeout | What it exercises |
|---|---|---|---|---|
| `router` | `http://127.0.0.1:80/healthz` | 20 / 250 ms | 5 s | The router's accept loop, the request-head parse and the route-table read lock that every request takes. 127.0.0.1 is never a route's hostname, so the router answers itself instead of a backend. A routed request's only other step is the backend's own dial, which no probe can make without a route. |
| `vips` | `http://127.0.0.1:9109/api/v1/vips` | 50 / 500 ms | 10 s | The VIP registry (the lock `PUT`/`DELETE` take), with each VIP's backend health and VRRP state. |
| `metrics` | `http://127.0.0.1:9111/metrics` | 100 / 1000 ms | 10 s | The full render ironprom scrapes: every counter, and the VIPs' health and VRRP state at scrape time. |

None sets `restart_after_stalled_secs`: a restart drops VRRP mastership and
every open connection, so a stall is reported, not acted on. The ports are
the defaults since #30, and the shipped config sets them explicitly.
Each VIP's backends' health and VRRP state are in `GET /api/v1/vips`; a VIP
from `[vip]` without `[api]` shows them only in the logs (and `ip addr`).

### Metrics

`[metrics]` serves Prometheus text (format 0.0.4) on `:9111/metrics` (#12,
for stormcos#64). `GET /metrics` on `:80` is still routing: the 404 for an
unclaimed host, or the backend of a claimed one. Only series that have
happened are printed.

| Series | Type | Labels | What |
|---|---|---|---|
| `stormlb_router_requests_total` | counter | `host`, `code` | Every request. `host` is the route hostname it matched (a wildcard as written, `*.example.com`), or `unrouted` for a host no route claims (a client's Host header never becomes a label). `code` is the backend's status, or the router's own (`400`, `404`, `308`, `200` for `/healthz`, `500`, `502`, `503`, `504`). |
| `stormlb_router_backend_requests_total` | counter | `route`, `backend`, `code` | Requests a backend answered: `route` is `namespace/name`, `backend` `namespace/service:port` (or the `storm.io/backend` address). |
| `stormlb_router_backend_endpoints` | gauge | `route`, `backend`, `state` (`total`, `healthy`) | Each route backend's endpoints, at scrape time. |
| `stormlb_router_request_duration_seconds` | histogram | `host` | From the request arriving to the backend's response head, retries included. Buckets 5 ms to 10 s. |
| `stormlb_router_upstream_errors_total` | counter | `host`, `kind` | `connect` (refused, unreachable: the endpoint is ejected and another tried), `tls` (an https backend: no CA loaded, verification or handshake failed), `no_response` (closed before a response head) or `timeout`. |
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
  exit, and probes the three APIs the registry declares (`apis`, see
  [Ports and endpoints](#ports-and-endpoints)). Liveness, `http://127.0.0.1:80/healthz`,
  is the pod's probe (stormcentral#607). There are also
  `stormlb-data` (64 MiB, `data1`, at `/var/lib/stormlb`) and `stormlb-logs`
  (64 MiB, `system1`, at `/var/log/stormd`).
- The config baked into the golden is the **router, the VIP API** (on
  loopback, saving its VIPs on the data volume) **and metrics** on `:9111`:

  ```toml
  [router]
  listen = "auto:80"
  backend_ca_file = "/data/stormcert/ca.crt"
  ca_file = "/data/stormcert/ca.crt"
  token_file = "/data/stormcert/stormlb.token"

  [router.tls]
  certs = [{ cert_file = "/data/stormcert/router.crt", key_file = "/data/stormcert/router.key" }]

  [api]
  listen = "127.0.0.1:9109"
  state_file = "/var/lib/stormlb/vips.json"

  [metrics]
  listen = "auto:9111"
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
encode/parse/checksum, BGP OPEN/capabilities/UPDATE/withdraw encoding (IPv4 and MP-BGP IPv6, 2- and 4-octet AS paths) and OPEN checks, router header
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
VRRP on the veth. Checked against the kernel's `ip addr` and `ip neigh`,
once with an IPv4 VIP and once with an IPv6 one (`fd09::100`, #39):
takeover after master-down, preemption by the higher priority, the
gratuitous ARP / unsolicited NA updating the peer's neighbour entry, a
resign on no healthy backend, preempting back, priority 0 on removal
cutting the takeover to about a second, and a VIP's VRRP replaced three
times while Master (at priority 255 the new instance claims at once) without
the old instance's release taking the address away (#44), and adverts for
another VIP with the same VRID and a higher priority leaving A Master of its
own (#45). (The IPv6 neighbour check turns on
`forwarding` and `accept_untracked_na` on the observer's veth, since Linux
only makes an entry from an untracked NA on a router; kernel 5.19+, and on
an older kernel it is skipped and says so.)

```
sc-build 'cargo build --release --locked && tests/vrrp-netns.sh'
```

`tests/bgp.rs` runs the speaker against a fake peer on loopback: nothing
announced before Established, announce/withdraw within a second of a health
change, the hold timer, a wrong peer AS refused, 4-octet ASNs on both sides,
AS_TRANS + AS4_PATH to an old peer, an IPv6 VIP announced and withdrawn by
MP_REACH/MP_UNREACH_NLRI, and a peer without IPv6 unicast refused with
Unsupported Capability. BGP against a real router (FRR, BIRD) is not
covered.

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
| `medium` | < 30 min | 400 without a Host, the 404 that names the host, `/healthz` on unclaimed and claimed hosts and its CRLF line endings, Host case and port, per-request routing on one connection, a streamed response not held back, an Upgrade as a two-way pipe, an 8 MiB body, the 16 KiB head limit (431), a dead backend's 502, a route update, a backendRef to its Service's endpoints, a headless Service with no endpoints a 503, rules by longest prefix with a 3:1 weight split, the route's status conditions, a cross-namespace backendRef with no ReferenceGrant refused (500, `RefNotPermitted`), failover past an endpoint that refuses connections, 50 hosts under concurrent load, deleted routes back to 404, no restart or crash under stormd. The VIP half is reported skip: it is not shipped. |
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
  on `:9111/metrics`, but the long suite's residue check doesn't read them
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

- [#34](https://github.com/glennswest/stormlb/issues/34): the API's default
  `127.0.0.1:9109` is stormfn's default port; the clash comes once stormfn
  runs on nodes.
- The API is plain HTTP on loopback, with an optional bearer token: no TLS
  (stormcos#81 allows nothing else off-node, which is why it is loopback).
- [rustkube#347](https://github.com/glennswest/rustkube/issues/347): rustkube
  writes every Running pod as ready in its EndpointSlices and Endpoints, and a
  named `targetPort` as the Service port. Until it is fixed, readiness does not
  take an endpoint out of rotation there; an active check
  (`storm.io/health-check-path`) and passive ejection do.
- [rustkube#348](https://github.com/glennswest/rustkube/issues/348): rustkube
  answers 404 for HTTPRoute `/status`, so the router's status is not seen
  there (routing is unaffected).
- LoadBalancer Services (an address per Service, `status.loadBalancer`,
  announcement, `externalTrafficPolicy: Local`) are flowsdn's, not stormlb's
  (owner, stormlb#37): flowsdn#475.
- [stormcert#69](https://github.com/glennswest/stormcert/issues/69):
  stormcert can't renew the router's wildcard pair (it re-issues it on
  every pass), so stormcos's renew check skips `router.crt` until then.
- VRRP's `CAP_NET_ADMIN` and `CAP_NET_RAW` come from
  stormpump keeping every capability for a container today. Once stormpump#47
  sets a default, stormlb's spec must ask for both.
