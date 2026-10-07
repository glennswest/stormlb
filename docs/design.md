# stormlb design notes

Why stormlb is shaped the way it is. The README describes what the code does.
Where this document describes something the code does **not** do yet, it says
so and links the issue.

## Why a pre-cluster VIP at all

The kube-apiserver needs one stable address before the cluster exists.
Kubelets, the other masters and `oc`/`kubectl` all need it. Cilium can't
provide it: it runs as a workload scheduled by the apiserver it would be
fronting. OpenShift on-prem solves this with `keepalived` + `haproxy` as host
services, and stormlb is that pair in one Rust binary.

The scope split follows from that. stormlb owns the control-plane VIP.
Cilium owns in-cluster Service traffic (eBPF LB, LB-IPAM, BGP / L2
announcements).

**Status:** implemented (`[vip]`, `[[backend]]`, `[health]`, `[vrrp]`,
`[bgp]`, and the VIP API `[api]`). The stormcos golden runs the router and
the API, with no VIP until stormcluster makes one. On a single node the VIP
is the node's own address, so nothing needs to float.

## A VIP whose backends change at runtime (#16)

When stormcluster forms a cluster from SNO nodes, kubelets and controllers
talk to one API endpoint that survives any single master going down: a VIP
in front of every master's `:6443` (owner's decision, stormcos#47,
2026-10-02). The set of masters changes while the cluster runs (form, join
as master, promote, demote, split), so the backends can't be fixed at start.

**API, not reload.** stormcluster needs to read the current backends before
it acts, and to know whether a change took, so it gets an HTTP API
(`PUT`/`GET`/`DELETE /api/v1/vips/{name}`) rather than a SIGHUP reload of the
TOML. Each VIP is named, so one process can serve more than one, and each
carries its own listener, pool, health spec and (optionally) VRRP instance.
The TOML `[vip]` is the VIP `default`, read-only through the API, so a file
and an API never fight over one VIP.

**A change never takes the VIP down.** A spec is validated, and any new
listener bound, before the running VIP is touched; a refusal leaves it as it
was. A backend that stays keeps its health (re-applying the same masters is
free), a new one is probed at once, and connections already proxied are
never cut. stormcluster can therefore apply its whole desired state each
time, idempotently.

**L4 pass-through.** TLS terminates on the apiservers, whose serving
certificate lists the VIP among its SANs, so stormlb never holds a key.
Health is `GET /readyz` over https, verified against the cluster CA
(`health.ca_file`), so a backend counts only when it is a real apiserver of
this cluster.

**Binding.** A VIP's listener binds the VIP address with `IP_FREEBIND`, so
every master listens before it holds the address, and a VRRP takeover needs
no rebind. It can't share `:6443` with the apiserver's wildcard bind on the
same node, which is why the VIP's port is stormcluster's choice
(glennswest/stormcluster#35).

**Persistence and access.** VIPs made through the API are saved to
`state_file` and served again at start, so a stormlb restart doesn't wait for
stormcluster to notice. The API is loopback by default: stormcluster runs on
every node and calls its own stormlb. Anywhere else, it needs a bearer
token.

**Moving between masters (#7).** Each VIP's VRRP instance follows RFC 5798
preemption (the highest-priority healthy master holds it), resigns with
priority 0 when it stops or loses every healthy backend, and claims the
address over rtnetlink with a raw gratuitous ARP, so the golden needs no
`ip` or `arping`.

## L2 vs L3

| | L2 (VRRP) | L3 (BGP anycast) |
|---|---|---|
| VIP owner | one node at a time | every healthy node |
| Failover | master-down timer (3 × advert + skew; 3.6 s at the defaults) | route withdraw |
| Ingress | active-passive | active-active (router ECMP) |
| Fabric needs | a plain L2 segment | BGP peering on the upstream router |

Both balance the backend (the masters) the same way, through the
health-checked L4 proxy. The only difference is how the VIP is presented to
the network.

**What the code does not do yet:**

- The VRRP master-down time is 3.6 s at a 1 s advert interval. That's the RFC
  formula, not the "sub-second" earlier docs claimed. Going sub-second needs
  a sub-second advert interval, and the config only takes whole seconds.
- BGP withdraws on "no healthy backends", but only on the next 60 s keepalive
  tick ([#6](https://github.com/glennswest/stormlb/issues/6)). Route-withdraw
  failover is as fast as that tick, not as fast as the health check.
- 4-octet ASNs, multiprotocol BGP and IPv6 are follow-ups.

**VRRP ownership follows health, not just liveness** (#7). Plain VRRP moves
the VIP only when its holder stops advertising. A holder whose backends are
all down would keep the VIP and drop every connection. So a Master with no
healthy backend resigns (priority 0, so a Backup takes over after Skew_Time,
well under a second), and a Backup with none never takes over. On the API
VIP every master checks the same set of apiservers, so this matters when a
node is cut off from the others, not when one apiserver fails. Preemption
(RFC default, `preempt = true`) gives the VIP back to the highest-priority
healthy node when it returns.

## stormlb vs a DNS load balancer

A health-checked, short-TTL DNS LB is a valid approach and can coexist. The
DNS name can point at the stormlb VIP, or stormlb can be the tighter-failover
alternative: one stable address, no dependency on resolver caches. Nothing in
stormlb depends on DNS. Backends are IP literals.

## Why the L7 router lives here

stormpump `docs/routing.md` describes inbound as: wildcard DNS
`*.storm1.<zone>` goes to a VIP, and something at the VIP demuxes on the Host
header. stormlb already owns the VIP, the health checks and the L4 path, so
the demux is here too. One component owns one request path. The alternative,
where the VIP is ours and the L7 hop is Cilium/Envoy's, gives two failure
domains and no single place to debug.

Choices that follow from the code (`src/router.rs`):

- **Per connection, not per request.** It reads one head, picks a backend and
  splices. Keep-alive, websockets and SSE come free. The cost: a connection
  whose later request names another host stays on the first backend.
  Browsers don't reuse a connection across distinct hostnames.
- **Polled, not watched.** A route change is an operator action, so 5 s of
  staleness is invisible. A poll also can't be wedged by a watch
  implementation bug in a reimplemented apiserver. The last good table
  survives apiserver errors.
- **`storm.io/backend` before backendRefs.** A node service is
  `127.0.0.1:<port>` on every node, so cluster manifests carry no node
  addresses.
- **`auto` listen.** It binds the node's routable address and loopback, not
  the wildcard, because stormimds holds `169.254.169.254:80`.

Deliberately not done (yet): wildcard hostnames, path and header matching,
more than the first rule and backendRef, and a 502 for a dead backend.

**Metrics follow the routing (#12).** The router routes per connection and
splices after the first head, so it counts the first request of each
connection. It reads the backend's first bytes on the way, for the status
and the time to first byte, while the client→backend half keeps copying, so
an upload is never held behind a response that waits for it. The `host`
label is a route's hostname or `unrouted`, never the client's Host header,
so a scanner can't grow the series set. Metrics have their own port
(`:9104`), because `/metrics` on `:80` is a routable path.

TLS is now a requirement, not an option: stormcos#81 has every node listener
serve TLS with a stormcert certificate and authenticate. The router
terminates TLS (`[router.tls]`, #14). The certificate is a file pair, not
something the router mints or fetches: stormcert issues it, stormcos
mounts it (stormcos#363), and the router re-reads it when it changes. So
the router doesn't depend on how stormcert will deliver per-route
certificates (stormcert#1, gap 3): any number of pairs, chosen by SNI,
already works. `:80` redirects once a certificate is loaded, never before,
so a node without one keeps working over plain HTTP. `/healthz` stays plain
because the health probe is the one exemption #81 allows. The router still
dials every backend over bare TCP, so an HTTPS-only backend can't be routed
([#13](https://github.com/glennswest/stormlb/issues/13)).

Its apiserver client is the other gap. The router reads anonymously, which
works only against the sno and bastion apiservers' `--dev-anonymous-admin`
([#9](https://github.com/glennswest/stormlb/issues/9)). It also can't verify
the apiserver's certificate, because it trusts only compiled-in public roots
and has no CA-file key
([#10](https://github.com/glennswest/stormlb/issues/10)).
