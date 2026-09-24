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
`[bgp]`) but **not shipped**. The stormcos golden is router-only. On a single
node the VIP is the node's own address, so nothing needs to float.

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
- A VRRP Backup doesn't preempt a lower-priority Master, doesn't handle
  priority 0, and VIP ownership doesn't follow backend health
  ([#7](https://github.com/glennswest/stormlb/issues/7)).
- BGP withdraws on "no healthy backends", but only on the next 60 s keepalive
  tick ([#6](https://github.com/glennswest/stormlb/issues/6)). Route-withdraw
  failover is as fast as that tick, not as fast as the health check.
- 4-octet ASNs, multiprotocol BGP, IPv6, a priority-0 resign on shutdown, and
  netlink-native VIP control (instead of `ip` and `arping`) are all follow-ups.

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
more than the first rule and backendRef, TLS termination, a 502 for a dead
backend, and authenticating to the apiserver.
