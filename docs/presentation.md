---
marp: true
paginate: true
title: stormlb
description: Purpose and functionality of stormlb, the Storm stack's inbound load balancer
---

<!--
Render: npx @marp-team/marp-cli docs/presentation.md        (HTML)
        npx @marp-team/marp-cli --pdf docs/presentation.md  (PDF)
Every claim here is checkable against the code (src/), README.md and
docs/design.md, as of the commit this file was last changed in.
-->

# stormlb

**The Storm stack's inbound load balancer**

One static Rust binary, two independent halves:

- **Router** (`[router]`): an L7 Host-header demux over Gateway API HTTPRoutes. *Ships today.*
- **VIP half** (`[vip]`, `[api]`): a health-checked L4 proxy for the kube-api VIP, presented by VRRP (L2) or BGP anycast (L3). VIPs can be made and changed at runtime through an HTTP API, for stormcluster (#16).

---

## The problem it solves

**Inbound traffic needs one address, and something at it that knows where to send each request.**

- **Control plane:** kube-apiserver needs a stable VIP *before the cluster exists*. Cilium can't provide it, because it runs as a workload scheduled by the apiserver it would front. OpenShift on-prem uses `keepalived + haproxy` for this. stormlb is that pair in one binary.
- **Apps:** wildcard DNS `*.storm1.<zone>` lands on a node. Something there has to demux on the `Host` header onto whatever HTTPRoutes declare (stormpump `docs/routing.md`).

**Scope split:** stormlb owns inbound (the VIP and the Host demux). Cilium owns in-cluster Service traffic. Don't use the L4 half for Service traffic.

---

## Where it sits in stormcos

From stormcentral's relationships graph (`stormcentral check`, `config/stormcentral.toml`):

```
   stormcos (product) ──┐
                        ├──▶  stormlb  ──▶  stormd
   rustkube ────────────┘   control-plane    (supervisor it runs under)
   (kube-apiserver; the router reads
    HTTPRoutes and Services from it)
```

- **Depends on:** `stormd` (runs it, restarts it, probes `/healthz`). At runtime the router reads from the **apiserver** (rustkube) at `https://127.0.0.1:6443`.
- **Depended on by:** `stormcos`, which ships it, and `rustkube`, whose control-plane VIP is stormlb's job.
- **Its role in stormcentral:** "Load balancer holding the kube-api VIP before the cluster exists". The component is `required = false`.

---

## How it works

```
                client  (Host: app.storm1.<zone>)
                  │
      ┌───────────▼──────────── stormlb ─────────────────────────────┐
      │  ROUTER  :80 (auto: node IP + 127.0.0.1)                     │
      │   read one request head (≤16 KiB) → Host → table lookup      │
      │   replay head to backend, then splice both ways              │
      │        ▲ route table, polled every 5 s (last good kept)      │
      │        └── GET apiserver /apis/gateway.networking.k8s.io/…   │
      │                                                              │
      │  VIP HALF  [vip] :6443 (not in the shipped golden)           │
      │   health loop (tcp|http|https) → healthy set                 │
      │   L4 proxy: round-robin over healthy backends, splice        │
      │   VRRP thread: own the VIP on one node (netlink + raw ARP)   │
      │   BGP tasks:   announce VIP/32 while ≥1 backend is healthy   │
      └──────────────────────────────────────────────────────────────┘
```

Router only if there is no `[vip]`. Otherwise the L4 balancer runs in the foreground, and the router (if configured) runs beside it.

---

## What works today: the router (shipped)

- **One head, then a wire.** Keep-alive, chunked bodies, websockets and SSE pass through untouched. Routing is per connection.
- **Route table:** every `spec.hostnames` entry maps to one backend:
  1. the `storm.io/backend` annotation, `host:port` verbatim (`127.0.0.1:9094` = "this node's console" on every node)
  2. otherwise the first `backendRef`, resolved to the Service's `clusterIP:port`
- **Survives the apiserver:** a failed poll keeps the last good table.
- **Reads anonymously:** it works only against the sno and bastion apiservers, where anonymous is cluster-admin (#9). It also doesn't verify the apiserver's certificate (#10).
- **Its own answers:** `400` with no Host, `404 no route for host <h>`, `200 router alive` for `/healthz` on an unclaimed host.
- **`listen = "auto:80"`** binds the node's routable IPv4 plus loopback, not `0.0.0.0`, because stormimds holds `169.254.169.254:80`.

---

## What works today: the VIP half (the API ships; no VIP until stormcluster makes one)

- **L4 proxy:** round-robin over *healthy* backends, TCP_NODELAY, splice. Backends start unhealthy; the first passing check admits them.
- **Health checks:** `tcp` connect, or `http`/`https` `GET /readyz` with an expected status; `https` verified against `ca_file` (the cluster CA) when given. Checks every 2 s with a 2 s timeout by default.
- **VIP API (`[api]`, #16):** `PUT`/`GET`/`DELETE /api/v1/vips/{name}` on `127.0.0.1:9103`. Named VIPs with their own backends, health and VRRP. A change is validated and bound before it is applied; backends that stay keep their health; proxied connections are never cut; saved to `state_file`.
- **VRRP v3 (RFC 5798 §6.4):** raw IP proto 112 to `224.0.0.18`. Master sends adverts; Backup takes over after the master-down time (3.6 s at defaults), preempts a lower-priority Master, and takes over after Skew_Time when a Master resigns (priority 0). Ownership follows backend health. Claim is `<vip>/32` over rtnetlink plus a raw gratuitous ARP: no `ip` or `arping` needed (#7).
- **BGP:** minimal speaker, one outbound session per peer on TCP 179. Announces `<vip>/32` with NEXT_HOP = `router_id` while a backend is healthy, and withdraws it otherwise. Upstream ECMP gives active-active.

Unit-tested; `tests/balancer.rs` drives round-robin and failover through the real proxy, and `tests/vips.rs` drives the API end to end (create, change backends, move, remove, refusals, token, restart from `state_file`, CA-verified https health). VRRP and BGP on the wire are **not** covered by tests.

---

## Planned and missing, marked as such

| | Status |
|---|---|
| Router credentials for the apiserver, and a CA file to verify it | **planned**, #9, #10 |
| TLS on `:443` (`[router.tls]`: SNI, reload, 308 from `:80`) | **done** (#14); not in the golden until stormcos#363 mints and mounts the wildcard |
| TLS to HTTPS-only backends (stormcos#81) | **done** (#13): `storm.io/backend-protocol: https`, verified against `backend_ca_file`; the golden sets it once stormcos#363 mounts the CA |
| BGP reacting faster than the 60 s keepalive tick, waiting for Established, hold timer | **planned**, #6 |
| Running the test container on test machines | runner shipped (`stormcentral test run`); `test/requires.toml` declares `host_network` (all suites) and the `nodes` read (`long`), `test/build.sh` builds the binary: **ready, not yet run on a test machine** |
| 4-octet ASNs, MP-BGP, IPv6 | follow-ups |
| Wildcard hosts, path/header matches, 502 on a dead backend | not done (router) |
| Sub-second VRRP failover (config takes whole seconds) | not possible today |

---

## Interfaces: CLI and config

```
stormlb [-c|--config <path>]     env STORMLB_CONFIG, default /etc/stormlb/stormlb.toml
RUST_LOG=info                    logs to stderr via tracing
```

One TOML file; a section's presence enables it:

| Section | Keys (defaults) |
|---|---|
| `[router]` | `listen` (`0.0.0.0:80`), `apiserver` (`https://127.0.0.1:6443`), `poll_secs` (5), `insecure` (true) |
| `[api]` | `listen` (`127.0.0.1:9103`), `token_file`, `state_file` |
| `[vip]` | `address`, `port` (required), `bind` (`0.0.0.0`) |
| `[[backend]]` | `address` (IP literal), `port` |
| `[health]` | `mode` (tcp), `path` (`/readyz`), `interval_secs` (2), `timeout_secs` (2), `expect_status` ([200]), `ca_file` |
| `[vrrp]` | `enabled` (false), `interface`, `vrid` (51), `priority` (100), `advert_interval_secs` (1) |
| `[bgp]` | `enabled` (false), `local_asn`, `router_id`, `[[bgp.peers]]` `address`/`asn` |

Full reference: README "Configuration". Example: `examples/stormlb.toml`.

---

## Interfaces: ports, health, metrics

| Port | What |
|---|---|
| **80** (router `listen`) | plain HTTP/1.x, no auth, from clients via `*.storm1.<zone>`; stormd's probe on `127.0.0.1:80/healthz`; 308 to https once a certificate is loaded |
| **443** (`[router.tls] listen`) | TLS, then the same demux; not in the golden until stormcos#363 |
| `[vip] port`, e.g. 6443, and each API VIP's port | kube-api clients via the VIP (not 6443 on a master: stormcluster#35) |
| **127.0.0.1:9103** (`[api]`) | the VIP API, for stormcluster on the node |
| **9104** (`[metrics]`) | Prometheus `/metrics`: requests by host and code, latency, upstream errors, connections, reloads, VIP health, `process_*` (#12) |
| IP proto 112 → `224.0.0.18` | VRRP peers |
| 179, outbound only | BGP peers |
| 180 | **stormd's** API in the golden, not stormlb's; plain, no auth (stormd#32) |

- **Health:** the router's `/healthz`, the API's `/healthz`, and each VIP's backends and VRRP state in `GET /api/v1/vips`.
- **Metrics:** `:9104/metrics` (#12): router requests by host and code, time to first byte, upstream errors, connections, route and certificate reloads, VIP connections and backend health, and the process's own memory, fds and CPU. stormd's `:180` still reports state and restarts.

---

## How it ships and is operated

- **Golden kind:** stormcentral `service`, a 32 MiB `stormlb` golden on pallet `system1`. It holds the static musl `/usr/sbin/stormlb` in a stormd base, plus `stormlb-data` (`/var/lib/stormlb`) and `stormlb-logs` (`/var/log/stormd`).
- **Baked config:** `[router] listen = "auto:80"`, `[api] state_file = "/var/lib/stormlb/vips.json"` (API on `127.0.0.1:9103`), and `[metrics] listen = "auto:9104"`.
- **How it starts:** no systemd; stormpump is PID 1. stormcos `build-goldens.sh` writes a `spec stormlb` stanza into `boot.d/40-services`: a container on the host network profile, sharing UTS. On the **sno** and **bastion** profiles it also writes `start stormlb`. stormd then runs it with `--config /etc/stormlb/stormlb.toml`, restarts it on exit and probes `/healthz`.
- **How it is updated:** push → `sc-build` on dev.g8.lo → `stormcentral component build stormlb`. That builds an immutable golden from the exact commit (`--release --locked`, musl) and files a stormcos release request. A stormcos release then carries the new golden to nodes.

---

## Status

- **Version** 0.1.0 (pre-1.0; `Cargo.toml` is the only version location).
- **Shipping:** the router and the VIP API (loopback, `state_file` on the data volume), in every stormcos build that includes stormlb, and started on sno and bastion nodes.
- **Not yet used:** no node serves a VIP until stormcluster makes one (stormcluster#10, and its port, stormcluster#35). On a single node the VIP is the node's own address, so nothing needs to float.
- **Tests:** unit and integration tests (`cargo test --locked` through `sc-build`), plus the `test/` container: short, medium and long suites against the router on a node, proven by a hermetic harness. The suites declare what they need of the node in `test/requires.toml`; they have not yet run on a test machine.

**Open issues that matter**

- stormcos#363: the router's certificate isn't minted, and `/data/stormcert` isn't mounted, so the golden serves plain `:80` and https routes fail closed (stormcos#81)
- #9: the router reads anonymously, so it works only on sno and bastion (`--dev-anonymous-admin`)
- #10: no CA-file key, so the apiserver's certificate is never verified
- #6: BGP reacts only on the 60 s tick

---

## Where to check any of this

| Claim | Source |
|---|---|
| Config keys and defaults | `src/config.rs`, `src/router.rs` |
| Startup order, router-only mode | `src/main.rs` |
| Router behaviour | `src/router.rs`, `docs/design.md` |
| L4 / health / VRRP / BGP | `src/balancer.rs`, `src/health.rs`, `src/vrrp.rs`, `src/bgp.rs`, `src/vip.rs` |
| Golden, port, config | stormcentral `components/stormcos.toml` |
| Relationships | stormcentral `config/stormcentral.toml` (`stormcentral check`) |
| boot.d spec and start line | stormcos `deploy/build-goldens.sh` |
| Why a router here | stormpump `docs/routing.md` |
