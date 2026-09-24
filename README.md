# stormlb

The Storm stack's **pre-cluster control-plane load balancer**, in Rust.

It provides the **kube-api VIP** (and optionally the ingress VIP) that must exist
*before and independent of* the cluster — the chicken-and-egg the in-cluster CNI
(Cilium) can't solve, because Cilium runs as a workload and can't front its own
apiserver. This is OpenShift on-prem's `keepalived + haproxy` role, done in Rust.

**Scope split:** stormlb owns the control-plane VIP(s). Cilium owns the in-cluster
Service/app data plane (eBPF LB + LB-IPAM + BGP / L2 Announcements). Don't use
stormlb for Service traffic.

## What it does

- **Health-checked L4 (TCP) balancer** across the master apiservers — TCP,
  HTTP, or HTTPS `/readyz` checks decide membership; only live masters get
  traffic. (round-robin)
- **VRRP (L2)** virtual-router state machine (RFC 5798) so one node owns the VIP
  with sub-second failover — active-passive ingress, active-active backend.
- **BGP-anycast (L3)** — every healthy node advertises the VIP `/32` and the
  upstream router ECMP-hashes flows → true active-active with route-withdraw
  failover. *(phase 2; interface + design in place.)*

## Modes

| | L2 (VRRP) | L3 (BGP anycast) |
|---|---|---|
| VIP owner | one node at a time | every node (anycast) |
| Failover | VRRP master-down (<1s) | route withdraw |
| Ingress | active-passive | active-active (ECMP) |
| Fabric needs | plain L2 segment | BGP peering |

Both balance the **backend** (masters) via health-checked L4; the difference is
how the VIP itself is presented to the network.

## vs a DNS load balancer

A health-checked, short-TTL DNS LB is a valid approach and can coexist — point
the DNS name at the stormlb VIP, or run stormlb as the tighter-failover
alternative (single stable VIP, no resolver-cache dependency, sub-second).

## Config

TOML — see [`examples/stormlb.toml`](examples/stormlb.toml). Run:

```
stormlb --config /etc/stormlb/stormlb.toml
```

## Status (v0.1)

Implemented and tested (20 tests):
- Config, health checks (TCP/HTTP/HTTPS), L4 balancer with round-robin + failover
  (integration test).
- **VRRP (L2)** — full control loop: `IPPROTO_VRRP` raw socket joining
  `224.0.0.18`, sends/receives advertisements, drives the RFC-5798 state machine
  (Init/Backup/Master, master-down timing, priority + address tie-break) and
  `claim`/`release`s the VIP through iproute2 on transitions. Needs
  `CAP_NET_ADMIN` / `CAP_NET_RAW`.
- **BGP (L3 anycast)** — a minimal eBGP speaker: OPEN/KEEPALIVE handshake and
  UPDATE announce/withdraw of the VIP `/32` (next-hop = this node) driven by
  backend health, with per-peer reconnect. 2-byte ASNs, IPv4 unicast.
- VIP controller (iproute2 + gratuitous ARP) behind a trait.

Follow-ups: 4-octet ASN capability + multiprotocol for BGP; a graceful
priority-0 VRRP resign on shutdown; netlink-native VIP control.

## Build / test

```
cargo build --release --locked
cargo test --locked
```

`Cargo.lock` is committed: goldens are built with `--locked` from an exact
commit, so the lockfile is what says which dependency versions shipped.
Change it only with a deliberate `cargo update` commit.

## Integration

Consumed by **stormcos** as a host service (systemd), started before the kubelet
so the apiserver VIP is up first. See the stormcos integration issue.
