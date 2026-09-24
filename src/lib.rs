//! stormlb — the Storm stack's pre-cluster control-plane load balancer.
//!
//! Provides the kube-api VIP (and optionally the ingress VIP) that must exist
//! *before and independent of* the cluster — the chicken-and-egg the in-cluster
//! CNI (Cilium) can't solve, since Cilium runs as a workload and can't front its
//! own apiserver. This is OpenShift on-prem's keepalived+haproxy role, in Rust.
//!
//! Pieces:
//! - [`pool`]     — the backend set + round-robin selection over healthy members.
//! - [`health`]   — periodic TCP/HTTP(S) health checks that drive membership.
//! - [`balancer`] — an async L4 (TCP) proxy across healthy backends.
//! - [`vrrp`]     — VRRP (RFC 5798) state machine for L2 VIP ownership/failover.
//! - [`vip`]      — add/remove the VIP on an interface (Linux).
//! - [`bgp`]      — BGP-anycast advertisement of the VIP (L3, active-active).
//! - [`router`]   — the L7 Host-header router over Gateway API HTTPRoutes.
//! - [`config`]   — the TOML config; `[vip]` and `[router]` are each optional.
//!
//! The shipped golden runs the router alone; see README "How it ships".

pub mod balancer;
pub mod bgp;
pub mod config;
pub mod health;
pub mod pool;
pub mod router;
pub mod vip;
pub mod vrrp;
