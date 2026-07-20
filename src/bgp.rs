//! BGP-anycast advertisement of the VIP (L3, active-active via ECMP).
//!
//! A minimal eBGP (RFC 4271) speaker: it opens a session to each peer, keeps it
//! alive, and advertises the VIP `/32` (next-hop = this node) while the node has
//! healthy backends — withdrawing it otherwise. Every healthy node advertises
//! the same `/32`, so the upstream router ECMP-hashes flows across them: true
//! active-active with route-withdraw failover.
//!
//! Scope: 2-byte ASNs (private ASNs < 65536 — the on-prem norm); 4-octet ASN
//! capability is a follow-up. IPv4 unicast only.

use crate::config::BgpCfg;
use anyhow::{Context, Result};
use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{info, warn};

const BGP_PORT: u16 = 179;
const HOLD_TIME: u16 = 180;
// Message types.
const OPEN: u8 = 1;
const UPDATE: u8 = 2;
const KEEPALIVE: u8 = 4;

/// Spawn one session task per configured peer. Each advertises `vip/32` with
/// next-hop `next_hop` while `advertise` is true.
pub fn spawn(cfg: &BgpCfg, vip: Ipv4Addr, next_hop: Ipv4Addr, advertise: Arc<AtomicBool>) {
    let bgp_id: Ipv4Addr = cfg.router_id.parse().unwrap_or(next_hop);
    let local_asn = cfg.local_asn;
    for peer in &cfg.peers {
        let Ok(peer_ip) = peer.address.parse::<Ipv4Addr>() else {
            warn!("bgp: bad peer address {}", peer.address);
            continue;
        };
        let peer_asn = peer.asn;
        let advertise = advertise.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) =
                    session(peer_ip, peer_asn, local_asn, bgp_id, next_hop, vip, &advertise).await
                {
                    warn!("bgp: session with {peer_ip} ended: {e}");
                }
                tokio::time::sleep(Duration::from_secs(5)).await; // reconnect backoff
            }
        });
    }
}

/// One peering session: OPEN handshake, keepalives, and advertise/withdraw the
/// VIP as `advertise` changes. Returns Err on any session failure (caller
/// reconnects).
async fn session(
    peer: Ipv4Addr,
    peer_asn: u32,
    local_asn: u32,
    bgp_id: Ipv4Addr,
    next_hop: Ipv4Addr,
    vip: Ipv4Addr,
    advertise: &AtomicBool,
) -> Result<()> {
    let mut stream = TcpStream::connect((peer, BGP_PORT))
        .await
        .with_context(|| format!("connecting to BGP peer {peer}"))?;
    info!("bgp: connected to {peer} (AS{peer_asn}); local AS{local_asn}");

    stream.write_all(&open_msg(local_asn as u16, HOLD_TIME, bgp_id)).await?;
    stream.write_all(&keepalive_msg()).await?;

    let keepalive_every = Duration::from_secs((HOLD_TIME / 3).max(1) as u64);
    let mut ticker = tokio::time::interval(keepalive_every);
    let mut announced = false;
    let mut hdr = [0u8; 19];

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                stream.write_all(&keepalive_msg()).await?;
                // Reconcile advertised state with health.
                let want = advertise.load(Ordering::Relaxed);
                if want && !announced {
                    stream.write_all(&update_announce(local_asn as u16, next_hop, vip, 32)).await?;
                    announced = true;
                    info!("bgp: advertising {vip}/32 to {peer} (next-hop {next_hop})");
                } else if !want && announced {
                    stream.write_all(&update_withdraw(vip, 32)).await?;
                    announced = false;
                    info!("bgp: withdrew {vip}/32 from {peer}");
                }
            }
            r = stream.read_exact(&mut hdr) => {
                r.context("bgp: peer closed")?;
                // Header: 16-byte marker, 2-byte length, 1-byte type.
                let len = u16::from_be_bytes([hdr[16], hdr[17]]) as usize;
                let body = len.saturating_sub(19);
                if body > 0 {
                    let mut drain = vec![0u8; body];
                    stream.read_exact(&mut drain).await.context("bgp: short read")?;
                }
                // We only need to keep the session up; NOTIFICATION (type 3) ends it.
                if hdr[18] == 3 {
                    anyhow::bail!("bgp: peer sent NOTIFICATION");
                }
            }
        }
    }
}

// ---- message encoders ------------------------------------------------------

/// Wrap a body in the 19-byte BGP header (marker + length + type).
fn message(msg_type: u8, body: &[u8]) -> Vec<u8> {
    let len = (19 + body.len()) as u16;
    let mut m = Vec::with_capacity(len as usize);
    m.extend_from_slice(&[0xff; 16]); // marker
    m.extend_from_slice(&len.to_be_bytes());
    m.push(msg_type);
    m.extend_from_slice(body);
    m
}

fn open_msg(local_asn: u16, hold: u16, bgp_id: Ipv4Addr) -> Vec<u8> {
    let mut body = Vec::new();
    body.push(4); // BGP version
    body.extend_from_slice(&local_asn.to_be_bytes());
    body.extend_from_slice(&hold.to_be_bytes());
    body.extend_from_slice(&bgp_id.octets());
    body.push(0); // optional-parameters length (none)
    message(OPEN, &body)
}

fn keepalive_msg() -> Vec<u8> {
    message(KEEPALIVE, &[])
}

/// Encode `<prefix_len>` + the significant prefix bytes (BGP prefix encoding).
fn encode_prefix(prefix: Ipv4Addr, prefix_len: u8) -> Vec<u8> {
    let bytes = ((prefix_len as usize) + 7) / 8;
    let mut v = Vec::with_capacity(1 + bytes);
    v.push(prefix_len);
    v.extend_from_slice(&prefix.octets()[..bytes]);
    v
}

/// UPDATE announcing `prefix/len` with ORIGIN=IGP, AS_PATH=[local_asn],
/// NEXT_HOP=next_hop.
fn update_announce(local_asn: u16, next_hop: Ipv4Addr, prefix: Ipv4Addr, prefix_len: u8) -> Vec<u8> {
    let mut attrs = Vec::new();
    // ORIGIN (well-known, transitive): type 1, len 1, value 0 = IGP.
    attrs.extend_from_slice(&[0x40, 1, 1, 0]);
    // AS_PATH: type 2; segment AS_SEQUENCE (2), count 1, one 2-byte AS.
    attrs.extend_from_slice(&[0x40, 2, 4, 2, 1]);
    attrs.extend_from_slice(&local_asn.to_be_bytes());
    // NEXT_HOP: type 3, len 4.
    attrs.extend_from_slice(&[0x40, 3, 4]);
    attrs.extend_from_slice(&next_hop.octets());

    let mut body = Vec::new();
    body.extend_from_slice(&0u16.to_be_bytes()); // withdrawn routes length = 0
    body.extend_from_slice(&(attrs.len() as u16).to_be_bytes());
    body.extend_from_slice(&attrs);
    body.extend_from_slice(&encode_prefix(prefix, prefix_len)); // NLRI
    message(UPDATE, &body)
}

/// UPDATE withdrawing `prefix/len`.
fn update_withdraw(prefix: Ipv4Addr, prefix_len: u8) -> Vec<u8> {
    let withdrawn = encode_prefix(prefix, prefix_len);
    let mut body = Vec::new();
    body.extend_from_slice(&(withdrawn.len() as u16).to_be_bytes());
    body.extend_from_slice(&withdrawn);
    body.extend_from_slice(&0u16.to_be_bytes()); // total path attribute length = 0
    message(UPDATE, &body)
}

/// Validate config; returns the next-hop (this node's IPv4, from `router_id`)
/// used in advertisements.
pub fn preflight(cfg: &BgpCfg) -> Result<Ipv4Addr> {
    if cfg.local_asn == 0 || cfg.local_asn > u16::MAX as u32 {
        anyhow::bail!("bgp.local_asn must be a 2-byte private ASN (1..65535) for now");
    }
    if cfg.peers.is_empty() {
        anyhow::bail!("bgp.enabled but no peers configured");
    }
    cfg.router_id
        .parse::<Ipv4Addr>()
        .map_err(|_| anyhow::anyhow!("bgp.router_id must be this node's IPv4 (used as next-hop)"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_framing_is_correct() {
        let m = keepalive_msg();
        assert_eq!(m.len(), 19);
        assert_eq!(&m[0..16], &[0xff; 16]);
        assert_eq!(u16::from_be_bytes([m[16], m[17]]), 19);
        assert_eq!(m[18], KEEPALIVE);
    }

    #[test]
    fn open_has_version_asn_hold_id() {
        let m = open_msg(64512, 180, Ipv4Addr::new(192, 168, 8, 51));
        assert_eq!(m[18], OPEN);
        let body = &m[19..];
        assert_eq!(body[0], 4); // version
        assert_eq!(u16::from_be_bytes([body[1], body[2]]), 64512);
        assert_eq!(u16::from_be_bytes([body[3], body[4]]), 180);
        assert_eq!(&body[5..9], &[192, 168, 8, 51]);
        assert_eq!(body[9], 0); // no opt params
    }

    #[test]
    fn announce_carries_attrs_and_nlri() {
        let m = update_announce(64512, Ipv4Addr::new(192, 168, 8, 51), Ipv4Addr::new(192, 168, 8, 50), 32);
        assert_eq!(m[18], UPDATE);
        let body = &m[19..];
        assert_eq!(u16::from_be_bytes([body[0], body[1]]), 0); // no withdrawn
        let attr_len = u16::from_be_bytes([body[2], body[3]]) as usize;
        let nlri = &body[4 + attr_len..];
        assert_eq!(nlri, &[32, 192, 168, 8, 50]); // /32 prefix
        // NEXT_HOP attribute value present.
        let attrs = &body[4..4 + attr_len];
        assert!(attrs.windows(4).any(|w| w == [192, 168, 8, 51]));
    }

    #[test]
    fn withdraw_lists_the_prefix() {
        let m = update_withdraw(Ipv4Addr::new(192, 168, 8, 50), 32);
        let body = &m[19..];
        let wlen = u16::from_be_bytes([body[0], body[1]]) as usize;
        assert_eq!(&body[2..2 + wlen], &[32, 192, 168, 8, 50]);
        // No path attributes.
        assert_eq!(u16::from_be_bytes([body[2 + wlen], body[3 + wlen]]), 0);
    }

    #[test]
    fn encode_prefix_uses_significant_bytes() {
        assert_eq!(encode_prefix(Ipv4Addr::new(10, 1, 2, 3), 32), vec![32, 10, 1, 2, 3]);
        assert_eq!(encode_prefix(Ipv4Addr::new(10, 1, 0, 0), 16), vec![16, 10, 1]);
        assert_eq!(encode_prefix(Ipv4Addr::new(10, 0, 0, 0), 8), vec![8, 10]);
    }
}
