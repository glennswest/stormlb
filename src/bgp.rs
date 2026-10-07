//! BGP-anycast advertisement of the VIP (L3, active-active via ECMP).
//!
//! A minimal eBGP (RFC 4271) speaker: it opens a session to each peer, keeps it
//! alive, and advertises the VIP `/32` (next-hop = this node) while the node has
//! healthy backends — withdrawing it otherwise. Every healthy node advertises
//! the same `/32`, so the upstream router ECMP-hashes flows across them: true
//! active-active with route-withdraw failover.
//!
//! The session follows RFC 4271's FSM as far as a speaker that only originates
//! one route needs (#6): OPEN sent, the peer's OPEN read and checked (version
//! 4, its AS as configured, a hold time of 0 or at least 3 s), KEEPALIVE, and
//! only once the peer's KEEPALIVE arrives (Established) any UPDATE. The hold
//! time is the smaller of ours and the peer's; keepalives go every third of
//! it, and a peer silent for a whole hold time gets a Hold Timer Expired
//! NOTIFICATION and the session is dropped. Announce and withdraw follow
//! `advertise` as it changes, not on the keepalive tick.
//!
//! Scope: 2-byte ASNs (private ASNs < 65536 — the on-prem norm); 4-octet ASN
//! capability is a follow-up. IPv4 unicast only.

use crate::config::BgpCfg;
use anyhow::{Context, Result};
use std::net::Ipv4Addr;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::OwnedWriteHalf;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::time::{timeout, Instant};
use tracing::{info, warn};

const HOLD_TIME: u16 = 180;
/// How long to wait for the peer's OPEN (RFC 4271's "large value", 4 min).
const OPEN_WAIT: Duration = Duration::from_secs(240);
// Message types.
const OPEN: u8 = 1;
const UPDATE: u8 = 2;
const NOTIFICATION: u8 = 3;
const KEEPALIVE: u8 = 4;
// NOTIFICATION codes and subcodes we send.
const ERR_HEADER: u8 = 1;
const ERR_OPEN: u8 = 2;
const ERR_OPEN_VERSION: u8 = 1;
const ERR_OPEN_PEER_AS: u8 = 2;
const ERR_OPEN_HOLD: u8 = 6;
const ERR_HOLD_EXPIRED: u8 = 4;
const ERR_FSM: u8 = 5;

/// Spawn one session task per configured peer. Each advertises `vip/32` with
/// next-hop `next_hop` while `advertise` holds true, and reacts to every
/// change of it.
pub fn spawn(cfg: &BgpCfg, vip: Ipv4Addr, next_hop: Ipv4Addr, advertise: watch::Receiver<bool>) {
    let bgp_id: Ipv4Addr = cfg.router_id.parse().unwrap_or(next_hop);
    let local_asn = cfg.local_asn;
    for peer in &cfg.peers {
        let Ok(peer_ip) = peer.address.parse::<Ipv4Addr>() else {
            warn!("bgp: bad peer address {}", peer.address);
            continue;
        };
        let p = Peer { ip: peer_ip, port: peer.port, asn: peer.asn };
        let mut advertise = advertise.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = session(&p, local_asn, bgp_id, next_hop, vip, &mut advertise).await {
                    warn!("bgp: session with {}:{} ended: {e:#}", p.ip, p.port);
                }
                tokio::time::sleep(Duration::from_secs(5)).await; // reconnect backoff
            }
        });
    }
}

struct Peer {
    ip: Ipv4Addr,
    port: u16,
    asn: u32,
}

/// One message from the peer: (type, body).
type Msg = (u8, Vec<u8>);

/// Read whole messages off the socket into a channel: the session's select!
/// then only ever waits on `recv`, which is cancel-safe (a `read_exact` in
/// select! could lose half a message when another arm fires).
fn spawn_reader(mut r: tokio::net::tcp::OwnedReadHalf) -> mpsc::Receiver<Result<Msg>> {
    let (tx, rx) = mpsc::channel(16);
    tokio::spawn(async move {
        loop {
            let m = read_msg(&mut r).await;
            let end = m.is_err();
            if tx.send(m).await.is_err() || end {
                return;
            }
        }
    });
    rx
}

async fn read_msg<R: AsyncReadExt + Unpin>(r: &mut R) -> Result<Msg> {
    let mut hdr = [0u8; 19];
    r.read_exact(&mut hdr).await.context("peer closed")?;
    anyhow::ensure!(hdr[..16] == [0xff; 16], "bad marker");
    let len = u16::from_be_bytes([hdr[16], hdr[17]]) as usize;
    anyhow::ensure!((19..=4096).contains(&len), "bad message length {len}");
    let mut body = vec![0u8; len - 19];
    r.read_exact(&mut body).await.context("short read")?;
    Ok((hdr[18], body))
}

/// The next message, or an error if the reader ended.
async fn next(rx: &mut mpsc::Receiver<Result<Msg>>) -> Result<Msg> {
    rx.recv().await.unwrap_or_else(|| Err(anyhow::anyhow!("peer closed")))
}

/// Send a NOTIFICATION and fail with `why`.
async fn notify<T>(w: &mut OwnedWriteHalf, code: u8, sub: u8, why: String) -> Result<T> {
    let _ = w.write_all(&notification_msg(code, sub)).await;
    anyhow::bail!(why)
}

/// Check the peer's OPEN; Ok(its hold time) or the NOTIFICATION (code,
/// subcode) to send and why.
fn check_open(body: &[u8], want_asn: u32) -> std::result::Result<u16, (u8, u8, String)> {
    if body.len() < 10 {
        return Err((ERR_OPEN, 0, format!("OPEN body of {} bytes", body.len())));
    }
    if body[0] != 4 {
        return Err((ERR_OPEN, ERR_OPEN_VERSION, format!("BGP version {}", body[0])));
    }
    let asn = u16::from_be_bytes([body[1], body[2]]) as u32;
    if asn != want_asn {
        return Err((ERR_OPEN, ERR_OPEN_PEER_AS, format!("peer AS{asn}, configured AS{want_asn}")));
    }
    let hold = u16::from_be_bytes([body[3], body[4]]);
    if hold == 1 || hold == 2 {
        return Err((ERR_OPEN, ERR_OPEN_HOLD, format!("hold time {hold} s")));
    }
    Ok(hold)
}

/// One peering session. Returns Err on any session failure (the caller
/// reconnects).
async fn session(
    p: &Peer,
    local_asn: u32,
    bgp_id: Ipv4Addr,
    next_hop: Ipv4Addr,
    vip: Ipv4Addr,
    advertise: &mut watch::Receiver<bool>,
) -> Result<()> {
    let stream = timeout(Duration::from_secs(10), TcpStream::connect((p.ip, p.port)))
        .await
        .map_err(|_| anyhow::anyhow!("connecting to {}:{} timed out", p.ip, p.port))?
        .with_context(|| format!("connecting to BGP peer {}:{}", p.ip, p.port))?;
    let (r, mut w) = stream.into_split();
    let mut rx = spawn_reader(r);
    info!("bgp: connected to {} (AS{}); local AS{local_asn}", p.ip, p.asn);
    w.write_all(&open_msg(local_asn as u16, HOLD_TIME, bgp_id)).await?;

    // OpenSent: the peer's OPEN.
    let (t, body) = match timeout(OPEN_WAIT, next(&mut rx)).await {
        Ok(m) => m?,
        Err(_) => return notify(&mut w, ERR_HOLD_EXPIRED, 0, "no OPEN from the peer".into()).await,
    };
    match t {
        OPEN => {}
        NOTIFICATION => anyhow::bail!("peer sent NOTIFICATION {:?} instead of OPEN", body.get(..2)),
        _ => return notify(&mut w, ERR_FSM, 0, format!("message type {t} before OPEN")).await,
    }
    let peer_hold = match check_open(&body, p.asn) {
        Ok(h) => h,
        Err((code, sub, why)) => return notify(&mut w, code, sub, format!("refused the peer's OPEN: {why}")).await,
    };
    let hold = HOLD_TIME.min(peer_hold);
    w.write_all(&keepalive_msg()).await?;

    // OpenConfirm: its KEEPALIVE makes the session Established.
    let wait = if hold == 0 { OPEN_WAIT } else { Duration::from_secs(hold as u64) };
    let (t, body) = match timeout(wait, next(&mut rx)).await {
        Ok(m) => m?,
        Err(_) => return notify(&mut w, ERR_HOLD_EXPIRED, 0, "no KEEPALIVE after OPEN".into()).await,
    };
    match t {
        KEEPALIVE => {}
        NOTIFICATION => anyhow::bail!("peer sent NOTIFICATION {:?}", body.get(..2)),
        _ => return notify(&mut w, ERR_FSM, 0, format!("message type {t} in OpenConfirm")).await,
    }
    info!("bgp: session with {} Established (hold {hold} s)", p.ip);

    let hold_d = Duration::from_secs(hold as u64);
    let far = Duration::from_secs(365 * 24 * 3600);
    let mut ticker = tokio::time::interval(if hold == 0 { far } else { hold_d / 3 });
    ticker.tick().await; // the first tick is immediate; the OPEN's KEEPALIVE just went
    let mut hold_deadline = Instant::now() + if hold == 0 { far } else { hold_d };
    let mut announced = false;
    // Announce at once if healthy now; from here on, on every change.
    advertise.mark_changed();

    loop {
        tokio::select! {
            _ = ticker.tick() => w.write_all(&keepalive_msg()).await?,
            _ = tokio::time::sleep_until(hold_deadline) => {
                return notify(&mut w, ERR_HOLD_EXPIRED, 0, format!("nothing from the peer for {hold} s (hold timer)")).await;
            }
            r = advertise.changed() => {
                r.context("health state gone")?;
                let want = *advertise.borrow_and_update();
                if want && !announced {
                    w.write_all(&update_announce(local_asn as u16, next_hop, vip, 32)).await?;
                    announced = true;
                    info!("bgp: advertising {vip}/32 to {} (next-hop {next_hop})", p.ip);
                } else if !want && announced {
                    w.write_all(&update_withdraw(vip, 32)).await?;
                    announced = false;
                    info!("bgp: withdrew {vip}/32 from {}", p.ip);
                }
            }
            m = next(&mut rx) => {
                let (t, body) = m?;
                if hold != 0 {
                    hold_deadline = Instant::now() + hold_d;
                }
                match t {
                    KEEPALIVE | UPDATE => {}
                    NOTIFICATION => anyhow::bail!("peer sent NOTIFICATION {:?}", body.get(..2)),
                    OPEN => return notify(&mut w, ERR_FSM, 0, "OPEN while Established".into()).await,
                    _ => return notify(&mut w, ERR_HEADER, 3, format!("message type {t}")).await,
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

fn notification_msg(code: u8, sub: u8) -> Vec<u8> {
    message(NOTIFICATION, &[code, sub])
}

/// Encode `<prefix_len>` + the significant prefix bytes (BGP prefix encoding).
fn encode_prefix(prefix: Ipv4Addr, prefix_len: u8) -> Vec<u8> {
    let bytes = (prefix_len as usize).div_ceil(8);
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
    fn the_peers_open_is_checked() {
        let open = |ver: u8, asn: u16, hold: u16| {
            let m = open_msg(asn, hold, Ipv4Addr::new(10, 0, 0, 1));
            let mut b = m[19..].to_vec();
            b[0] = ver;
            b
        };
        assert_eq!(check_open(&open(4, 64512, 90), 64512), Ok(90));
        assert_eq!(check_open(&open(4, 64512, 0), 64512), Ok(0), "0 = no keepalives");
        assert_eq!(check_open(&open(3, 64512, 90), 64512).unwrap_err().1, ERR_OPEN_VERSION);
        assert_eq!(check_open(&open(4, 64513, 90), 64512).unwrap_err().1, ERR_OPEN_PEER_AS);
        assert_eq!(check_open(&open(4, 64512, 2), 64512).unwrap_err().1, ERR_OPEN_HOLD);
        assert!(check_open(&[4, 0], 64512).is_err());
        assert_eq!(notification_msg(4, 0)[18..], [NOTIFICATION, 4, 0]);
    }

    #[test]
    fn encode_prefix_uses_significant_bytes() {
        assert_eq!(encode_prefix(Ipv4Addr::new(10, 1, 2, 3), 32), vec![32, 10, 1, 2, 3]);
        assert_eq!(encode_prefix(Ipv4Addr::new(10, 1, 0, 0), 16), vec![16, 10, 1]);
        assert_eq!(encode_prefix(Ipv4Addr::new(10, 0, 0, 0), 8), vec![8, 10]);
    }
}
