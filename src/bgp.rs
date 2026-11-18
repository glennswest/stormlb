//! BGP-anycast advertisement of the VIP (L3, active-active via ECMP).
//!
//! A minimal eBGP (RFC 4271) speaker: it opens a session to each peer, keeps it
//! alive, and advertises the VIP as a host route (`/32` or `/128`, next-hop =
//! this node) while the node has healthy backends — withdrawing it otherwise.
//! Every healthy node advertises the same route, so the upstream router
//! ECMP-hashes flows across them: true active-active with route-withdraw
//! failover.
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
//! Capabilities (RFC 5492, #40). The OPEN offers two:
//! - **4-octet AS** (RFC 6793): our full ASN, with AS_TRANS (23456) in My AS
//!   when it does not fit 16 bits. A peer that offers it too is identified by
//!   its 4-octet ASN and gets a 4-byte AS_PATH. A peer that does not (an "old"
//!   speaker) gets a 2-byte AS_PATH, with AS_TRANS and an AS4_PATH carrying
//!   the real ASN when ours is above 65535; a peer configured with an ASN
//!   above 65535 must offer it, or its OPEN is refused (Bad Peer AS).
//! - **Multiprotocol** (RFC 4760) for the VIP's family, unicast: AFI 1 for an
//!   IPv4 VIP, AFI 2 for an IPv6 one. An IPv4 VIP goes in the classic NLRI
//!   with NEXT_HOP; an IPv6 VIP in MP_REACH_NLRI / MP_UNREACH_NLRI with a
//!   16-byte global next-hop. The family must be negotiated: a peer that does
//!   not offer it (a peer offering no Multiprotocol capability at all has
//!   IPv4 unicast only) is refused with Unsupported Capability.
//!
//! The transport may be IPv4 or IPv6 whatever the VIP's family.

use crate::config::BgpCfg;
use anyhow::{Context, Result};
use std::net::{IpAddr, Ipv4Addr};
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
/// RFC 6793: the 2-octet stand-in for an ASN above 65535.
pub const AS_TRANS: u32 = 23456;
// Message types.
const OPEN: u8 = 1;
const UPDATE: u8 = 2;
const NOTIFICATION: u8 = 3;
const KEEPALIVE: u8 = 4;
// OPEN optional parameter and capability codes.
const OPT_CAPABILITIES: u8 = 2;
const CAP_MULTIPROTOCOL: u8 = 1;
const CAP_AS4: u8 = 65;
// Address families.
const AFI_IPV4: u16 = 1;
const AFI_IPV6: u16 = 2;
const SAFI_UNICAST: u8 = 1;
// Path attributes (type codes).
const ATTR_ORIGIN: u8 = 1;
const ATTR_AS_PATH: u8 = 2;
const ATTR_NEXT_HOP: u8 = 3;
const ATTR_MP_REACH: u8 = 14;
const ATTR_MP_UNREACH: u8 = 15;
const ATTR_AS4_PATH: u8 = 17;
// Attribute flags.
const FLAG_OPTIONAL: u8 = 0x80;
const FLAG_TRANSITIVE: u8 = 0x40;
const AS_SEQUENCE: u8 = 2;
// NOTIFICATION codes and subcodes we send.
const ERR_HEADER: u8 = 1;
const ERR_OPEN: u8 = 2;
const ERR_OPEN_VERSION: u8 = 1;
const ERR_OPEN_PEER_AS: u8 = 2;
const ERR_OPEN_HOLD: u8 = 6;
const ERR_OPEN_UNSUPPORTED_CAP: u8 = 7;
const ERR_HOLD_EXPIRED: u8 = 4;
const ERR_FSM: u8 = 5;

/// An address family: (AFI, SAFI).
type Family = (u16, u8);

fn family_of(ip: IpAddr) -> Family {
    match ip {
        IpAddr::V4(_) => (AFI_IPV4, SAFI_UNICAST),
        IpAddr::V6(_) => (AFI_IPV6, SAFI_UNICAST),
    }
}

/// Spawn one session task per configured peer. Each advertises the VIP's
/// host route with next-hop `next_hop` (from [`preflight`]) while `advertise`
/// holds true, and reacts to every change of it.
pub fn spawn(cfg: &BgpCfg, vip: IpAddr, next_hop: IpAddr, advertise: watch::Receiver<bool>) {
    let bgp_id: Ipv4Addr = cfg.router_id.parse().unwrap_or(match next_hop {
        IpAddr::V4(a) => a,
        IpAddr::V6(_) => Ipv4Addr::UNSPECIFIED,
    });
    let local = Local { asn: cfg.local_asn, bgp_id, next_hop, vip };
    for peer in &cfg.peers {
        let Ok(peer_ip) = peer.address.parse::<IpAddr>() else {
            warn!("bgp: bad peer address {}", peer.address);
            continue;
        };
        let p = Peer { ip: peer_ip, port: peer.port, asn: peer.asn };
        let mut advertise = advertise.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = session(&p, &local, &mut advertise).await {
                    warn!("bgp: session with {} ended: {e:#}", p.addr());
                }
                tokio::time::sleep(Duration::from_secs(5)).await; // reconnect backoff
            }
        });
    }
}

struct Peer {
    ip: IpAddr,
    port: u16,
    asn: u32,
}

impl Peer {
    fn addr(&self) -> std::net::SocketAddr {
        (self.ip, self.port).into()
    }
}

/// This speaker's side of every session.
#[derive(Clone, Copy)]
struct Local {
    asn: u32,
    bgp_id: Ipv4Addr,
    next_hop: IpAddr,
    vip: IpAddr,
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
async fn notify<T>(w: &mut OwnedWriteHalf, code: u8, sub: u8, data: &[u8], why: String) -> Result<T> {
    let _ = w.write_all(&notification_msg(code, sub, data)).await;
    anyhow::bail!(why)
}

/// What the peer's OPEN negotiated.
#[derive(Debug, PartialEq)]
struct PeerOpen {
    hold: u16,
    /// It offered the 4-octet AS capability: 4-byte AS_PATH to it.
    as4: bool,
    /// Its AS (the 4-octet capability's when offered).
    asn: u32,
}

/// Why the peer's OPEN is refused: the NOTIFICATION to send and the log line.
#[derive(Debug, PartialEq)]
struct Refusal {
    code: u8,
    sub: u8,
    data: Vec<u8>,
    why: String,
}

fn refuse(sub: u8, why: String) -> Refusal {
    Refusal { code: ERR_OPEN, sub, data: Vec::new(), why }
}

/// The OPEN's optional parameters as (type, value), in either the RFC 4271
/// layout or RFC 9072's extended one (length 255, type 255, then 2-byte
/// lengths).
fn open_params(body: &[u8]) -> std::result::Result<Vec<(u8, &[u8])>, String> {
    let (mut i, end, wide) = if body[9] == 255 && body.get(10) == Some(&255) {
        let len = body.get(11..13).ok_or("short extended parameters length")?;
        (13, 13 + u16::from_be_bytes([len[0], len[1]]) as usize, true)
    } else {
        (10, 10 + body[9] as usize, false)
    };
    if end != body.len() {
        return Err(format!("optional parameters run to {end}, OPEN body is {} bytes", body.len()));
    }
    let mut out = Vec::new();
    while i < end {
        let t = body[i];
        let (len, at) = if wide {
            let l = body.get(i + 1..i + 3).ok_or("truncated parameter")?;
            (u16::from_be_bytes([l[0], l[1]]) as usize, i + 3)
        } else {
            (*body.get(i + 1).ok_or("truncated parameter")? as usize, i + 2)
        };
        let v = body.get(at..at + len).filter(|_| at + len <= end).ok_or("parameter overruns the OPEN")?;
        out.push((t, v));
        i = at + len;
    }
    Ok(out)
}

/// The capabilities in a Capabilities parameter's value, as (code, value).
fn capabilities(mut v: &[u8]) -> std::result::Result<Vec<(u8, &[u8])>, String> {
    let mut out = Vec::new();
    while !v.is_empty() {
        let (code, len) = (v[0], *v.get(1).ok_or("truncated capability")? as usize);
        let val = v.get(2..2 + len).ok_or("capability overruns its parameter")?;
        out.push((code, val));
        v = &v[2 + len..];
    }
    Ok(out)
}

/// Check the peer's OPEN against the AS it must be and the family we
/// announce; what it negotiated, or the NOTIFICATION to refuse it with.
fn check_open(body: &[u8], want_asn: u32, family: Family) -> std::result::Result<PeerOpen, Refusal> {
    if body.len() < 10 {
        return Err(refuse(0, format!("OPEN body of {} bytes", body.len())));
    }
    if body[0] != 4 {
        return Err(refuse(ERR_OPEN_VERSION, format!("BGP version {}", body[0])));
    }
    let my_as = u16::from_be_bytes([body[1], body[2]]) as u32;
    let hold = u16::from_be_bytes([body[3], body[4]]);
    let params = open_params(body).map_err(|e| refuse(0, e))?;
    let (mut as4, mut families) = (None, Vec::new());
    for (t, v) in params {
        if t != OPT_CAPABILITIES {
            continue; // RFC 5492 drops the others; nothing we need
        }
        for (code, val) in capabilities(v).map_err(|e| refuse(0, e))? {
            match (code, val.len()) {
                (CAP_AS4, 4) => as4 = Some(u32::from_be_bytes([val[0], val[1], val[2], val[3]])),
                (CAP_MULTIPROTOCOL, 4) => families.push((u16::from_be_bytes([val[0], val[1]]), val[3])),
                _ => {}
            }
        }
    }
    let asn = as4.unwrap_or(my_as);
    if asn != want_asn {
        let why = match as4 {
            None if want_asn > u16::MAX as u32 => {
                format!("peer AS{asn} without the 4-octet AS capability, configured AS{want_asn}")
            }
            _ => format!("peer AS{asn}, configured AS{want_asn}"),
        };
        return Err(refuse(ERR_OPEN_PEER_AS, why));
    }
    if hold == 1 || hold == 2 {
        return Err(refuse(ERR_OPEN_HOLD, format!("hold time {hold} s")));
    }
    // RFC 4760: no Multiprotocol capability at all = IPv4 unicast only.
    let negotiated = families.contains(&family) || (families.is_empty() && family == (AFI_IPV4, SAFI_UNICAST));
    if !negotiated {
        return Err(Refusal {
            code: ERR_OPEN,
            sub: ERR_OPEN_UNSUPPORTED_CAP,
            data: mp_capability(family).to_vec(),
            why: format!("the peer does not offer AFI {}/SAFI {} (offers {families:?})", family.0, family.1),
        });
    }
    Ok(PeerOpen { hold, as4: as4.is_some(), asn })
}

/// One peering session. Returns Err on any session failure (the caller
/// reconnects).
async fn session(p: &Peer, local: &Local, advertise: &mut watch::Receiver<bool>) -> Result<()> {
    let stream = timeout(Duration::from_secs(10), TcpStream::connect(p.addr()))
        .await
        .map_err(|_| anyhow::anyhow!("connecting to {} timed out", p.addr()))?
        .with_context(|| format!("connecting to BGP peer {}", p.addr()))?;
    let (r, mut w) = stream.into_split();
    let mut rx = spawn_reader(r);
    let family = family_of(local.vip);
    info!("bgp: connected to {} (AS{}); local AS{}", p.ip, p.asn, local.asn);
    w.write_all(&open_msg(local.asn, HOLD_TIME, local.bgp_id, family)).await?;

    // OpenSent: the peer's OPEN.
    let (t, body) = match timeout(OPEN_WAIT, next(&mut rx)).await {
        Ok(m) => m?,
        Err(_) => return notify(&mut w, ERR_HOLD_EXPIRED, 0, &[], "no OPEN from the peer".into()).await,
    };
    match t {
        OPEN => {}
        NOTIFICATION => anyhow::bail!("peer sent NOTIFICATION {:?} instead of OPEN", body.get(..2)),
        _ => return notify(&mut w, ERR_FSM, 0, &[], format!("message type {t} before OPEN")).await,
    }
    let peer = match check_open(&body, p.asn, family) {
        Ok(o) => o,
        Err(r) => return notify(&mut w, r.code, r.sub, &r.data, format!("refused the peer's OPEN: {}", r.why)).await,
    };
    let hold = HOLD_TIME.min(peer.hold);
    w.write_all(&keepalive_msg()).await?;

    // OpenConfirm: its KEEPALIVE makes the session Established.
    let wait = if hold == 0 { OPEN_WAIT } else { Duration::from_secs(hold as u64) };
    let (t, body) = match timeout(wait, next(&mut rx)).await {
        Ok(m) => m?,
        Err(_) => return notify(&mut w, ERR_HOLD_EXPIRED, 0, &[], "no KEEPALIVE after OPEN".into()).await,
    };
    match t {
        KEEPALIVE => {}
        NOTIFICATION => anyhow::bail!("peer sent NOTIFICATION {:?}", body.get(..2)),
        _ => return notify(&mut w, ERR_FSM, 0, &[], format!("message type {t} in OpenConfirm")).await,
    }
    info!(
        "bgp: session with {} (AS{}) Established (hold {hold} s, {}-octet AS)",
        p.ip,
        peer.asn,
        if peer.as4 { 4 } else { 2 }
    );

    let hold_d = Duration::from_secs(hold as u64);
    let far = Duration::from_secs(365 * 24 * 3600);
    let mut ticker = tokio::time::interval(if hold == 0 { far } else { hold_d / 3 });
    ticker.tick().await; // the first tick is immediate; the OPEN's KEEPALIVE just went
    let mut hold_deadline = Instant::now() + if hold == 0 { far } else { hold_d };
    let mut announced = false;
    let route = format!("{}/{}", local.vip, host_len(local.vip));
    // Announce at once if healthy now; from here on, on every change.
    advertise.mark_changed();

    loop {
        tokio::select! {
            _ = ticker.tick() => w.write_all(&keepalive_msg()).await?,
            _ = tokio::time::sleep_until(hold_deadline) => {
                return notify(&mut w, ERR_HOLD_EXPIRED, 0, &[], format!("nothing from the peer for {hold} s (hold timer)")).await;
            }
            r = advertise.changed() => {
                r.context("health state gone")?;
                let want = *advertise.borrow_and_update();
                if want && !announced {
                    w.write_all(&update_announce(local.asn, peer.as4, local.next_hop, local.vip)).await?;
                    announced = true;
                    info!("bgp: advertising {route} to {} (next-hop {})", p.ip, local.next_hop);
                } else if !want && announced {
                    w.write_all(&update_withdraw(local.vip)).await?;
                    announced = false;
                    info!("bgp: withdrew {route} from {}", p.ip);
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
                    OPEN => return notify(&mut w, ERR_FSM, 0, &[], "OPEN while Established".into()).await,
                    _ => return notify(&mut w, ERR_HEADER, 3, &[t], format!("message type {t}")).await,
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

/// The 16-bit AS for a 2-octet field: the ASN, or AS_TRANS above 65535.
fn as2(asn: u32) -> u16 {
    u16::try_from(asn).unwrap_or(AS_TRANS as u16)
}

/// A Multiprotocol capability (code, length, AFI, reserved, SAFI).
fn mp_capability((afi, safi): Family) -> [u8; 6] {
    let a = afi.to_be_bytes();
    [CAP_MULTIPROTOCOL, 4, a[0], a[1], 0, safi]
}

/// OPEN with the Multiprotocol capability for `family` and the 4-octet AS
/// capability carrying `local_asn` (My AS is AS_TRANS when it is > 65535).
fn open_msg(local_asn: u32, hold: u16, bgp_id: Ipv4Addr, family: Family) -> Vec<u8> {
    let mut caps = mp_capability(family).to_vec();
    caps.extend_from_slice(&[CAP_AS4, 4]);
    caps.extend_from_slice(&local_asn.to_be_bytes());
    let mut body = vec![4]; // BGP version
    body.extend_from_slice(&as2(local_asn).to_be_bytes());
    body.extend_from_slice(&hold.to_be_bytes());
    body.extend_from_slice(&bgp_id.octets());
    body.push(2 + caps.len() as u8); // optional parameters length
    body.extend_from_slice(&[OPT_CAPABILITIES, caps.len() as u8]);
    body.extend_from_slice(&caps);
    message(OPEN, &body)
}

fn keepalive_msg() -> Vec<u8> {
    message(KEEPALIVE, &[])
}

fn notification_msg(code: u8, sub: u8, data: &[u8]) -> Vec<u8> {
    let mut body = vec![code, sub];
    body.extend_from_slice(data);
    message(NOTIFICATION, &body)
}

/// A path attribute (flags, type, 1-byte length, value; every one we send
/// is under 256 bytes).
fn attr(flags: u8, t: u8, value: &[u8]) -> Vec<u8> {
    let mut v = vec![flags, t, value.len() as u8];
    v.extend_from_slice(value);
    v
}

/// The prefix length of an address's host route.
fn host_len(ip: IpAddr) -> u8 {
    if ip.is_ipv4() {
        32
    } else {
        128
    }
}

/// The host route of `ip` in BGP prefix encoding (`<len>` + the address).
fn host_prefix(ip: IpAddr) -> Vec<u8> {
    let mut v = vec![host_len(ip)];
    match ip {
        IpAddr::V4(a) => v.extend_from_slice(&a.octets()),
        IpAddr::V6(a) => v.extend_from_slice(&a.octets()),
    }
    v
}

/// AS_PATH (and AS4_PATH) for a path of our own AS alone. A 4-octet peer gets
/// 4-byte ASNs; an old one 2-byte ones, with AS_TRANS and an AS4_PATH holding
/// the real ASN when it is above 65535 (RFC 6793 §4.2.2).
fn as_path(local_asn: u32, as4: bool) -> Vec<u8> {
    let seg4 = {
        let mut s = vec![AS_SEQUENCE, 1];
        s.extend_from_slice(&local_asn.to_be_bytes());
        s
    };
    if as4 {
        return attr(FLAG_TRANSITIVE, ATTR_AS_PATH, &seg4);
    }
    let mut seg2 = vec![AS_SEQUENCE, 1];
    seg2.extend_from_slice(&as2(local_asn).to_be_bytes());
    let mut v = attr(FLAG_TRANSITIVE, ATTR_AS_PATH, &seg2);
    if local_asn > u16::MAX as u32 {
        v.extend(attr(FLAG_OPTIONAL | FLAG_TRANSITIVE, ATTR_AS4_PATH, &seg4));
    }
    v
}

/// UPDATE announcing `vip`'s host route with ORIGIN=IGP and AS_PATH=[local].
/// IPv4: NEXT_HOP and the classic NLRI. IPv6: MP_REACH_NLRI (AFI 2, SAFI 1,
/// the 16-byte next-hop). `next_hop` is in the VIP's family ([`preflight`]).
fn update_announce(local_asn: u32, as4: bool, next_hop: IpAddr, vip: IpAddr) -> Vec<u8> {
    let mut attrs = attr(FLAG_TRANSITIVE, ATTR_ORIGIN, &[0]); // IGP
    attrs.extend(as_path(local_asn, as4));
    let mut nlri = Vec::new();
    match (vip, next_hop) {
        (IpAddr::V4(_), IpAddr::V4(nh)) => {
            attrs.extend(attr(FLAG_TRANSITIVE, ATTR_NEXT_HOP, &nh.octets()));
            nlri = host_prefix(vip);
        }
        (_, nh) => {
            let (afi, safi) = family_of(vip);
            let mut v = afi.to_be_bytes().to_vec();
            v.push(safi);
            let nh = match nh {
                IpAddr::V6(a) => a.octets().to_vec(),
                IpAddr::V4(a) => a.octets().to_vec(), // not reached: preflight
            };
            v.push(nh.len() as u8);
            v.extend_from_slice(&nh);
            v.push(0); // reserved
            v.extend(host_prefix(vip));
            attrs.extend(attr(FLAG_OPTIONAL, ATTR_MP_REACH, &v));
        }
    }
    let mut body = 0u16.to_be_bytes().to_vec(); // withdrawn routes length = 0
    body.extend_from_slice(&(attrs.len() as u16).to_be_bytes());
    body.extend_from_slice(&attrs);
    body.extend_from_slice(&nlri);
    message(UPDATE, &body)
}

/// UPDATE withdrawing `vip`'s host route: the classic withdrawn routes for
/// IPv4, MP_UNREACH_NLRI for IPv6.
fn update_withdraw(vip: IpAddr) -> Vec<u8> {
    let mut body = Vec::new();
    match vip {
        IpAddr::V4(_) => {
            let withdrawn = host_prefix(vip);
            body.extend_from_slice(&(withdrawn.len() as u16).to_be_bytes());
            body.extend_from_slice(&withdrawn);
            body.extend_from_slice(&0u16.to_be_bytes()); // no path attributes
        }
        IpAddr::V6(_) => {
            let (afi, safi) = family_of(vip);
            let mut v = afi.to_be_bytes().to_vec();
            v.push(safi);
            v.extend(host_prefix(vip));
            let attrs = attr(FLAG_OPTIONAL, ATTR_MP_UNREACH, &v);
            body.extend_from_slice(&0u16.to_be_bytes()); // no classic withdrawn routes
            body.extend_from_slice(&(attrs.len() as u16).to_be_bytes());
            body.extend_from_slice(&attrs);
        }
    }
    message(UPDATE, &body)
}

/// An ASN we can use or expect: 1..=4294967294, not AS_TRANS (RFC 6793
/// reserves it as a stand-in; 0 and 4294967295 are reserved, RFC 7607/7300).
fn check_asn(what: &str, asn: u32) -> Result<()> {
    if asn == 0 || asn == AS_TRANS || asn == u32::MAX {
        anyhow::bail!("{what} {asn} is reserved (1..4294967294, not 23456)");
    }
    Ok(())
}

/// Validate config for announcing `vip`; returns the next-hop advertised for
/// it: `next_hop`, else `router_id`, in the VIP's family.
pub fn preflight(cfg: &BgpCfg, vip: IpAddr) -> Result<IpAddr> {
    check_asn("bgp.local_asn", cfg.local_asn)?;
    if cfg.peers.is_empty() {
        anyhow::bail!("bgp.enabled but no peers configured");
    }
    for p in &cfg.peers {
        p.address
            .parse::<IpAddr>()
            .map_err(|_| anyhow::anyhow!("bgp peer address {:?} is not an IP address", p.address))?;
        check_asn(&format!("bgp peer {} asn", p.address), p.asn)?;
    }
    let id: Ipv4Addr = cfg
        .router_id
        .parse()
        .map_err(|_| anyhow::anyhow!("bgp.router_id must be a dotted quad (the BGP identifier)"))?;
    if id.is_unspecified() {
        anyhow::bail!("bgp.router_id must not be 0.0.0.0");
    }
    let next_hop: IpAddr = match &cfg.next_hop {
        Some(n) => n.parse().map_err(|_| anyhow::anyhow!("bgp.next_hop {n:?} is not an IP address"))?,
        None => IpAddr::V4(id),
    };
    match (vip, next_hop) {
        (IpAddr::V4(_), IpAddr::V4(_)) => Ok(next_hop),
        (IpAddr::V6(_), IpAddr::V6(a)) if a.segments()[0] & 0xffc0 == 0xfe80 || a.is_unspecified() => {
            anyhow::bail!("bgp.next_hop {a} must be this node's global IPv6, not link-local")
        }
        (IpAddr::V6(_), IpAddr::V6(_)) => Ok(next_hop),
        (IpAddr::V6(_), _) => anyhow::bail!("an IPv6 VIP needs bgp.next_hop set to this node's IPv6"),
        (IpAddr::V4(_), _) => anyhow::bail!("an IPv4 VIP needs an IPv4 bgp.next_hop"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BgpPeer;
    use std::net::Ipv6Addr;

    const V4: Family = (AFI_IPV4, SAFI_UNICAST);
    const V6: Family = (AFI_IPV6, SAFI_UNICAST);

    #[test]
    fn header_framing_is_correct() {
        let m = keepalive_msg();
        assert_eq!(m.len(), 19);
        assert_eq!(&m[0..16], &[0xff; 16]);
        assert_eq!(u16::from_be_bytes([m[16], m[17]]), 19);
        assert_eq!(m[18], KEEPALIVE);
    }

    #[test]
    fn open_has_version_asn_hold_id_and_capabilities() {
        let m = open_msg(64512, 180, Ipv4Addr::new(192, 168, 8, 51), V4);
        assert_eq!(m[18], OPEN);
        let body = &m[19..];
        assert_eq!(body[0], 4); // version
        assert_eq!(u16::from_be_bytes([body[1], body[2]]), 64512);
        assert_eq!(u16::from_be_bytes([body[3], body[4]]), 180);
        assert_eq!(&body[5..9], &[192, 168, 8, 51]);
        assert_eq!(
            &body[9..],
            &[14, OPT_CAPABILITIES, 12, CAP_MULTIPROTOCOL, 4, 0, 1, 0, 1, CAP_AS4, 4, 0, 0, 0xfc, 0x00]
        );
        // Our own OPEN reads back as what it offers.
        let o = check_open(body, 64512, V4).unwrap();
        assert_eq!(o, PeerOpen { hold: 180, as4: true, asn: 64512 });
    }

    #[test]
    fn a_4_octet_asn_goes_as_as_trans_and_the_capability() {
        let m = open_msg(4_200_000_000, 90, Ipv4Addr::new(10, 0, 0, 1), V6);
        let body = &m[19..];
        assert_eq!(u16::from_be_bytes([body[1], body[2]]), 23456);
        let caps = &body[12..];
        assert_eq!(caps, [&mp_capability(V6)[..], &[CAP_AS4, 4], &4_200_000_000u32.to_be_bytes()].concat());
        assert_eq!(check_open(body, 4_200_000_000, V6).unwrap().asn, 4_200_000_000);
    }

    /// A peer OPEN: My AS, hold, then optional parameters as given.
    fn peer_open(my_as: u16, hold: u16, params: &[u8]) -> Vec<u8> {
        let mut b = vec![4];
        b.extend_from_slice(&my_as.to_be_bytes());
        b.extend_from_slice(&hold.to_be_bytes());
        b.extend_from_slice(&[10, 0, 0, 2, params.len() as u8]);
        b.extend_from_slice(params);
        b
    }

    fn caps_param(caps: &[&[u8]]) -> Vec<u8> {
        let c = caps.concat();
        [&[OPT_CAPABILITIES, c.len() as u8][..], &c].concat()
    }

    fn as4_cap(asn: u32) -> Vec<u8> {
        [&[CAP_AS4, 4][..], &asn.to_be_bytes()].concat()
    }

    #[test]
    fn the_peers_open_is_checked() {
        let plain = |ver: u8, asn: u16, hold: u16| {
            let mut b = peer_open(asn, hold, &[]);
            b[0] = ver;
            b
        };
        assert_eq!(check_open(&plain(4, 64512, 90), 64512, V4), Ok(PeerOpen { hold: 90, as4: false, asn: 64512 }));
        assert_eq!(check_open(&plain(4, 64512, 0), 64512, V4).unwrap().hold, 0, "0 = no keepalives");
        assert_eq!(check_open(&plain(3, 64512, 90), 64512, V4).unwrap_err().sub, ERR_OPEN_VERSION);
        assert_eq!(check_open(&plain(4, 64513, 90), 64512, V4).unwrap_err().sub, ERR_OPEN_PEER_AS);
        assert_eq!(check_open(&plain(4, 64512, 2), 64512, V4).unwrap_err().sub, ERR_OPEN_HOLD);
        assert!(check_open(&[4, 0], 64512, V4).is_err());
        // Optional parameters length past the body.
        let mut bad = plain(4, 64512, 90);
        bad[9] = 4;
        assert_eq!(check_open(&bad, 64512, V4).unwrap_err().sub, 0);
        assert_eq!(notification_msg(4, 0, &[])[18..], [NOTIFICATION, 4, 0]);
    }

    #[test]
    fn a_4_octet_peer_is_identified_by_its_capability() {
        let p = caps_param(&[&mp_capability(V4), &as4_cap(4_200_000_001)]);
        let body = peer_open(23456, 90, &p);
        assert_eq!(check_open(&body, 4_200_000_001, V4), Ok(PeerOpen { hold: 90, as4: true, asn: 4_200_000_001 }));
        // Configured for a 4-octet ASN, but the peer is an old speaker.
        let old = peer_open(23456, 90, &[]);
        let r = check_open(&old, 4_200_000_001, V4).unwrap_err();
        assert_eq!(r.sub, ERR_OPEN_PEER_AS);
        assert!(r.why.contains("without the 4-octet AS capability"), "{}", r.why);
        // The capability's ASN wins over My AS.
        let body = peer_open(65001, 90, &caps_param(&[&as4_cap(65002)]));
        assert_eq!(check_open(&body, 65001, V4).unwrap_err().sub, ERR_OPEN_PEER_AS);
    }

    #[test]
    fn the_vips_family_must_be_negotiated() {
        // No Multiprotocol capability: IPv4 unicast only.
        assert!(check_open(&peer_open(65001, 90, &[]), 65001, V4).is_ok());
        let r = check_open(&peer_open(65001, 90, &[]), 65001, V6).unwrap_err();
        assert_eq!((r.code, r.sub, r.data), (ERR_OPEN, ERR_OPEN_UNSUPPORTED_CAP, mp_capability(V6).to_vec()));
        // IPv6 offered: fine for an IPv6 VIP, and IPv4 is then not implied.
        let v6 = caps_param(&[&mp_capability(V6)]);
        assert!(check_open(&peer_open(65001, 90, &v6), 65001, V6).is_ok());
        assert_eq!(check_open(&peer_open(65001, 90, &v6), 65001, V4).unwrap_err().sub, ERR_OPEN_UNSUPPORTED_CAP);
        // Both, in two Capabilities parameters, with an unknown one between.
        let both = [caps_param(&[&mp_capability(V4)]), caps_param(&[&[70, 0]]), caps_param(&[&mp_capability(V6)])].concat();
        assert!(check_open(&peer_open(65001, 90, &both), 65001, V6).is_ok());
    }

    #[test]
    fn extended_optional_parameters_are_read() {
        // RFC 9072: length 255, type 255, 2-byte total, 2-byte param lengths.
        let caps = [&mp_capability(V6)[..], &as4_cap(4_200_000_001)].concat();
        let mut params = vec![255, 255];
        params.extend_from_slice(&((3 + caps.len()) as u16).to_be_bytes());
        params.push(OPT_CAPABILITIES);
        params.extend_from_slice(&(caps.len() as u16).to_be_bytes());
        params.extend_from_slice(&caps);
        let mut body = peer_open(23456, 90, &[]);
        body.truncate(9);
        body.extend_from_slice(&params);
        assert_eq!(check_open(&body, 4_200_000_001, V6).unwrap().asn, 4_200_000_001);
    }

    /// A path attribute as (flags, type, value).
    type Attr = (u8, u8, Vec<u8>);

    /// An UPDATE body's (withdrawn, attributes, NLRI).
    fn parse_update(m: &[u8]) -> (Vec<u8>, Vec<Attr>, Vec<u8>) {
        let body = &m[19..];
        let wl = u16::from_be_bytes([body[0], body[1]]) as usize;
        let withdrawn = body[2..2 + wl].to_vec();
        let al = u16::from_be_bytes([body[2 + wl], body[3 + wl]]) as usize;
        let mut a = &body[4 + wl..4 + wl + al];
        let mut attrs = Vec::new();
        while !a.is_empty() {
            let len = a[2] as usize;
            attrs.push((a[0], a[1], a[3..3 + len].to_vec()));
            a = &a[3 + len..];
        }
        (withdrawn, attrs, body[4 + wl + al..].to_vec())
    }

    fn find(attrs: &[Attr], t: u8) -> Option<&Vec<u8>> {
        attrs.iter().find(|a| a.1 == t).map(|a| &a.2)
    }

    #[test]
    fn announce_carries_attrs_and_nlri() {
        let m = update_announce(64512, false, "192.168.8.51".parse().unwrap(), "192.168.8.50".parse().unwrap());
        assert_eq!(m[18], UPDATE);
        let (w, attrs, nlri) = parse_update(&m);
        assert!(w.is_empty());
        assert_eq!(nlri, [32, 192, 168, 8, 50]); // /32 prefix
        assert_eq!(find(&attrs, ATTR_ORIGIN).unwrap(), &[0]);
        assert_eq!(find(&attrs, ATTR_AS_PATH).unwrap(), &[2, 1, 0xfc, 0x00]);
        assert_eq!(find(&attrs, ATTR_NEXT_HOP).unwrap(), &[192, 168, 8, 51]);
        assert!(find(&attrs, ATTR_AS4_PATH).is_none());
    }

    #[test]
    fn as_path_follows_the_peers_as_size() {
        let nh: IpAddr = "10.0.0.1".parse().unwrap();
        let vip: IpAddr = "10.0.0.100".parse().unwrap();
        let big = 4_200_000_000u32;
        // 4-octet peer: 4-byte AS_PATH, no AS4_PATH.
        let (_, a, _) = parse_update(&update_announce(big, true, nh, vip));
        assert_eq!(find(&a, ATTR_AS_PATH).unwrap(), &[&[2, 1][..], &big.to_be_bytes()].concat());
        assert!(find(&a, ATTR_AS4_PATH).is_none());
        let (_, a, _) = parse_update(&update_announce(64512, true, nh, vip));
        assert_eq!(find(&a, ATTR_AS_PATH).unwrap(), &[2, 1, 0, 0, 0xfc, 0x00]);
        // Old peer, big ASN: AS_TRANS in AS_PATH, the real one in AS4_PATH
        // (optional transitive).
        let (_, a, _) = parse_update(&update_announce(big, false, nh, vip));
        assert_eq!(find(&a, ATTR_AS_PATH).unwrap(), &[2, 1, 0x5b, 0xa0]);
        let as4 = a.iter().find(|x| x.1 == ATTR_AS4_PATH).unwrap();
        assert_eq!(as4.0, FLAG_OPTIONAL | FLAG_TRANSITIVE);
        assert_eq!(as4.2, [&[2, 1][..], &big.to_be_bytes()].concat());
    }

    #[test]
    fn an_ipv6_vip_goes_in_mp_reach_and_mp_unreach() {
        let nh: Ipv6Addr = "fd09::1".parse().unwrap();
        let vip: Ipv6Addr = "fd09::100".parse().unwrap();
        let (w, a, nlri) = parse_update(&update_announce(64512, true, nh.into(), vip.into()));
        assert!(w.is_empty() && nlri.is_empty(), "nothing IPv6 in the classic fields");
        assert!(find(&a, ATTR_NEXT_HOP).is_none());
        let mp = a.iter().find(|x| x.1 == ATTR_MP_REACH).unwrap();
        assert_eq!(mp.0, FLAG_OPTIONAL);
        let want = [&[0, 2, 1, 16][..], &nh.octets(), &[0, 128], &vip.octets()].concat();
        assert_eq!(mp.2, want);

        let (w, a, nlri) = parse_update(&update_withdraw(vip.into()));
        assert!(w.is_empty() && nlri.is_empty());
        let un = a.iter().find(|x| x.1 == ATTR_MP_UNREACH).unwrap();
        assert_eq!(un.0, FLAG_OPTIONAL);
        assert_eq!(un.2, [&[0, 2, 1, 128][..], &vip.octets()].concat());
    }

    #[test]
    fn withdraw_lists_the_prefix() {
        let (w, a, nlri) = parse_update(&update_withdraw("192.168.8.50".parse().unwrap()));
        assert_eq!(w, [32, 192, 168, 8, 50]);
        assert!(a.is_empty() && nlri.is_empty());
    }

    fn cfg(local: u32, router_id: &str, next_hop: Option<&str>, peer: &str, peer_asn: u32) -> BgpCfg {
        BgpCfg {
            enabled: true,
            local_asn: local,
            router_id: router_id.into(),
            next_hop: next_hop.map(Into::into),
            peers: vec![BgpPeer { address: peer.into(), asn: peer_asn, port: 179 }],
        }
    }

    #[test]
    fn preflight_checks_asns_addresses_and_the_next_hop_family() {
        let v4: IpAddr = "10.0.0.100".parse().unwrap();
        let v6: IpAddr = "fd09::100".parse().unwrap();
        let ok = |c: BgpCfg, vip| preflight(&c, vip);
        assert_eq!(ok(cfg(64512, "10.0.0.1", None, "10.0.0.2", 65001), v4).unwrap(), "10.0.0.1".parse::<IpAddr>().unwrap());
        assert!(ok(cfg(4_200_000_000, "10.0.0.1", None, "10.0.0.2", 4_200_000_001), v4).is_ok());
        assert!(ok(cfg(64512, "10.0.0.1", None, "fd09::2", 65001), v4).is_ok(), "IPv6 transport");
        for (local, peer) in [(0, 65001), (23456, 65001), (u32::MAX, 65001), (64512, 0), (64512, 23456)] {
            assert!(ok(cfg(local, "10.0.0.1", None, "10.0.0.2", peer), v4).is_err(), "{local}/{peer}");
        }
        assert!(ok(cfg(64512, "fd09::1", None, "10.0.0.2", 65001), v4).is_err(), "router_id is a dotted quad");
        assert!(ok(cfg(64512, "10.0.0.1", None, "peer", 65001), v4).is_err());
        // IPv6 VIP: next_hop must be set, IPv6, and not link-local.
        assert!(ok(cfg(64512, "10.0.0.1", None, "fd09::2", 65001), v6).is_err());
        assert!(ok(cfg(64512, "10.0.0.1", Some("fe80::1"), "fd09::2", 65001), v6).is_err());
        assert_eq!(
            ok(cfg(64512, "10.0.0.1", Some("fd09::1"), "fd09::2", 65001), v6).unwrap(),
            "fd09::1".parse::<IpAddr>().unwrap()
        );
        assert!(ok(cfg(64512, "10.0.0.1", Some("fd09::1"), "10.0.0.2", 65001), v4).is_err());
    }
}
