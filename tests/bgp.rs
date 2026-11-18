//! Integration tests: the BGP speaker against a fake peer (#6). The peer
//! listens on a free port, records every message stormlb sends with when it
//! arrived, and plays its side of the session as each test needs: an old
//! 2-octet IPv4-only speaker, or one offering the 4-octet AS and
//! Multiprotocol capabilities (#40).

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::time::Duration;
use stormlb::bgp;
use stormlb::config::{BgpCfg, BgpPeer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch};
use tokio::time::Instant;

const OPEN: u8 = 1;
const UPDATE: u8 = 2;
const NOTIFICATION: u8 = 3;
const KEEPALIVE: u8 = 4;
const VIP: Ipv4Addr = Ipv4Addr::new(10, 9, 0, 100);

fn msg(t: u8, body: &[u8]) -> Vec<u8> {
    let mut m = vec![0xff; 16];
    m.extend_from_slice(&((19 + body.len()) as u16).to_be_bytes());
    m.push(t);
    m.extend_from_slice(body);
    m
}

/// The peer's OPEN: My AS (AS_TRANS when a 4-octet ASN doesn't fit), and
/// with `as4` the 4-octet AS capability and Multiprotocol for each AFI.
fn open(play: &Play) -> Vec<u8> {
    let mut caps = Vec::new();
    if play.as4 {
        caps.extend_from_slice(&[65, 4]);
        caps.extend_from_slice(&play.asn.to_be_bytes());
    }
    for afi in play.afis {
        caps.extend_from_slice(&[1, 4, 0, *afi as u8, 0, 1]);
    }
    let mut b = vec![4];
    b.extend_from_slice(&u16::try_from(play.asn).unwrap_or(23456).to_be_bytes());
    b.extend_from_slice(&play.hold.to_be_bytes());
    b.extend_from_slice(&[10, 0, 0, 2]);
    if caps.is_empty() {
        b.push(0);
    } else {
        b.extend_from_slice(&[2 + caps.len() as u8, 2, caps.len() as u8]);
        b.extend_from_slice(&caps);
    }
    msg(OPEN, &b)
}

/// How the fake peer behaves.
#[derive(Clone, Copy)]
struct Play {
    asn: u32,
    hold: u16,
    /// Offer the 4-octet AS capability.
    as4: bool,
    /// Multiprotocol capabilities offered (AFIs, SAFI 1); none = IPv4 only.
    afis: &'static [u16],
    /// Wait this long after stormlb's OPEN before answering.
    delay_open: Duration,
    /// Keep sending KEEPALIVEs (every hold/3); false: silent after the first.
    keepalives: bool,
}

/// What the peer saw: (type, body, when), and when it sent its KEEPALIVE.
enum Seen {
    Msg(u8, Vec<u8>, Instant),
    SentKeepalive(Instant),
    Closed,
}

async fn fake_peer(play: Play) -> (u16, mpsc::UnboundedReceiver<Seen>) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let (s, _) = l.accept().await.unwrap();
        let (mut r, mut w) = s.into_split();
        let txr = tx.clone();
        let reader = tokio::spawn(async move {
            loop {
                let mut hdr = [0u8; 19];
                if r.read_exact(&mut hdr).await.is_err() {
                    let _ = txr.send(Seen::Closed);
                    return;
                }
                let len = u16::from_be_bytes([hdr[16], hdr[17]]) as usize;
                let mut body = vec![0u8; len - 19];
                if r.read_exact(&mut body).await.is_err() {
                    let _ = txr.send(Seen::Closed);
                    return;
                }
                let _ = txr.send(Seen::Msg(hdr[18], body, Instant::now()));
            }
        });
        tokio::time::sleep(play.delay_open).await;
        // Stamped before the OPEN goes: stormlb can't be Established earlier.
        let at = Instant::now();
        let _ = w.write_all(&open(&play)).await;
        let _ = w.write_all(&msg(KEEPALIVE, &[])).await;
        let _ = tx.send(Seen::SentKeepalive(at));
        if play.keepalives {
            loop {
                tokio::time::sleep(Duration::from_secs((play.hold / 3).max(1) as u64)).await;
                if w.write_all(&msg(KEEPALIVE, &[])).await.is_err() {
                    break;
                }
            }
        }
        let _ = reader.await;
    });
    (port, rx)
}

fn speaker(port: u16, peer_asn: u32) -> (watch::Sender<bool>, BgpCfg) {
    let (tx, _) = watch::channel(false);
    let cfg = BgpCfg {
        enabled: true,
        local_asn: 64512,
        router_id: "10.0.0.1".into(),
        next_hop: None,
        peers: vec![BgpPeer { address: "127.0.0.1".into(), asn: peer_asn, port }],
    };
    (tx, cfg)
}

/// The next message of type `t` (others are skipped), or None by `secs`.
async fn next_of(rx: &mut mpsc::UnboundedReceiver<Seen>, t: u8, secs: u64) -> Option<(Vec<u8>, Instant)> {
    let end = Instant::now() + Duration::from_secs(secs);
    loop {
        match tokio::time::timeout_at(end, rx.recv()).await {
            Ok(Some(Seen::Msg(ty, body, at))) if ty == t => return Some((body, at)),
            Ok(Some(_)) => continue,
            _ => return None,
        }
    }
}

/// An UPDATE's withdrawn-routes and NLRI parts.
fn update_parts(body: &[u8]) -> (Vec<u8>, Vec<u8>) {
    let wl = u16::from_be_bytes([body[0], body[1]]) as usize;
    let withdrawn = body[2..2 + wl].to_vec();
    let al = u16::from_be_bytes([body[2 + wl], body[3 + wl]]) as usize;
    let nlri = body[4 + wl + al..].to_vec();
    (withdrawn, nlri)
}

#[tokio::test]
async fn announces_only_when_established_then_follows_health_at_once() {
    let play = Play { asn: 65001, hold: 9, delay_open: Duration::from_millis(1500), keepalives: true, as4: false, afis: &[] };
    let (port, mut rx) = fake_peer(play).await;
    let (tx, cfg) = speaker(port, 65001);
    tx.send_replace(true); // healthy from the start
    bgp::spawn(&cfg, VIP.into(), Ipv4Addr::new(10, 0, 0, 1).into(), tx.subscribe());

    // Our OPEN first, carrying our AS and hold time, and the capabilities
    // (Multiprotocol IPv4 unicast, 4-octet AS 64512) an old peer ignores.
    let (o, opened) = next_of(&mut rx, OPEN, 5).await.expect("stormlb sends OPEN");
    assert_eq!(u16::from_be_bytes([o[1], o[2]]), 64512);
    assert_eq!(u16::from_be_bytes([o[3], o[4]]), 180);
    assert_eq!(o[9..], [14, 2, 12, 1, 4, 0, 1, 0, 1, 65, 4, 0, 0, 0xfc, 0x00]);

    // Nothing is announced before the peer's OPEN and KEEPALIVE (1.5 s
    // later); then the announce follows at once.
    // Both events, in whichever order the two tasks report them.
    let (mut established, mut first) = (None, None);
    while established.is_none() || first.is_none() {
        match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.expect("progress").unwrap() {
            Seen::SentKeepalive(at) => established = Some(at),
            Seen::Msg(UPDATE, body, at) if first.is_none() => first = Some((body, at)),
            Seen::Msg(..) => {}
            Seen::Closed => panic!("session closed"),
        }
    }
    let (established, first_update) = (established.unwrap(), first.unwrap());
    assert!(first_update.1 >= established);
    assert!(first_update.1 - established < Duration::from_millis(500), "announced {:?} after Established", first_update.1 - established);
    assert!(first_update.1 - opened >= Duration::from_millis(1400));
    assert_eq!(update_parts(&first_update.0).1, vec![32, 10, 9, 0, 100], "announces the VIP /32");

    // Health lost: withdrawn within a second (it was up to 60 s, #6).
    let t = Instant::now();
    tx.send_replace(false);
    let (w, at) = next_of(&mut rx, UPDATE, 3).await.expect("a withdraw");
    assert_eq!(update_parts(&w).0, vec![32, 10, 9, 0, 100]);
    assert!(at - t < Duration::from_secs(1), "withdrew after {:?}", at - t);

    // Health back: announced again at once.
    let t = Instant::now();
    tx.send_replace(true);
    let (a, at) = next_of(&mut rx, UPDATE, 3).await.expect("a re-announce");
    assert_eq!(update_parts(&a).1, vec![32, 10, 9, 0, 100]);
    assert!(at - t < Duration::from_secs(1));
}

#[tokio::test]
async fn a_silent_peer_is_dropped_at_the_hold_time() {
    // Hold 3 s: the smaller of ours (180) and the peer's.
    let play = Play { asn: 65001, hold: 3, delay_open: Duration::ZERO, keepalives: false, as4: false, afis: &[] };
    let (port, mut rx) = fake_peer(play).await;
    let (tx, cfg) = speaker(port, 65001);
    bgp::spawn(&cfg, VIP.into(), Ipv4Addr::new(10, 0, 0, 1).into(), tx.subscribe());
    let mut established = None;
    let mut keepalives = 0;
    let (body, at) = loop {
        match tokio::time::timeout(Duration::from_secs(8), rx.recv()).await.expect("progress").unwrap() {
            Seen::SentKeepalive(at) => established = Some(at),
            Seen::Msg(KEEPALIVE, _, _) => keepalives += 1,
            Seen::Msg(NOTIFICATION, body, at) => break (body, at),
            Seen::Msg(..) => {}
            Seen::Closed => panic!("closed without a NOTIFICATION"),
        }
    };
    assert_eq!(body[..2], [4, 0], "Hold Timer Expired");
    let after = at - established.unwrap();
    assert!(after >= Duration::from_millis(2800) && after < Duration::from_millis(4500), "dropped after {after:?}");
    // Its own keepalives went every hold/3 (1 s), plus the one after OPEN.
    assert!(keepalives >= 3, "{keepalives} keepalives");
}

#[tokio::test]
async fn a_peer_with_the_wrong_as_is_refused() {
    let play = Play { asn: 65002, hold: 90, delay_open: Duration::ZERO, keepalives: true, as4: false, afis: &[] };
    let (port, mut rx) = fake_peer(play).await;
    let (tx, cfg) = speaker(port, 65001);
    tx.send_replace(true);
    bgp::spawn(&cfg, VIP.into(), Ipv4Addr::new(10, 0, 0, 1).into(), tx.subscribe());
    let (body, _) = next_of(&mut rx, NOTIFICATION, 5).await.expect("a NOTIFICATION");
    assert_eq!(body[..2], [2, 2], "OPEN Message Error / Bad Peer AS");
    // And no route was ever announced on that session.
    assert!(next_of(&mut rx, UPDATE, 1).await.is_none());
}

/// A path attribute as (flags, type, value).
type Attr = (u8, u8, Vec<u8>);

/// An UPDATE's path attributes.
fn update_attrs(body: &[u8]) -> Vec<Attr> {
    let wl = u16::from_be_bytes([body[0], body[1]]) as usize;
    let al = u16::from_be_bytes([body[2 + wl], body[3 + wl]]) as usize;
    let mut a = &body[4 + wl..4 + wl + al];
    let mut out = Vec::new();
    while !a.is_empty() {
        let len = a[2] as usize;
        out.push((a[0], a[1], a[3..3 + len].to_vec()));
        a = &a[3 + len..];
    }
    out
}

fn attr_value(attrs: &[Attr], t: u8) -> Option<Vec<u8>> {
    attrs.iter().find(|a| a.1 == t).map(|a| a.2.clone())
}

#[tokio::test]
async fn four_octet_asns_both_sides() {
    let (local, remote) = (4_200_000_000u32, 4_200_000_001u32);
    let play = Play { asn: remote, hold: 9, delay_open: Duration::ZERO, keepalives: true, as4: true, afis: &[1] };
    let (port, mut rx) = fake_peer(play).await;
    let (tx, mut cfg) = speaker(port, remote);
    cfg.local_asn = local;
    tx.send_replace(true);
    bgp::spawn(&cfg, VIP.into(), Ipv4Addr::new(10, 0, 0, 1).into(), tx.subscribe());

    // My AS is AS_TRANS; the capability carries the real ASN.
    let (o, _) = next_of(&mut rx, OPEN, 5).await.expect("stormlb sends OPEN");
    assert_eq!(u16::from_be_bytes([o[1], o[2]]), 23456);
    assert!(o[10..].windows(6).any(|w| w == [&[65, 4][..], &local.to_be_bytes()].concat()));

    // The peer is accepted by its 4-octet ASN, and gets a 4-byte AS_PATH.
    let (u, _) = next_of(&mut rx, UPDATE, 5).await.expect("an announce");
    let attrs = update_attrs(&u);
    assert_eq!(attr_value(&attrs, 2).unwrap(), [&[2, 1][..], &local.to_be_bytes()].concat());
    assert!(attr_value(&attrs, 17).is_none(), "no AS4_PATH to a 4-octet peer");
    assert_eq!(update_parts(&u).1, vec![32, 10, 9, 0, 100]);
}

#[tokio::test]
async fn a_4_octet_asn_to_an_old_peer_goes_as_as_trans_and_as4_path() {
    let local = 4_200_000_000u32;
    let play = Play { asn: 65001, hold: 9, delay_open: Duration::ZERO, keepalives: true, as4: false, afis: &[] };
    let (port, mut rx) = fake_peer(play).await;
    let (tx, mut cfg) = speaker(port, 65001);
    cfg.local_asn = local;
    tx.send_replace(true);
    bgp::spawn(&cfg, VIP.into(), Ipv4Addr::new(10, 0, 0, 1).into(), tx.subscribe());
    let (u, _) = next_of(&mut rx, UPDATE, 5).await.expect("an announce");
    let attrs = update_attrs(&u);
    assert_eq!(attr_value(&attrs, 2).unwrap(), [2, 1, 0x5b, 0xa0], "AS_TRANS");
    assert_eq!(attr_value(&attrs, 17).unwrap(), [&[2, 1][..], &local.to_be_bytes()].concat());
}

#[tokio::test]
async fn an_ipv6_vip_is_announced_and_withdrawn_by_mp_bgp() {
    let vip: Ipv6Addr = "fd09::100".parse().unwrap();
    let nh: Ipv6Addr = "fd09::1".parse().unwrap();
    let play = Play { asn: 65001, hold: 9, delay_open: Duration::ZERO, keepalives: true, as4: true, afis: &[2] };
    let (port, mut rx) = fake_peer(play).await;
    let (tx, mut cfg) = speaker(port, 65001);
    cfg.next_hop = Some(nh.to_string());
    let next_hop = bgp::preflight(&cfg, vip.into()).expect("an IPv6 VIP with an IPv6 next_hop");
    assert_eq!(next_hop, IpAddr::V6(nh));
    tx.send_replace(true);
    bgp::spawn(&cfg, vip.into(), next_hop, tx.subscribe());

    // OPEN offers Multiprotocol AFI 2 / SAFI 1.
    let (o, _) = next_of(&mut rx, OPEN, 5).await.expect("stormlb sends OPEN");
    assert!(o[10..].windows(6).any(|w| w == [1, 4, 0, 2, 0, 1]));

    // Announce: MP_REACH_NLRI (optional, non-transitive), nothing classic.
    let (u, _) = next_of(&mut rx, UPDATE, 5).await.expect("an announce");
    let (withdrawn, nlri) = update_parts(&u);
    assert!(withdrawn.is_empty() && nlri.is_empty());
    let attrs = update_attrs(&u);
    assert!(attr_value(&attrs, 3).is_none(), "no IPv4 NEXT_HOP");
    let reach = attrs.iter().find(|a| a.1 == 14).expect("MP_REACH_NLRI");
    assert_eq!(reach.0, 0x80);
    assert_eq!(reach.2, [&[0, 2, 1, 16][..], &nh.octets(), &[0, 128], &vip.octets()].concat());

    // Withdraw: MP_UNREACH_NLRI, within a second.
    let t = Instant::now();
    tx.send_replace(false);
    let (u, at) = next_of(&mut rx, UPDATE, 3).await.expect("a withdraw");
    assert!(at - t < Duration::from_secs(1));
    let unreach = attr_value(&update_attrs(&u), 15).expect("MP_UNREACH_NLRI");
    assert_eq!(unreach, [&[0, 2, 1, 128][..], &vip.octets()].concat());
}

#[tokio::test]
async fn an_ipv6_vip_to_a_peer_without_ipv6_unicast_is_refused() {
    let play = Play { asn: 65001, hold: 90, delay_open: Duration::ZERO, keepalives: true, as4: true, afis: &[] };
    let (port, mut rx) = fake_peer(play).await;
    let (tx, mut cfg) = speaker(port, 65001);
    cfg.next_hop = Some("fd09::1".into());
    tx.send_replace(true);
    bgp::spawn(&cfg, "fd09::100".parse().unwrap(), "fd09::1".parse().unwrap(), tx.subscribe());
    let (body, _) = next_of(&mut rx, NOTIFICATION, 5).await.expect("a NOTIFICATION");
    assert_eq!(body, [2, 7, 1, 4, 0, 2, 0, 1], "Unsupported Capability: Multiprotocol IPv6 unicast");
    assert!(next_of(&mut rx, UPDATE, 1).await.is_none());
}
