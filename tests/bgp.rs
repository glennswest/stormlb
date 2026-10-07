//! Integration tests: the BGP speaker against a fake peer (#6). The peer
//! listens on a free port, records every message stormlb sends with when it
//! arrived, and plays its side of the session as each test needs.

use std::net::Ipv4Addr;
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

fn open(asn: u16, hold: u16) -> Vec<u8> {
    let mut b = vec![4];
    b.extend_from_slice(&asn.to_be_bytes());
    b.extend_from_slice(&hold.to_be_bytes());
    b.extend_from_slice(&[10, 0, 0, 2, 0]);
    msg(OPEN, &b)
}

/// How the fake peer behaves.
#[derive(Clone, Copy)]
struct Play {
    asn: u16,
    hold: u16,
    /// Wait this long after stormlb's OPEN before answering.
    delay_open: Duration,
    /// Keep sending KEEPALIVEs (every hold/3); false: silent after the first.
    keepalives: bool,
}

/// What the peer saw: (type, body, when), and when it sent its KEEPALIVE.
enum Seen {
    Msg(u8, Vec<u8>, Instant),
    SentKeepalive(Instant),
    Closed(Instant),
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
                    let _ = txr.send(Seen::Closed(Instant::now()));
                    return;
                }
                let len = u16::from_be_bytes([hdr[16], hdr[17]]) as usize;
                let mut body = vec![0u8; len - 19];
                if r.read_exact(&mut body).await.is_err() {
                    let _ = txr.send(Seen::Closed(Instant::now()));
                    return;
                }
                let _ = txr.send(Seen::Msg(hdr[18], body, Instant::now()));
            }
        });
        tokio::time::sleep(play.delay_open).await;
        // Stamped before the OPEN goes: stormlb can't be Established earlier.
        let at = Instant::now();
        let _ = w.write_all(&open(play.asn, play.hold)).await;
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
    let play = Play { asn: 65001, hold: 9, delay_open: Duration::from_millis(1500), keepalives: true };
    let (port, mut rx) = fake_peer(play).await;
    let (tx, cfg) = speaker(port, 65001);
    tx.send_replace(true); // healthy from the start
    bgp::spawn(&cfg, VIP, Ipv4Addr::new(10, 0, 0, 1), tx.subscribe());

    // Our OPEN first, carrying our AS and hold time.
    let (o, opened) = next_of(&mut rx, OPEN, 5).await.expect("stormlb sends OPEN");
    assert_eq!(u16::from_be_bytes([o[1], o[2]]), 64512);
    assert_eq!(u16::from_be_bytes([o[3], o[4]]), 180);

    // Nothing is announced before the peer's OPEN and KEEPALIVE (1.5 s
    // later); then the announce follows at once.
    // Both events, in whichever order the two tasks report them.
    let (mut established, mut first) = (None, None);
    while established.is_none() || first.is_none() {
        match tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.expect("progress").unwrap() {
            Seen::SentKeepalive(at) => established = Some(at),
            Seen::Msg(UPDATE, body, at) if first.is_none() => first = Some((body, at)),
            Seen::Msg(..) => {}
            Seen::Closed(_) => panic!("session closed"),
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
    let play = Play { asn: 65001, hold: 3, delay_open: Duration::ZERO, keepalives: false };
    let (port, mut rx) = fake_peer(play).await;
    let (tx, cfg) = speaker(port, 65001);
    bgp::spawn(&cfg, VIP, Ipv4Addr::new(10, 0, 0, 1), tx.subscribe());
    let mut established = None;
    let mut keepalives = 0;
    let (body, at) = loop {
        match tokio::time::timeout(Duration::from_secs(8), rx.recv()).await.expect("progress").unwrap() {
            Seen::SentKeepalive(at) => established = Some(at),
            Seen::Msg(KEEPALIVE, _, _) => keepalives += 1,
            Seen::Msg(NOTIFICATION, body, at) => break (body, at),
            Seen::Msg(..) => {}
            Seen::Closed(_) => panic!("closed without a NOTIFICATION"),
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
    let play = Play { asn: 65002, hold: 90, delay_open: Duration::ZERO, keepalives: true };
    let (port, mut rx) = fake_peer(play).await;
    let (tx, cfg) = speaker(port, 65001);
    tx.send_replace(true);
    bgp::spawn(&cfg, VIP, Ipv4Addr::new(10, 0, 0, 1), tx.subscribe());
    let (body, _) = next_of(&mut rx, NOTIFICATION, 5).await.expect("a NOTIFICATION");
    assert_eq!(body[..2], [2, 2], "OPEN Message Error / Bad Peer AS");
    // And no route was ever announced on that session.
    assert!(next_of(&mut rx, UPDATE, 1).await.is_none());
}
