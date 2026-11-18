//! VRRP v3 (RFC 5798) — virtual-router state machine + advertisement codec.
//!
//! [`VirtualRouter`] is the state machine alone — which node holds the VIP
//! and when it transitions Master/Backup — so every rule is unit-testable
//! without root or a peer: each event returns what the caller must do. [`run`]
//! is the wire path around it: a raw IPPROTO_VRRP socket joined to
//! 224.0.0.18 (IPv4) or ff02::12 (IPv6, #39), advertisements while Master,
//! the master-down timer while Backup, and VIP claim/release through
//! [`crate::vip::VipController`].
//!
//! IPv6 follows RFC 5798 for IPv6: the source is the interface's link-local
//! address, the hop limit 255 (checked on receipt, from `IPV6_HOPLIMIT`),
//! the checksum over the IPv6 pseudo-header, and the takeover announced by an
//! unsolicited Neighbor Advertisement. One deviation, as for IPv4: there is
//! no virtual MAC, so no virtual link-local address either; the address list
//! carries the VIP itself (§5.2.9 wants the virtual router's link-local
//! first), and the node's own MAC answers for it.
//!
//! Per RFC 5798 §6.4: a Backup preempts a lower-priority Master (it discards
//! that Master's advertisements, so its master-down timer runs out) unless
//! `preempt` is off; a Backup learns the Master's advertisement interval; a
//! priority-0 advertisement (a Master resigning) cuts the wait to Skew_Time,
//! and a Master that stops sends one. Beyond the RFC, ownership follows
//! backend health: a Master with no healthy backend resigns, and a Backup
//! with none never takes over, so the VIP sits where it can be served.

use crate::vip::VipController;
use anyhow::{Context, Result};
use socket2::{Domain, Protocol, SockAddr, Socket, Type};
use std::mem::MaybeUninit;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddrV4, SocketAddrV6};
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

pub use crate::vip::{interface_ipv4, interface_ipv6_link_local};

/// The link-local multicast group all VRRP routers listen on (RFC 5798).
const VRRP_MCAST: Ipv4Addr = Ipv4Addr::new(224, 0, 0, 18);
/// Its IPv6 counterpart (RFC 5798 §5.1.2.2).
const VRRP_MCAST6: Ipv6Addr = Ipv6Addr::new(0xff02, 0, 0, 0, 0, 0, 0, 0x12);
/// IP protocol number assigned to VRRP.
const IPPROTO_VRRP: i32 = 112;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Initialize,
    Backup,
    Master,
}

/// Whether this node may hold the VIP now (it has a healthy backend).
pub type Healthy = Arc<dyn Fn() -> bool + Send + Sync>;

/// A running VRRP loop's handle: stop it, and read where it stands.
#[derive(Default)]
pub struct Handle {
    stop: AtomicBool,
    /// 0 not running, 1 Backup, 2 Master.
    state: AtomicU8,
}

impl Handle {
    /// Ask the loop to stop. A Master sends priority 0 (so a Backup takes
    /// over after Skew_Time, not the full master-down time) and releases the
    /// VIP, within about half a second.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }
    /// `"master"`, `"backup"`, or `"stopped"` (not started, stopped, or failed).
    pub fn state(&self) -> &'static str {
        match self.state.load(Ordering::Relaxed) {
            1 => "backup",
            2 => "master",
            _ => "stopped",
        }
    }
    fn set(&self, s: State) {
        let v = match s {
            State::Backup => 1,
            State::Master => 2,
            State::Initialize => 0,
        };
        self.state.store(v, Ordering::Relaxed);
    }
}

/// What the caller does after an advertisement arrives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnAdvert {
    /// Nothing (a Master ignoring a lower peer; a preempting Backup
    /// discarding a lower-priority Master, so its timer runs out).
    Ignore,
    /// Backup: restart the master-down timer at this many centiseconds.
    ResetTimer(u32),
    /// Master: advertise now (a peer resigned).
    AdvertiseNow,
    /// Master → Backup: release the VIP, restart the master-down timer at
    /// this many centiseconds.
    Demoted(u32),
}

/// One VRRP virtual router instance.
#[derive(Debug, Clone)]
pub struct VirtualRouter {
    pub vrid: u8,
    /// 1..=255; higher wins. 255 = the VIP's address owner (starts Master).
    pub priority: u8,
    /// A Backup takes over from a lower-priority Master (RFC default).
    pub preempt: bool,
    /// Our advertisement interval, in centiseconds (VRRPv3 unit).
    pub advert_interval_centis: u16,
    /// The Master's advertisement interval, learned from its adverts while
    /// Backup (RFC 5798 Master_Adver_Interval).
    pub master_adver_interval_centis: u16,
    pub state: State,
}

impl VirtualRouter {
    pub fn new(vrid: u8, priority: u8, preempt: bool, advert_interval_secs: u64) -> Self {
        let centis = (advert_interval_secs.max(1) * 100).min(0x0fff) as u16;
        Self {
            vrid,
            priority,
            preempt,
            advert_interval_centis: centis,
            master_adver_interval_centis: centis,
            state: State::Initialize,
        }
    }

    /// Skew_Time = ((256 - priority) * Master_Adver_Interval) / 256.
    pub fn skew_centis(&self) -> u32 {
        ((256 - self.priority as u32) * self.master_adver_interval_centis as u32) / 256
    }

    /// Master_Down_Interval = 3 * Master_Adver_Interval + Skew_Time
    /// (RFC 5798 §6.1).
    pub fn master_down_interval_centis(&self) -> u32 {
        3 * self.master_adver_interval_centis as u32 + self.skew_centis()
    }

    /// Startup: the address owner (priority 255) goes straight to Master —
    /// if it can serve; everyone else starts Backup and waits out the timer.
    pub fn start(&mut self, healthy: bool) -> State {
        self.state = if self.priority == 255 && healthy { State::Master } else { State::Backup };
        self.state
    }

    /// The master-down timer ran out while Backup. Returns true if we now
    /// take over (claim, advertise); false while we have no healthy backend
    /// (the caller restarts the timer).
    pub fn on_master_down_timeout(&mut self, healthy: bool) -> bool {
        if self.state == State::Backup && healthy {
            self.state = State::Master;
            self.master_adver_interval_centis = self.advert_interval_centis;
            return true;
        }
        false
    }

    /// Backend health, checked every turn. Returns true if a Master must
    /// resign now (send priority 0, release, become Backup).
    pub fn on_health(&mut self, healthy: bool) -> bool {
        if self.state == State::Master && !healthy {
            self.state = State::Backup;
            return true;
        }
        false
    }

    /// An advertisement from a peer (RFC 5798 §6.4.2, §6.4.3).
    /// Equal priorities are broken on the primary address (the IPv4
    /// interface address, or the IPv6 link-local): the higher one wins.
    pub fn on_advertisement(&mut self, peer_priority: u8, peer_addr: IpAddr, our_addr: IpAddr, peer_interval_centis: u16) -> OnAdvert {
        match self.state {
            State::Backup => {
                if peer_priority == 0 {
                    // The Master resigned: wait only Skew_Time.
                    OnAdvert::ResetTimer(self.skew_centis())
                } else if !self.preempt || peer_priority >= self.priority {
                    self.master_adver_interval_centis = peer_interval_centis.max(1);
                    OnAdvert::ResetTimer(self.master_down_interval_centis())
                } else {
                    // A lower-priority Master: discard, so the timer runs
                    // out and we preempt it.
                    OnAdvert::Ignore
                }
            }
            State::Master => {
                if peer_priority == 0 {
                    OnAdvert::AdvertiseNow
                } else if peer_priority > self.priority || (peer_priority == self.priority && peer_addr > our_addr) {
                    self.state = State::Backup;
                    self.master_adver_interval_centis = peer_interval_centis.max(1);
                    OnAdvert::Demoted(self.master_down_interval_centis())
                } else {
                    OnAdvert::Ignore
                }
            }
            State::Initialize => OnAdvert::Ignore,
        }
    }
}

/// Encode a VRRP v3 advertisement (the VRRP payload, not the IP header).
/// Layout (RFC 5798 §5.1): version+type, vrid, priority, count, rsvd+max-advert,
/// checksum, then the VIP list (all of `src`'s family). The checksum covers
/// the IPv4 or IPv6 pseudo-header (`src`, 224.0.0.18 or ff02::12, protocol
/// 112, length) as §5.2.8 requires.
pub fn encode_advertisement(vrid: u8, priority: u8, advert_interval_centis: u16, vips: &[IpAddr], src: IpAddr) -> Vec<u8> {
    let mut p = Vec::with_capacity(8 + vips.len() * 16);
    p.push(0x31); // version 3 (high nibble), type 1 = Advertisement (low nibble)
    p.push(vrid);
    p.push(priority);
    p.push(vips.len() as u8); // Count IPvX Addr
    // Rsvd (4 bits) + Max Advertise Interval (12 bits), in centiseconds.
    let mai = advert_interval_centis & 0x0fff;
    p.extend_from_slice(&mai.to_be_bytes());
    p.extend_from_slice(&[0, 0]); // checksum placeholder
    for ip in vips {
        match ip {
            IpAddr::V4(a) => p.extend_from_slice(&a.octets()),
            IpAddr::V6(a) => p.extend_from_slice(&a.octets()),
        }
    }
    let ck = ones_complement_checksum(&with_pseudo_header(&p, src));
    p[6..8].copy_from_slice(&ck.to_be_bytes());
    p
}

/// The pseudo-header for `src`'s family followed by the VRRP payload, for
/// the checksum: IPv4 (src, 224.0.0.18, 0, 112, u16 length) or IPv6 (RFC
/// 8200 §8.1: src, ff02::12, u32 length, three zeros, next header 112).
fn with_pseudo_header(vrrp: &[u8], src: IpAddr) -> Vec<u8> {
    let mut b = Vec::with_capacity(40 + vrrp.len());
    match src {
        IpAddr::V4(src) => {
            b.extend_from_slice(&src.octets());
            b.extend_from_slice(&VRRP_MCAST.octets());
            b.push(0);
            b.push(IPPROTO_VRRP as u8);
            b.extend_from_slice(&(vrrp.len() as u16).to_be_bytes());
        }
        IpAddr::V6(src) => {
            b.extend_from_slice(&src.octets());
            b.extend_from_slice(&VRRP_MCAST6.octets());
            b.extend_from_slice(&(vrrp.len() as u32).to_be_bytes());
            b.extend_from_slice(&[0, 0, 0, IPPROTO_VRRP as u8]);
        }
    }
    b.extend_from_slice(vrrp);
    b
}

/// Standard 16-bit one's-complement checksum over the buffer.
fn ones_complement_checksum(data: &[u8]) -> u16 {
    let mut sum: u32 = 0;
    let mut chunks = data.chunks_exact(2);
    for c in &mut chunks {
        sum += u16::from_be_bytes([c[0], c[1]]) as u32;
    }
    if let [last] = chunks.remainder() {
        sum += (*last as u32) << 8;
    }
    while (sum >> 16) != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

/// A received advertisement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Advert {
    pub priority: u8,
    pub src: IpAddr,
    pub interval_centis: u16,
}

/// Parse a received datagram (IP header + VRRP payload, as delivered by a raw
/// IPPROTO_VRRP socket) if it is a valid v3 advertisement for `our_vrid`:
/// IP TTL 255 (it was not routed, §5.1.1.3) and a correct checksum.
pub fn parse_advertisement(datagram: &[u8], our_vrid: u8) -> Option<Advert> {
    if datagram.len() < 20 {
        return None;
    }
    let ihl = ((datagram[0] & 0x0f) as usize) * 4;
    if ihl < 20 || datagram.len() < ihl + 8 || datagram[8] != 255 {
        return None;
    }
    let src = IpAddr::V4(Ipv4Addr::new(datagram[12], datagram[13], datagram[14], datagram[15]));
    parse_payload(&datagram[ihl..], src, our_vrid)
}

/// Parse a received IPv6 advertisement: a raw IPv6 socket delivers the VRRP
/// payload alone, with the source from the socket address and the hop limit
/// from `IPV6_HOPLIMIT` ancillary data. Valid if the hop limit is 255 (it was
/// not routed), the checksum over the IPv6 pseudo-header is right, and it is
/// a v3 advertisement for `our_vrid`.
pub fn parse_advertisement_v6(payload: &[u8], src: Ipv6Addr, hop_limit: Option<u8>, our_vrid: u8) -> Option<Advert> {
    if hop_limit != Some(255) {
        return None;
    }
    parse_payload(payload, IpAddr::V6(src), our_vrid)
}

fn parse_payload(vrrp: &[u8], src: IpAddr, our_vrid: u8) -> Option<Advert> {
    if vrrp.len() < 8 {
        return None;
    }
    let version = vrrp[0] >> 4;
    let vtype = vrrp[0] & 0x0f;
    if version != 3 || vtype != 1 || vrrp[1] != our_vrid {
        return None;
    }
    if ones_complement_checksum(&with_pseudo_header(vrrp, src)) != 0 {
        return None;
    }
    Some(Advert { priority: vrrp[2], src, interval_centis: u16::from_be_bytes([vrrp[4], vrrp[5]]) & 0x0fff })
}

/// One VRRP instance's settings.
#[derive(Debug, Clone)]
pub struct Params {
    pub vrid: u8,
    pub priority: u8,
    pub preempt: bool,
    pub advert_interval_secs: u64,
    pub iface: String,
    /// IPv4 or IPv6: the instance speaks that family's VRRP.
    pub vip: IpAddr,
}

/// Run the VRRP control loop for one virtual router. Blocking — intended to be
/// spawned on a dedicated OS thread. Sends advertisements while Master, watches
/// for the master-down timeout while Backup, and claims/releases the VIP through
/// `ctl` on transitions; `healthy` is asked every turn. Requires CAP_NET_ADMIN /
/// CAP_NET_RAW. Returns when `handle` is stopped, after resigning if Master.
pub fn run(p: Params, ctl: Arc<dyn VipController>, healthy: Healthy, handle: Arc<Handle>) -> Result<()> {
    let r = run_loop(&p, ctl, healthy, &handle);
    handle.set(State::Initialize);
    r
}

fn run_loop(p: &Params, ctl: Arc<dyn VipController>, healthy: Healthy, handle: &Handle) -> Result<()> {
    let (vrid, iface, vip) = (p.vrid, p.iface.as_str(), p.vip);
    let Some(wire) = Wire::open(iface, vip, handle)? else { return Ok(()) };
    let src = wire.src;

    let mut vr = VirtualRouter::new(vrid, p.priority, p.preempt, p.advert_interval_secs);
    let interval_centis = vr.advert_interval_centis;
    let advert_interval = Duration::from_millis(interval_centis as u64 * 10);
    let centis = |c: u32| Duration::from_millis(c as u64 * 10);
    let vip_s = vip.to_string();
    let send = |prio: u8| {
        let pkt = encode_advertisement(vrid, prio, interval_centis, &[vip], src);
        if let Err(e) = wire.sock.send_to(&pkt, &wire.dst) {
            warn!("VRRP vrid={vrid}: send advert failed: {e}");
        }
    };

    let state = vr.start(healthy());
    handle.set(state);
    info!("VRRP vrid={vrid} priority={} preempt={} iface={iface} vip={vip} -> {state:?}", p.priority, p.preempt);
    let mut next_advert = Instant::now();
    let mut master_down_deadline = Instant::now() + centis(vr.master_down_interval_centis());
    if state == State::Master {
        if let Err(e) = ctl.claim(&vip_s, iface) {
            warn!("VRRP: could not claim VIP {vip} on {iface}: {e:#}");
        }
    }

    loop {
        if handle.stop.load(Ordering::Relaxed) {
            if vr.state == State::Master {
                info!("VRRP vrid={vrid}: stopped — resigning, releasing VIP {vip}");
                send(0);
                let _ = ctl.release(&vip_s, iface);
            }
            return Ok(());
        }
        if vr.on_health(healthy()) {
            info!("VRRP vrid={vrid}: no healthy backend — resigning, releasing VIP {vip}");
            send(0);
            let _ = ctl.release(&vip_s, iface);
            master_down_deadline = Instant::now() + centis(vr.master_down_interval_centis());
        }
        handle.set(vr.state);
        // Wake for the next advert (Master) or the master-down deadline
        // (Backup), and at least every 500 ms to see a stop or a health change.
        let now = Instant::now();
        let wake = if vr.state == State::Master { next_advert } else { master_down_deadline };
        let timeout = wake.saturating_duration_since(now).clamp(Duration::from_millis(10), Duration::from_millis(500));
        wire.sock.set_read_timeout(Some(timeout))?;

        match wire.recv(vrid) {
            Ok(None) => {}
            Ok(Some(a)) => {
                if a.src == src {
                    continue; // our own advert (shouldn't happen with loop off)
                }
                match vr.on_advertisement(a.priority, a.src, src, a.interval_centis) {
                    OnAdvert::Ignore => {}
                    OnAdvert::ResetTimer(c) => master_down_deadline = Instant::now() + centis(c),
                    OnAdvert::AdvertiseNow => next_advert = Instant::now(),
                    OnAdvert::Demoted(c) => {
                        info!("VRRP vrid={vrid}: preempted by {} (prio {}) — releasing VIP", a.src, a.priority);
                        let _ = ctl.release(&vip_s, iface);
                        master_down_deadline = Instant::now() + centis(c);
                    }
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
                let now = Instant::now();
                match vr.state {
                    State::Master if now >= next_advert => {
                        send(p.priority);
                        next_advert = now + advert_interval;
                    }
                    State::Backup if now >= master_down_deadline => {
                        if vr.on_master_down_timeout(healthy()) {
                            info!("VRRP vrid={vrid}: master-down — becoming MASTER, claiming VIP {vip}");
                            if let Err(e) = ctl.claim(&vip_s, iface) {
                                warn!("VRRP: could not claim VIP: {e:#}");
                            }
                            next_advert = now; // advertise immediately
                        } else {
                            master_down_deadline = now + centis(vr.master_down_interval_centis());
                        }
                    }
                    _ => {}
                }
            }
            Err(e) => return Err(e).context("VRRP recv"),
        }
    }
}

/// A VRRP socket for one family on one interface.
struct Wire {
    sock: Socket,
    /// Our primary address: the advert source and the tie-break.
    src: IpAddr,
    dst: SockAddr,
}

impl Wire {
    /// Open the socket for `vip`'s family on `iface`. The source address is
    /// waited for (up to 10 s: a link-local is tentative for a moment after
    /// the link comes up); `None` if `handle` is stopped meanwhile.
    fn open(iface: &str, vip: IpAddr, handle: &Handle) -> Result<Option<Wire>> {
        let deadline = Instant::now() + Duration::from_secs(10);
        let src = loop {
            let r = match vip {
                IpAddr::V4(_) => interface_ipv4(iface).map(IpAddr::V4),
                IpAddr::V6(_) => interface_ipv6_link_local(iface).map(IpAddr::V6),
            };
            match r {
                Ok(a) => break a,
                Err(e) if Instant::now() >= deadline => return Err(e),
                Err(_) if handle.stop.load(Ordering::Relaxed) => return Ok(None),
                Err(_) => std::thread::sleep(Duration::from_millis(200)),
            }
        };
        match src {
            IpAddr::V4(src4) => {
                let sock = Socket::new(Domain::IPV4, Type::RAW, Some(Protocol::from(IPPROTO_VRRP)))
                    .context("creating VRRP raw socket (needs CAP_NET_RAW)")?;
                sock.set_multicast_if_v4(&src4)?;
                sock.set_multicast_ttl_v4(255)?;
                sock.set_multicast_loop_v4(false)?;
                sock.join_multicast_v4(&VRRP_MCAST, &src4).context("joining 224.0.0.18")?;
                Ok(Some(Wire { sock, src, dst: SockAddr::from(SocketAddrV4::new(VRRP_MCAST, 0)) }))
            }
            IpAddr::V6(src6) => {
                let index = crate::vip::link(iface)?.index;
                let sock = Socket::new(Domain::IPV6, Type::RAW, Some(Protocol::from(IPPROTO_VRRP)))
                    .context("creating VRRP raw IPv6 socket (needs CAP_NET_RAW)")?;
                sock.set_multicast_if_v6(index)?;
                sock.set_multicast_hops_v6(255)?;
                sock.set_multicast_loop_v6(false)?;
                set_recv_hoplimit(&sock)?;
                // Bound to the link-local (and so to the interface): the
                // kernel sends from it, as the checksum assumes.
                sock.bind(&SockAddr::from(SocketAddrV6::new(src6, 0, 0, index)))
                    .with_context(|| format!("binding the VRRP socket to {src6}%{iface}"))?;
                sock.join_multicast_v6(&VRRP_MCAST6, index).context("joining ff02::12")?;
                Ok(Some(Wire { sock, src, dst: SockAddr::from(SocketAddrV6::new(VRRP_MCAST6, 0, 0, index)) }))
            }
        }
    }

    /// One datagram: `Some` if it is a valid advertisement for `vrid`.
    fn recv(&self, vrid: u8) -> std::io::Result<Option<Advert>> {
        let mut buf = [0u8; 512];
        match self.src {
            IpAddr::V4(_) => {
                // SAFETY: a [u8] is a valid [MaybeUninit<u8>] of the same length.
                let ubuf = unsafe { &mut *(&mut buf[..] as *mut [u8] as *mut [MaybeUninit<u8>]) };
                let (n, _) = self.sock.recv_from(ubuf)?;
                Ok(parse_advertisement(&buf[..n], vrid))
            }
            IpAddr::V6(_) => {
                let (n, from, hops) = recv_v6(&self.sock, &mut buf)?;
                Ok(parse_advertisement_v6(&buf[..n], from, hops, vrid))
            }
        }
    }
}

/// Ask for each datagram's hop limit (`IPV6_HOPLIMIT` ancillary data).
fn set_recv_hoplimit(sock: &Socket) -> std::io::Result<()> {
    let on: libc::c_int = 1;
    // SAFETY: a valid fd and a c_int option of its size.
    let r = unsafe {
        libc::setsockopt(
            sock.as_raw_fd(),
            libc::IPPROTO_IPV6,
            libc::IPV6_RECVHOPLIMIT,
            &on as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        )
    };
    if r < 0 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(())
}

/// recvmsg on a raw IPv6 socket: (length, source, hop limit if reported).
fn recv_v6(sock: &Socket, buf: &mut [u8]) -> std::io::Result<(usize, Ipv6Addr, Option<u8>)> {
    // SAFETY: every pointer in msg points at a live local buffer of the
    // length given; the cmsg walk stays within msg_controllen.
    unsafe {
        let mut from: libc::sockaddr_in6 = std::mem::zeroed();
        let mut iov = libc::iovec { iov_base: buf.as_mut_ptr() as *mut libc::c_void, iov_len: buf.len() };
        let mut ctrl = [0u64; 16];
        let mut msg: libc::msghdr = std::mem::zeroed();
        msg.msg_name = &mut from as *mut _ as *mut libc::c_void;
        msg.msg_namelen = std::mem::size_of::<libc::sockaddr_in6>() as libc::socklen_t;
        msg.msg_iov = &mut iov;
        msg.msg_iovlen = 1;
        msg.msg_control = ctrl.as_mut_ptr() as *mut libc::c_void;
        msg.msg_controllen = std::mem::size_of_val(&ctrl) as _;
        let n = libc::recvmsg(sock.as_raw_fd(), &mut msg, 0);
        if n < 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut hops = None;
        let mut c = libc::CMSG_FIRSTHDR(&msg);
        while !c.is_null() {
            if (*c).cmsg_level == libc::IPPROTO_IPV6 && (*c).cmsg_type == libc::IPV6_HOPLIMIT {
                let v = std::ptr::read_unaligned(libc::CMSG_DATA(c) as *const libc::c_int);
                hops = u8::try_from(v).ok();
            }
            c = libc::CMSG_NXTHDR(&msg, c);
        }
        Ok((n as usize, Ipv6Addr::from(from.sin6_addr.s6_addr), hops))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: IpAddr = IpAddr::V4(Ipv4Addr::new(192, 168, 8, 10));
    const B: IpAddr = IpAddr::V4(Ipv4Addr::new(192, 168, 8, 20));

    fn master(prio: u8, preempt: bool) -> VirtualRouter {
        let mut vr = VirtualRouter::new(51, prio, preempt, 1);
        vr.start(true);
        assert!(vr.on_master_down_timeout(true));
        vr
    }

    #[test]
    fn address_owner_starts_master_only_when_healthy() {
        assert_eq!(VirtualRouter::new(51, 255, true, 1).start(true), State::Master);
        assert_eq!(VirtualRouter::new(51, 255, true, 1).start(false), State::Backup);
        assert_eq!(VirtualRouter::new(51, 100, true, 1).start(true), State::Backup);
    }

    #[test]
    fn backup_takes_over_on_timeout_only_when_healthy() {
        let mut vr = VirtualRouter::new(51, 100, true, 1);
        vr.start(true);
        assert!(!vr.on_master_down_timeout(false), "no healthy backend: stay Backup");
        assert_eq!(vr.state, State::Backup);
        assert!(vr.on_master_down_timeout(true));
        assert_eq!(vr.state, State::Master);
        assert!(!vr.on_master_down_timeout(true), "idempotent once Master");
    }

    #[test]
    fn a_backup_preempts_a_lower_priority_master() {
        let mut vr = VirtualRouter::new(51, 200, true, 1);
        vr.start(true);
        // The Master is 100: its adverts are discarded, so the timer runs out.
        assert_eq!(vr.on_advertisement(100, A, B, 100), OnAdvert::Ignore);
        // A Master at or above us resets the timer.
        assert_eq!(vr.on_advertisement(200, A, B, 100), OnAdvert::ResetTimer(vr.master_down_interval_centis()));
        assert_eq!(vr.on_advertisement(250, A, B, 100), OnAdvert::ResetTimer(vr.master_down_interval_centis()));
        // The timer runs out: we take over, and the old Master yields to our adverts.
        assert!(vr.on_master_down_timeout(true));
        let mut old = master(100, true);
        assert!(matches!(old.on_advertisement(200, B, A, 100), OnAdvert::Demoted(_)));
        assert_eq!(old.state, State::Backup);
    }

    #[test]
    fn without_preempt_a_backup_waits_for_any_master() {
        let mut vr = VirtualRouter::new(51, 200, false, 1);
        vr.start(true);
        assert!(matches!(vr.on_advertisement(100, A, B, 100), OnAdvert::ResetTimer(_)));
    }

    #[test]
    fn a_backup_learns_the_masters_interval() {
        let mut vr = VirtualRouter::new(51, 100, true, 1);
        vr.start(true);
        // Master advertises every 3 s: down = 3*300 + (156*300)/256 = 1082.
        assert_eq!(vr.on_advertisement(200, A, B, 300), OnAdvert::ResetTimer(1082));
        assert_eq!(vr.master_adver_interval_centis, 300);
        // Taking over goes back to our own interval.
        assert!(vr.on_master_down_timeout(true));
        assert_eq!(vr.master_adver_interval_centis, 100);
    }

    #[test]
    fn priority_zero_cuts_a_backups_wait_to_skew_time() {
        let mut vr = VirtualRouter::new(51, 100, true, 1);
        vr.start(true);
        // skew = (156*100)/256 = 60 cs, not the 360 cs master-down.
        assert_eq!(vr.on_advertisement(0, A, B, 100), OnAdvert::ResetTimer(60));
        let mut m = master(100, true);
        assert_eq!(m.on_advertisement(0, A, B, 100), OnAdvert::AdvertiseNow, "a Master answers a resign at once");
        assert_eq!(m.state, State::Master);
    }

    #[test]
    fn a_master_with_no_healthy_backend_resigns() {
        let mut m = master(200, true);
        assert!(!m.on_health(true));
        assert!(m.on_health(false), "resign: priority 0 and release");
        assert_eq!(m.state, State::Backup);
        assert!(!m.on_health(false), "only once");
        // Health back: a Backup again, it takes over on the next timeout.
        assert!(m.on_master_down_timeout(true));
    }

    #[test]
    fn a_master_yields_to_higher_priority_and_ties_on_address() {
        let mut m = master(100, true);
        assert_eq!(m.on_advertisement(50, B, A, 100), OnAdvert::Ignore);
        assert_eq!(m.state, State::Master);
        assert!(matches!(m.on_advertisement(100, B, A, 100), OnAdvert::Demoted(_)), "equal priority, higher address wins");
        let mut m = master(100, true);
        assert_eq!(m.on_advertisement(100, A, B, 100), OnAdvert::Ignore, "equal priority, lower address loses");
    }

    #[test]
    fn master_down_interval_matches_rfc() {
        // advert=100cs, priority=100: skew=((256-100)*100)/256 = 60; 3*100+60=360
        let vr = VirtualRouter::new(51, 100, true, 1);
        assert_eq!(vr.master_down_interval_centis(), 360);
    }

    fn datagram(vrrp: &[u8], src: [u8; 4], ttl: u8) -> Vec<u8> {
        let mut dg = vec![0u8; 20];
        dg[0] = 0x45; // IPv4, IHL=5 (20-byte header)
        dg[8] = ttl;
        dg[12..16].copy_from_slice(&src);
        dg.extend_from_slice(vrrp);
        dg
    }

    #[test]
    fn parses_advertisement_from_raw_datagram() {
        let src = Ipv4Addr::new(192, 168, 8, 11);
        let vrrp = encode_advertisement(51, 200, 100, &[Ipv4Addr::new(192, 168, 8, 50).into()], src.into());
        let dg = datagram(&vrrp, src.octets(), 255);
        assert_eq!(parse_advertisement(&dg, 51), Some(Advert { priority: 200, src: src.into(), interval_centis: 100 }));
        // Wrong vrid, a too-short datagram, a routed one (TTL < 255), a
        // checksum made for another source: all None.
        assert!(parse_advertisement(&dg, 99).is_none());
        assert!(parse_advertisement(&dg[..10], 51).is_none());
        assert!(parse_advertisement(&datagram(&vrrp, src.octets(), 254), 51).is_none());
        assert!(parse_advertisement(&datagram(&vrrp, [192, 168, 8, 12], 255), 51).is_none());
    }

    #[test]
    fn advertisement_encodes_with_a_pseudo_header_checksum() {
        let vips = [Ipv4Addr::new(192, 168, 8, 50).into()];
        let src: IpAddr = Ipv4Addr::new(192, 168, 8, 11).into();
        let pkt = encode_advertisement(51, 200, 100, &vips, src);
        assert_eq!(pkt[0], 0x31); // v3, advertisement
        assert_eq!(pkt[1], 51); // vrid
        assert_eq!(pkt[2], 200); // priority
        assert_eq!(pkt[3], 1); // one VIP
        assert_eq!(u16::from_be_bytes([pkt[4], pkt[5]]), 100);
        assert_eq!(&pkt[8..12], &[192, 168, 8, 50]);
        // Over the pseudo-header and the packet, the sum checks to zero; over
        // the packet alone (the old, non-RFC checksum) it does not.
        assert_eq!(ones_complement_checksum(&with_pseudo_header(&pkt, src)), 0);
        assert_ne!(ones_complement_checksum(&pkt), 0);
    }

    #[test]
    fn ipv6_advertisement_checksums_over_the_ipv6_pseudo_header() {
        let src: Ipv6Addr = "fe80::1".parse().unwrap();
        let vip: Ipv6Addr = "fd09::100".parse().unwrap();
        let pkt = encode_advertisement(51, 200, 100, &[vip.into()], src.into());
        assert_eq!(pkt.len(), 8 + 16);
        assert_eq!(pkt[3], 1);
        assert_eq!(&pkt[8..24], &vip.octets());
        // Pseudo-header: src, ff02::12, u32 length, 0 0 0 112.
        let ph = with_pseudo_header(&pkt, src.into());
        assert_eq!(&ph[16..32], &VRRP_MCAST6.octets());
        assert_eq!(&ph[32..40], &[0, 0, 0, 24, 0, 0, 0, 112]);
        assert_eq!(ones_complement_checksum(&ph), 0);
    }

    #[test]
    fn parses_an_ipv6_advertisement_only_at_hop_limit_255() {
        let src: Ipv6Addr = "fe80::2".parse().unwrap();
        let pkt = encode_advertisement(51, 150, 100, &["fd09::100".parse().unwrap()], src.into());
        assert_eq!(
            parse_advertisement_v6(&pkt, src, Some(255), 51),
            Some(Advert { priority: 150, src: src.into(), interval_centis: 100 })
        );
        assert!(parse_advertisement_v6(&pkt, src, Some(254), 51).is_none(), "routed");
        assert!(parse_advertisement_v6(&pkt, src, None, 51).is_none(), "hop limit unknown");
        assert!(parse_advertisement_v6(&pkt, src, Some(255), 52).is_none(), "another vrid");
        assert!(parse_advertisement_v6(&pkt, "fe80::3".parse().unwrap(), Some(255), 51).is_none(), "another source");
        assert!(parse_advertisement_v6(&pkt[..7], src, Some(255), 51).is_none(), "short");
    }

    #[test]
    fn ipv6_ties_break_on_the_link_local() {
        let lo: IpAddr = "fe80::1".parse().unwrap();
        let hi: IpAddr = "fe80::2".parse().unwrap();
        let mut m = master(100, true);
        assert_eq!(m.on_advertisement(100, lo, hi, 100), OnAdvert::Ignore);
        assert!(matches!(m.on_advertisement(100, hi, lo, 100), OnAdvert::Demoted(_)));
    }
}
