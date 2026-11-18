//! Add/remove the VIP on a local interface, and announce takeover.
//!
//! Behind a trait so the VRRP logic stays testable without root: tests use a
//! recording mock, production uses [`Netlink`]. Everything is a kernel API —
//! rtnetlink for links and addresses, an `AF_PACKET` socket for the
//! gratuitous ARP (IPv4), a raw ICMPv6 socket for the unsolicited Neighbor
//! Advertisement (IPv6, #39) — so the node needs no `ip`, `arping` or
//! `ndsend` binary (the shipped golden has none). Needs CAP_NET_ADMIN
//! (addresses) and CAP_NET_RAW (ARP, NA).

use anyhow::{Context, Result};
use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddrV6};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::time::Duration;
use tracing::{info, warn};

pub trait VipController: Send + Sync {
    /// Bring the VIP up on `iface` (idempotent) and announce it (gratuitous
    /// ARP for IPv4, unsolicited Neighbor Advertisement for IPv6).
    fn claim(&self, vip: &str, iface: &str) -> Result<()>;
    /// Remove the VIP from `iface` (idempotent).
    fn release(&self, vip: &str, iface: &str) -> Result<()>;
}

/// The rtnetlink controller: `<vip>/32` (IPv4) or `<vip>/128` (IPv6, with
/// `IFA_F_NODAD` so it is usable at once rather than after duplicate address
/// detection: the VRRP election already decided who holds it) added to and
/// deleted from the interface, then three announcements — gratuitous ARP
/// replies, or unsolicited Neighbor Advertisements with the override flag —
/// so neighbours and switches learn the new owner at once.
pub struct Netlink;

impl VipController for Netlink {
    fn claim(&self, vip: &str, iface: &str) -> Result<()> {
        let ip: IpAddr = vip.parse().with_context(|| format!("VIP {vip} is not an IP address"))?;
        let l = link(iface)?;
        info!("claiming VIP {vip} on {iface}");
        let (payload, len) = match ip {
            IpAddr::V4(v4) => (addr_payload(l.index, v4), 32),
            IpAddr::V6(v6) => (addr_payload6(l.index, v6), 128),
        };
        match addr_change(RTM_NEWADDR, NLM_F_CREATE | NLM_F_EXCL, &payload) {
            Err(e) if e.raw_os_error() == Some(libc::EEXIST) => {}
            r => r.with_context(|| format!("adding {vip}/{len} to {iface}"))?,
        }
        if l.mac != [0; 6] {
            // Spaced out, off the caller's thread: VRRP must keep advertising.
            let index = l.index;
            let mac = l.mac;
            std::thread::spawn(move || {
                for i in 0..3 {
                    let r = match ip {
                        IpAddr::V4(v4) => send_garp(index, mac, v4),
                        IpAddr::V6(v6) => send_na(index, mac, v6),
                    };
                    if let Err(e) = r {
                        warn!("announcing {ip} (gratuitous ARP / neighbor advertisement): {e}");
                        return;
                    }
                    if i < 2 {
                        std::thread::sleep(Duration::from_millis(500));
                    }
                }
            });
        }
        Ok(())
    }

    fn release(&self, vip: &str, iface: &str) -> Result<()> {
        let ip: IpAddr = vip.parse().with_context(|| format!("VIP {vip} is not an IP address"))?;
        let l = link(iface)?;
        info!("releasing VIP {vip} from {iface}");
        let (payload, len) = match ip {
            IpAddr::V4(v4) => (addr_payload(l.index, v4), 32),
            IpAddr::V6(v6) => (addr_payload6(l.index, v6), 128),
        };
        match addr_change(RTM_DELADDR, 0, &payload) {
            Err(e) if e.raw_os_error() == Some(libc::EADDRNOTAVAIL) => Ok(()),
            r => r.with_context(|| format!("deleting {vip}/{len} from {iface}")),
        }
    }
}

/// An interface: its index and hardware address (all zero for `lo`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Link {
    pub index: u32,
    pub mac: [u8; 6],
}

/// Look an interface up by name.
pub fn link(iface: &str) -> Result<Link> {
    let msgs = Nl::open()?
        .request(RTM_GETLINK, NLM_F_DUMP, &[0u8; 16])
        .context("listing interfaces (rtnetlink)")?;
    parse_links(&msgs, iface).with_context(|| format!("no interface {iface}"))
}

/// The interface's own IPv4 address, the VRRP source: its first address
/// that is not a /32 (a VIP is), else its first.
pub fn interface_ipv4(iface: &str) -> Result<Ipv4Addr> {
    let l = link(iface)?;
    let mut req = [0u8; 8];
    req[0] = libc::AF_INET as u8;
    let msgs = Nl::open()?
        .request(RTM_GETADDR, NLM_F_DUMP, &req)
        .context("listing addresses (rtnetlink)")?;
    let addrs = parse_addrs(&msgs, l.index);
    addrs
        .iter()
        .find(|(_, len)| *len < 32)
        .or(addrs.first())
        .map(|(a, _)| *a)
        .with_context(|| format!("no IPv4 address on interface {iface}"))
}

/// The interface's IPv6 link-local address, the VRRP source for IPv6 (RFC
/// 5798 §5.1.2.1): its first `fe80::/10` address that has finished duplicate
/// address detection (not tentative, not failed).
pub fn interface_ipv6_link_local(iface: &str) -> Result<Ipv6Addr> {
    let l = link(iface)?;
    let mut req = [0u8; 8];
    req[0] = libc::AF_INET6 as u8;
    let msgs = Nl::open()?
        .request(RTM_GETADDR, NLM_F_DUMP, &req)
        .context("listing addresses (rtnetlink)")?;
    parse_addrs6(&msgs, l.index)
        .into_iter()
        .find(|a| is_link_local(&a.addr) && a.flags & (IFA_F_TENTATIVE | IFA_F_DADFAILED) == 0)
        .map(|a| a.addr)
        .with_context(|| format!("no usable IPv6 link-local address on interface {iface} (IPv6 off, or DAD not finished)"))
}

/// `fe80::/10`.
pub fn is_link_local(a: &Ipv6Addr) -> bool {
    a.segments()[0] & 0xffc0 == 0xfe80
}

// ── rtnetlink ──────────────────────────────────────────────────────────────

const NLMSG_ERROR: u16 = 2;
const NLMSG_DONE: u16 = 3;
const RTM_NEWLINK: u16 = 16;
const RTM_GETLINK: u16 = 18;
const RTM_NEWADDR: u16 = 20;
const RTM_DELADDR: u16 = 21;
const RTM_GETADDR: u16 = 22;
const NLM_F_REQUEST: u16 = 0x1;
const NLM_F_ACK: u16 = 0x4;
const NLM_F_EXCL: u16 = 0x200;
const NLM_F_CREATE: u16 = 0x400;
const NLM_F_DUMP: u16 = 0x300;
const IFLA_ADDRESS: u16 = 1;
const IFLA_IFNAME: u16 = 3;
const IFA_ADDRESS: u16 = 1;
const IFA_LOCAL: u16 = 2;
const IFA_FLAGS: u16 = 8;
const IFA_F_NODAD: u8 = 0x02;
const IFA_F_DADFAILED: u32 = 0x08;
const IFA_F_TENTATIVE: u32 = 0x40;

/// An address change (an [`addr_payload`] or [`addr_payload6`]) as
/// RTM_NEWADDR or RTM_DELADDR.
fn addr_change(ty: u16, flags: u16, payload: &[u8]) -> io::Result<()> {
    Nl::open()?.request(ty, flags | NLM_F_ACK, payload).map(|_| ())
}

/// ifaddrmsg {AF_INET, /32, flags 0, scope universe, index} + IFA_LOCAL +
/// IFA_ADDRESS.
pub fn addr_payload(index: u32, ip: Ipv4Addr) -> Vec<u8> {
    let mut p = vec![libc::AF_INET as u8, 32, 0, 0];
    p.extend_from_slice(&index.to_ne_bytes());
    for ty in [IFA_LOCAL, IFA_ADDRESS] {
        p.extend_from_slice(&8u16.to_ne_bytes());
        p.extend_from_slice(&ty.to_ne_bytes());
        p.extend_from_slice(&ip.octets());
    }
    p
}

/// ifaddrmsg {AF_INET6, /128, IFA_F_NODAD, scope universe, index} +
/// IFA_ADDRESS: `<vip>/128`, usable at once (no duplicate address detection).
pub fn addr_payload6(index: u32, ip: Ipv6Addr) -> Vec<u8> {
    let mut p = vec![libc::AF_INET6 as u8, 128, IFA_F_NODAD, 0];
    p.extend_from_slice(&index.to_ne_bytes());
    p.extend_from_slice(&20u16.to_ne_bytes());
    p.extend_from_slice(&IFA_ADDRESS.to_ne_bytes());
    p.extend_from_slice(&ip.octets());
    p
}

/// One netlink message: header + payload, padded to 4.
pub fn nl_msg(ty: u16, flags: u16, seq: u32, payload: &[u8]) -> Vec<u8> {
    let len = 16 + payload.len();
    let mut m = Vec::with_capacity(align(len));
    m.extend_from_slice(&(len as u32).to_ne_bytes());
    m.extend_from_slice(&ty.to_ne_bytes());
    m.extend_from_slice(&(flags | NLM_F_REQUEST).to_ne_bytes());
    m.extend_from_slice(&seq.to_ne_bytes());
    m.extend_from_slice(&0u32.to_ne_bytes());
    m.extend_from_slice(payload);
    m.resize(align(len), 0);
    m
}

fn align(n: usize) -> usize {
    (n + 3) & !3
}

fn u16_at(b: &[u8], i: usize) -> u16 {
    u16::from_ne_bytes([b[i], b[i + 1]])
}
fn u32_at(b: &[u8], i: usize) -> u32 {
    u32::from_ne_bytes([b[i], b[i + 1], b[i + 2], b[i + 3]])
}

/// The rtattrs in `b`, as (type, data).
fn attrs(mut b: &[u8]) -> Vec<(u16, &[u8])> {
    let mut out = Vec::new();
    while b.len() >= 4 {
        let len = u16_at(b, 0) as usize;
        if len < 4 || len > b.len() {
            break;
        }
        out.push((u16_at(b, 2) & 0x3fff, &b[4..len]));
        b = &b[align(len).min(b.len())..];
    }
    out
}

/// The interface named `name` among RTM_NEWLINK messages.
pub fn parse_links(msgs: &[(u16, Vec<u8>)], name: &str) -> Option<Link> {
    for (ty, p) in msgs {
        if *ty != RTM_NEWLINK || p.len() < 16 {
            continue;
        }
        let index = u32_at(p, 4);
        let (mut found, mut mac) = (false, [0u8; 6]);
        for (t, d) in attrs(&p[16..]) {
            match t {
                IFLA_IFNAME => found = d.split(|&c| c == 0).next() == Some(name.as_bytes()),
                IFLA_ADDRESS if d.len() == 6 => mac.copy_from_slice(d),
                _ => {}
            }
        }
        if found {
            return Some(Link { index, mac });
        }
    }
    None
}

/// Interface `index`'s IPv4 addresses with their prefix lengths, in order,
/// among RTM_NEWADDR messages.
pub fn parse_addrs(msgs: &[(u16, Vec<u8>)], index: u32) -> Vec<(Ipv4Addr, u8)> {
    let mut out = Vec::new();
    for (ty, p) in msgs {
        if *ty != RTM_NEWADDR || p.len() < 8 || p[0] != libc::AF_INET as u8 || u32_at(p, 4) != index {
            continue;
        }
        let (mut local, mut address) = (None, None);
        for (t, d) in attrs(&p[8..]) {
            if d.len() == 4 {
                let a = Ipv4Addr::new(d[0], d[1], d[2], d[3]);
                match t {
                    IFA_LOCAL => local = Some(a),
                    IFA_ADDRESS => address = Some(a),
                    _ => {}
                }
            }
        }
        if let Some(a) = local.or(address) {
            out.push((a, p[1]));
        }
    }
    out
}

/// An IPv6 address on an interface, as rtnetlink lists it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Addr6 {
    pub addr: Ipv6Addr,
    pub prefix_len: u8,
    /// `IFA_F_*` (the `IFA_FLAGS` attribute when present, else the header's).
    pub flags: u32,
}

/// Interface `index`'s IPv6 addresses, in order, among RTM_NEWADDR messages.
pub fn parse_addrs6(msgs: &[(u16, Vec<u8>)], index: u32) -> Vec<Addr6> {
    let mut out = Vec::new();
    for (ty, p) in msgs {
        if *ty != RTM_NEWADDR || p.len() < 8 || p[0] != libc::AF_INET6 as u8 || u32_at(p, 4) != index {
            continue;
        }
        let (mut local, mut address, mut flags) = (None, None, p[2] as u32);
        for (t, d) in attrs(&p[8..]) {
            match (t, d.len()) {
                (IFA_LOCAL, 16) => local = Some(Ipv6Addr::from(<[u8; 16]>::try_from(d).unwrap_or_default())),
                (IFA_ADDRESS, 16) => address = Some(Ipv6Addr::from(<[u8; 16]>::try_from(d).unwrap_or_default())),
                (IFA_FLAGS, 4) => flags = u32_at(d, 0),
                _ => {}
            }
        }
        if let Some(addr) = local.or(address) {
            out.push(Addr6 { addr, prefix_len: p[1], flags });
        }
    }
    out
}

/// A NETLINK_ROUTE socket.
struct Nl {
    fd: OwnedFd,
}

impl Nl {
    fn open() -> io::Result<Nl> {
        // SAFETY: plain syscalls; the fd is owned from here on.
        unsafe {
            let fd = libc::socket(libc::AF_NETLINK, libc::SOCK_RAW | libc::SOCK_CLOEXEC, libc::NETLINK_ROUTE);
            if fd < 0 {
                return Err(io::Error::last_os_error());
            }
            let fd = OwnedFd::from_raw_fd(fd);
            let mut sa: libc::sockaddr_nl = std::mem::zeroed();
            sa.nl_family = libc::AF_NETLINK as u16;
            let r = libc::bind(fd.as_raw_fd(), &sa as *const _ as *const libc::sockaddr, std::mem::size_of::<libc::sockaddr_nl>() as u32);
            if r < 0 {
                return Err(io::Error::last_os_error());
            }
            let tv = libc::timeval { tv_sec: 2, tv_usec: 0 };
            libc::setsockopt(
                fd.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_RCVTIMEO,
                &tv as *const _ as *const libc::c_void,
                std::mem::size_of::<libc::timeval>() as u32,
            );
            Ok(Nl { fd })
        }
    }

    /// Send one request and collect the replies: until NLMSG_DONE for a dump,
    /// or the ACK. A negative error in an NLMSG_ERROR is returned as that errno.
    fn request(&mut self, ty: u16, flags: u16, payload: &[u8]) -> io::Result<Vec<(u16, Vec<u8>)>> {
        let seq = 1;
        let msg = nl_msg(ty, flags, seq, payload);
        // SAFETY: msg is a valid buffer of its length.
        let n = unsafe { libc::send(self.fd.as_raw_fd(), msg.as_ptr() as *const libc::c_void, msg.len(), 0) };
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
        let mut out = Vec::new();
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            // SAFETY: buf is a valid, writable buffer of its length.
            let n = unsafe { libc::recv(self.fd.as_raw_fd(), buf.as_mut_ptr() as *mut libc::c_void, buf.len(), 0) };
            if n < 0 {
                return Err(io::Error::last_os_error());
            }
            let mut b = &buf[..n as usize];
            while b.len() >= 16 {
                let len = u32_at(b, 0) as usize;
                if len < 16 || len > b.len() {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, "truncated netlink message"));
                }
                let (mty, mseq) = (u16_at(b, 4), u32_at(b, 8));
                let body = &b[16..len];
                b = &b[align(len).min(b.len())..];
                if mseq != seq {
                    continue;
                }
                match mty {
                    NLMSG_DONE => return Ok(out),
                    NLMSG_ERROR => {
                        let err = if body.len() >= 4 { i32::from_ne_bytes([body[0], body[1], body[2], body[3]]) } else { 0 };
                        return if err == 0 { Ok(out) } else { Err(io::Error::from_raw_os_error(-err)) };
                    }
                    _ => out.push((mty, body.to_vec())),
                }
            }
        }
    }
}

// ── gratuitous ARP ─────────────────────────────────────────────────────────

const ETH_P_ARP: u16 = 0x0806;

/// An unsolicited ARP reply announcing `ip` at `mac` (what `arping -A` sends):
/// sender and target are both the VIP, so every neighbour updates its cache.
pub fn garp_frame(mac: [u8; 6], ip: Ipv4Addr) -> [u8; 28] {
    let mut f = [0u8; 28];
    f[0..2].copy_from_slice(&1u16.to_be_bytes()); // Ethernet
    f[2..4].copy_from_slice(&0x0800u16.to_be_bytes()); // IPv4
    f[4] = 6;
    f[5] = 4;
    f[6..8].copy_from_slice(&2u16.to_be_bytes()); // reply
    f[8..14].copy_from_slice(&mac);
    f[14..18].copy_from_slice(&ip.octets());
    f[18..24].copy_from_slice(&mac);
    f[24..28].copy_from_slice(&ip.octets());
    f
}

/// Broadcast one gratuitous ARP on interface `index` (the kernel adds the
/// Ethernet header).
fn send_garp(index: u32, mac: [u8; 6], ip: Ipv4Addr) -> io::Result<()> {
    let frame = garp_frame(mac, ip);
    // SAFETY: plain syscalls on an fd owned here; sll and frame are valid.
    unsafe {
        let fd = libc::socket(libc::AF_PACKET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, (ETH_P_ARP.to_be()) as i32);
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        let fd = OwnedFd::from_raw_fd(fd);
        let mut sll: libc::sockaddr_ll = std::mem::zeroed();
        sll.sll_family = libc::AF_PACKET as u16;
        sll.sll_protocol = ETH_P_ARP.to_be();
        sll.sll_ifindex = index as i32;
        sll.sll_halen = 6;
        sll.sll_addr[..6].copy_from_slice(&[0xff; 6]);
        let n = libc::sendto(
            fd.as_raw_fd(),
            frame.as_ptr() as *const libc::c_void,
            frame.len(),
            0,
            &sll as *const _ as *const libc::sockaddr,
            std::mem::size_of::<libc::sockaddr_ll>() as u32,
        );
        if n < 0 {
            return Err(io::Error::last_os_error());
        }
    }
    Ok(())
}

// ── unsolicited Neighbor Advertisement (IPv6) ──────────────────────────────

/// All-nodes link-local multicast, where an unsolicited NA goes (RFC 4861).
const ALL_NODES: Ipv6Addr = Ipv6Addr::new(0xff02, 0, 0, 0, 0, 0, 0, 1);

/// An unsolicited Neighbor Advertisement for `target` at `mac` (RFC 4861
/// §4.4, §7.2.6): ICMPv6 type 136, the Override flag (not Solicited, not
/// Router), and the target link-layer address option, so every neighbour
/// that has an entry for the VIP moves it to this MAC. The checksum is left
/// zero: the kernel fills it on a raw ICMPv6 socket.
pub fn na_packet(mac: [u8; 6], target: Ipv6Addr) -> [u8; 32] {
    let mut p = [0u8; 32];
    p[0] = 136; // Neighbor Advertisement
    p[4] = 0x20; // R=0, S=0, O=1
    p[8..24].copy_from_slice(&target.octets());
    p[24] = 2; // Target Link-Layer Address
    p[25] = 1; // length, in 8-octet units
    p[26..32].copy_from_slice(&mac);
    p
}

/// Send one unsolicited NA for `target` to ff02::1 on interface `index`, hop
/// limit 255 (receivers drop any other, RFC 4861 §7.1.2), from the
/// interface's link-local address (the kernel's choice for ff02::1).
fn send_na(index: u32, mac: [u8; 6], target: Ipv6Addr) -> io::Result<()> {
    use socket2::{Domain, Protocol, SockAddr, Socket, Type};
    let sock = Socket::new(Domain::IPV6, Type::RAW, Some(Protocol::ICMPV6))?;
    sock.set_multicast_if_v6(index)?;
    sock.set_multicast_hops_v6(255)?;
    sock.set_multicast_loop_v6(false)?;
    let dst = SockAddr::from(SocketAddrV6::new(ALL_NODES, 0, 0, index));
    sock.send_to(&na_packet(mac, target), &dst).map(|_| ())
}

#[cfg(test)]
pub mod mock {
    use super::*;
    use std::sync::Mutex;

    /// Records claim/release calls for tests.
    #[derive(Default)]
    pub struct MockVip {
        pub events: Mutex<Vec<String>>,
    }
    impl VipController for MockVip {
        fn claim(&self, vip: &str, iface: &str) -> Result<()> {
            self.events.lock().unwrap().push(format!("claim {vip} {iface}"));
            Ok(())
        }
        fn release(&self, vip: &str, iface: &str) -> Result<()> {
            self.events
                .lock()
                .unwrap()
                .push(format!("release {vip} {iface}"));
            Ok(())
        }
    }

    #[test]
    fn mock_records_claim_and_release() {
        let m = MockVip::default();
        m.claim("192.168.8.50", "eth0").unwrap();
        m.release("192.168.8.50", "eth0").unwrap();
        let ev = m.events.lock().unwrap().clone();
        assert_eq!(ev, ["claim 192.168.8.50 eth0", "release 192.168.8.50 eth0"]);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_address_request_is_a_32_with_local_and_address() {
        let p = addr_payload(7, Ipv4Addr::new(192, 168, 8, 50));
        assert_eq!(&p[..4], &[libc::AF_INET as u8, 32, 0, 0]);
        assert_eq!(u32_at(&p, 4), 7);
        let a = attrs(&p[8..]);
        assert_eq!(a, vec![(IFA_LOCAL, &[192, 168, 8, 50][..]), (IFA_ADDRESS, &[192, 168, 8, 50][..])]);
        let m = nl_msg(RTM_NEWADDR, NLM_F_CREATE | NLM_F_EXCL | NLM_F_ACK, 1, &p);
        assert_eq!(u32_at(&m, 0) as usize, 16 + p.len());
        assert_eq!(u16_at(&m, 4), RTM_NEWADDR);
        assert_eq!(u16_at(&m, 6), NLM_F_REQUEST | NLM_F_CREATE | NLM_F_EXCL | NLM_F_ACK);
        assert_eq!(m.len() % 4, 0);
    }

    #[test]
    fn an_ipv6_address_request_is_a_128_without_dad() {
        let ip: Ipv6Addr = "fd09::100".parse().unwrap();
        let p = addr_payload6(7, ip);
        assert_eq!(&p[..4], &[libc::AF_INET6 as u8, 128, IFA_F_NODAD, 0]);
        assert_eq!(u32_at(&p, 4), 7);
        assert_eq!(attrs(&p[8..]), vec![(IFA_ADDRESS, &ip.octets()[..])]);
        assert_eq!(p.len() % 4, 0);
    }

    #[test]
    fn ipv6_addresses_parse_with_their_flags() {
        let msg = |idx: u32, ip: &str, len: u8, hdr_flags: u8, attr_flags: Option<u32>| {
            let mut p = addr_payload6(idx, ip.parse().unwrap());
            p[1] = len;
            p[2] = hdr_flags;
            if let Some(f) = attr_flags {
                p.extend_from_slice(&8u16.to_ne_bytes());
                p.extend_from_slice(&IFA_FLAGS.to_ne_bytes());
                p.extend_from_slice(&f.to_ne_bytes());
            }
            (RTM_NEWADDR, p)
        };
        let msgs = vec![
            msg(3, "fd09::1", 64, 0, None),
            msg(3, "fe80::1", 64, 0x40, Some(0x40)),
            msg(4, "fe80::4", 64, 0, None),
            msg(3, "fe80::2", 64, 0, Some(0x80)),
            // An IPv4 address on the same interface is not listed.
            (RTM_NEWADDR, addr_payload(3, Ipv4Addr::new(10, 0, 0, 1))),
        ];
        let a = parse_addrs6(&msgs, 3);
        assert_eq!(a.len(), 3);
        assert_eq!(a[0], Addr6 { addr: "fd09::1".parse().unwrap(), prefix_len: 64, flags: 0 });
        assert_eq!(a[1].flags, IFA_F_TENTATIVE, "tentative: skipped as a VRRP source");
        assert_eq!(a[2], Addr6 { addr: "fe80::2".parse().unwrap(), prefix_len: 64, flags: 0x80 });
        assert!(is_link_local(&a[1].addr) && is_link_local(&"febf::1".parse().unwrap()));
        assert!(!is_link_local(&a[0].addr) && !is_link_local(&Ipv6Addr::LOCALHOST));
    }

    #[test]
    fn the_neighbor_advertisement_overrides_with_our_mac() {
        let target: Ipv6Addr = "fd09::100".parse().unwrap();
        let p = na_packet([2, 0, 0, 0, 0, 9], target);
        assert_eq!(p[0], 136);
        assert_eq!(p[1], 0);
        assert_eq!(p[4], 0x20, "override only: unsolicited, not a router");
        assert_eq!(&p[8..24], &target.octets());
        assert_eq!(&p[24..32], &[2, 1, 2, 0, 0, 0, 0, 9]);
    }

    #[test]
    fn links_and_addresses_parse_from_messages() {
        // RTM_NEWLINK for index 3 "eth0" with a MAC.
        let mut l = vec![0u8; 16];
        l[4..8].copy_from_slice(&3u32.to_ne_bytes());
        for (t, d) in [(IFLA_IFNAME, b"eth0\0".to_vec()), (IFLA_ADDRESS, vec![2, 0, 0, 0, 0, 9])] {
            let len = 4 + d.len();
            l.extend_from_slice(&(len as u16).to_ne_bytes());
            l.extend_from_slice(&t.to_ne_bytes());
            l.extend_from_slice(&d);
            l.resize(align(l.len()), 0);
        }
        let msgs = vec![(RTM_NEWLINK, l)];
        assert_eq!(parse_links(&msgs, "eth0"), Some(Link { index: 3, mac: [2, 0, 0, 0, 0, 9] }));
        assert_eq!(parse_links(&msgs, "eth"), None, "a prefix is not a match");
        // Two addresses on 3 (a /32 VIP listed first), one on 4.
        let addr = |idx: u32, ip: [u8; 4], len: u8| {
            let mut p = addr_payload(idx, Ipv4Addr::from(ip));
            p[1] = len;
            (RTM_NEWADDR, p)
        };
        let msgs = vec![addr(3, [10, 0, 0, 50], 32), addr(4, [10, 1, 0, 1], 24), addr(3, [10, 0, 0, 5], 24)];
        assert_eq!(parse_addrs(&msgs, 3), vec![(Ipv4Addr::new(10, 0, 0, 50), 32), (Ipv4Addr::new(10, 0, 0, 5), 24)]);
    }

    #[test]
    fn the_gratuitous_arp_is_a_reply_from_and_to_the_vip() {
        let f = garp_frame([2, 0, 0, 0, 0, 9], Ipv4Addr::new(192, 168, 8, 50));
        assert_eq!(&f[..8], &[0, 1, 8, 0, 6, 4, 0, 2]);
        assert_eq!(&f[8..14], &[2, 0, 0, 0, 0, 9]);
        assert_eq!(&f[14..18], &[192, 168, 8, 50]);
        assert_eq!(&f[24..28], &[192, 168, 8, 50]);
    }

    /// The real kernel, unprivileged: every Linux network namespace has `lo`.
    #[test]
    fn loopback_is_found_by_netlink() {
        let l = link("lo").unwrap();
        assert_eq!(l.mac, [0; 6]);
        assert_eq!(interface_ipv4("lo").unwrap(), Ipv4Addr::LOCALHOST);
        // `lo` has ::1 (or no IPv6 at all), never a link-local.
        assert!(interface_ipv6_link_local("lo").is_err());
        assert!(link("no-such-if0").is_err());
    }
}
