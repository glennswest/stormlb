//! A raw HTTP/1.1 client, written for this: the suites check what the router
//! puts on the wire (line endings, a close with no response, bytes arriving
//! before the backend has finished), which a client library would smooth
//! over.

use std::io;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

/// Per read, write or connect.
pub const IO: Duration = Duration::from_secs(10);

pub struct Resp {
    pub status: u16,
    pub head: String,
    pub body: Vec<u8>,
    /// The head ended in CRLF CRLF (RFC 9112), not a bare LF LF.
    pub crlf: bool,
}

impl Resp {
    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }

    /// Which test backend answered (`backend=<name>` in its body).
    pub fn backend(&self) -> Option<String> {
        field(&self.text(), "backend")
    }
}

/// `key=value` from a test backend's body.
pub fn field(body: &str, key: &str) -> Option<String> {
    body.split_whitespace()
        .find_map(|w| w.strip_prefix(key).and_then(|r| r.strip_prefix('=')))
        .map(str::to_string)
}

pub struct Conn {
    s: TcpStream,
    pub buf: Vec<u8>,
}

fn timed_out(_: tokio::time::error::Elapsed) -> io::Error {
    io::Error::new(io::ErrorKind::TimedOut, "timed out")
}

impl Conn {
    pub async fn open(addr: &str) -> io::Result<Conn> {
        let s = timeout(IO, TcpStream::connect(addr)).await.map_err(timed_out)??;
        let _ = s.set_nodelay(true);
        Ok(Conn { s, buf: Vec::new() })
    }

    pub async fn send(&mut self, b: &[u8]) -> io::Result<()> {
        timeout(IO, self.s.write_all(b)).await.map_err(timed_out)?
    }

    /// Read what arrives within `wait` into `buf`. `Ok(0)` is the peer's EOF.
    pub async fn fill(&mut self, wait: Duration) -> io::Result<usize> {
        let mut tmp = [0u8; 16384];
        let n = timeout(wait, self.s.read(&mut tmp)).await.map_err(timed_out)??;
        self.buf.extend_from_slice(&tmp[..n]);
        Ok(n)
    }

    /// One response. `None` when the peer closed without sending a byte.
    /// The body is `content-length` bytes when there is one, else up to EOF.
    pub async fn response(&mut self) -> io::Result<Option<Resp>> {
        let (end, crlf) = loop {
            if let Some(e) = head_end(&self.buf) {
                break e;
            }
            match self.fill(IO).await {
                Ok(0) if self.buf.is_empty() => return Ok(None),
                Err(e) if e.kind() == io::ErrorKind::ConnectionReset && self.buf.is_empty() => return Ok(None),
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        format!("closed mid-head after {:?}", String::from_utf8_lossy(&self.buf)),
                    ))
                }
                Ok(_) => {}
                Err(e) => return Err(e),
            }
        };
        let rest = self.buf.split_off(end);
        let head = String::from_utf8_lossy(&std::mem::replace(&mut self.buf, rest)).into_owned();
        let status = head
            .split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, format!("no status in {head:?}")))?;
        let body = if status == 101 {
            Vec::new()
        } else if let Some(n) = header(&head, "content-length").and_then(|v| v.parse::<usize>().ok()) {
            while self.buf.len() < n {
                if self.fill(IO).await? == 0 {
                    break;
                }
            }
            let n = n.min(self.buf.len());
            self.buf.drain(..n).collect()
        } else {
            while self.fill(IO).await? != 0 {}
            std::mem::take(&mut self.buf)
        };
        Ok(Some(Resp { status, head, body, crlf }))
    }

    /// Wait for the peer to close. Returns whatever arrived first; a reset
    /// counts as closed.
    pub async fn closed(&mut self, wait: Duration) -> io::Result<Vec<u8>> {
        let end = tokio::time::Instant::now() + wait;
        loop {
            let left = end.saturating_duration_since(tokio::time::Instant::now());
            match self.fill(left).await {
                Ok(0) => return Ok(std::mem::take(&mut self.buf)),
                Ok(_) => {}
                Err(e) if matches!(e.kind(), io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe) => {
                    return Ok(std::mem::take(&mut self.buf))
                }
                Err(e) => return Err(e),
            }
        }
    }
}

/// Where the head ends, and whether it ended in CRLF CRLF: whichever of the
/// two terminators comes first.
pub fn head_end(buf: &[u8]) -> Option<(usize, bool)> {
    let crlf = buf.windows(4).position(|w| w == b"\r\n\r\n");
    let lf = buf.windows(2).position(|w| w == b"\n\n");
    match (crlf, lf) {
        (Some(c), Some(l)) if l < c + 2 => Some((l + 2, false)),
        (Some(c), _) => Some((c + 4, true)),
        (None, Some(l)) => Some((l + 2, false)),
        (None, None) => None,
    }
}

/// A header's value, case-insensitively.
pub fn header(head: &str, name: &str) -> Option<String> {
    head.lines().skip(1).find_map(|l| {
        let (k, v) = l.split_once(':')?;
        k.trim().eq_ignore_ascii_case(name).then(|| v.trim().to_string())
    })
}

/// `GET path` for `host`, closing after.
pub fn get(host: &str, path: &str) -> Vec<u8> {
    format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nUser-Agent: stormlb-test\r\nConnection: close\r\n\r\n").into_bytes()
}

/// One request on a new connection.
pub async fn fetch(addr: &str, host: &str, path: &str) -> io::Result<Option<Resp>> {
    let mut c = Conn::open(addr).await?;
    c.send(&get(host, path)).await?;
    c.response().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_head_ends_at_the_first_terminator() {
        assert_eq!(head_end(b"HTTP/1.1 200 OK\r\na: b\r\n\r\nbody\n\n"), Some((25, true)));
        assert_eq!(head_end(b"HTTP/1.1 200 OK\na: b\n\nbody\r\n\r\n"), Some((22, false)));
        assert_eq!(head_end(b"HTTP/1.1 200 OK\r\n"), None);
    }

    #[test]
    fn headers_and_fields_are_found() {
        let head = "HTTP/1.1 200 OK\r\nContent-Length: 12\r\n\r\n";
        assert_eq!(header(head, "content-length").as_deref(), Some("12"));
        assert_eq!(field("backend=a host=x req=2", "req").as_deref(), Some("2"));
        assert_eq!(field("backend=a", "host"), None);
    }
}
