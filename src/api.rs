//! The VIP API (`[api]`): create, change, read and remove named VIPs at
//! runtime, for stormcluster as the masters of a cluster change.
//!
//! - `GET /api/v1/vips` — every VIP, with its status.
//! - `GET /api/v1/vips/{name}` — one VIP: its spec and status (each backend's
//!   health, where it listens, VRRP state).
//! - `PUT /api/v1/vips/{name}` — create (201) or replace (200) it from a
//!   [`VipSpec`]; the answer is the VIP as `GET` shows it.
//! - `DELETE /api/v1/vips/{name}` — remove it (200).
//! - `GET /healthz` — `ok`, never authenticated.
//!
//! Errors are `{"error": "…"}` with 400 (a bad request), 401 (no or wrong
//! token), 404, 405, 409 (a listener in use; the config file's VIP), 413, or
//! 500 (applied but not saved). Plain HTTP/1.1, one request per connection.
//! A `token_file` makes every `/api/v1` request carry `Authorization: Bearer
//! <token>`; without one the API must listen on loopback.

use crate::config::ApiCfg;
use crate::vips::{ApplyError, Registry, VipSpec};
use anyhow::Context;
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tracing::{debug, info, warn};

/// The most a request head may be.
const MAX_HEAD: usize = 16 * 1024;
/// The most a request body may be.
const MAX_BODY: usize = 1024 * 1024;

/// Check the config, bind `listen` and serve forever.
pub async fn run(cfg: ApiCfg, reg: Arc<Registry>) -> anyhow::Result<()> {
    let listen: SocketAddr = cfg.listen.parse().with_context(|| format!("[api] listen {:?}: want ip:port", cfg.listen))?;
    if cfg.token_file.is_none() && !listen.ip().is_loopback() {
        anyhow::bail!("[api] listen {listen} is not loopback, so it needs token_file: anyone who can reach it could point a VIP anywhere");
    }
    let l = TcpListener::bind(listen).await.with_context(|| format!("binding the VIP API on {listen}"))?;
    info!("VIP API listening on {listen}{}", if cfg.token_file.is_some() { " (bearer token)" } else { "" });
    serve(l, reg, cfg.token_file).await
}

/// Serve on an already-bound listener.
pub async fn serve(l: TcpListener, reg: Arc<Registry>, token_file: Option<String>) -> anyhow::Result<()> {
    let token_file = Arc::new(token_file);
    loop {
        let (conn, peer) = match l.accept().await {
            Ok(x) => x,
            Err(e) => {
                warn!("VIP API accept: {e}");
                continue;
            }
        };
        let (reg, tf) = (reg.clone(), token_file.clone());
        tokio::spawn(async move {
            if let Err(e) = tokio::time::timeout(Duration::from_secs(30), conn_task(conn, &reg, tf.as_deref())).await {
                debug!("VIP API {peer}: {e}");
            }
        });
    }
}

async fn conn_task(mut conn: TcpStream, reg: &Registry, token_file: Option<&str>) {
    let (status, body) = match read_request(&mut conn).await {
        Ok(req) => handle(reg, token_file, &req),
        Err((status, msg)) => (status, json!({ "error": msg })),
    };
    let text = body.to_string() + "\n";
    let head = format!(
        "HTTP/1.1 {status} {}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        reason(status),
        text.len()
    );
    let _ = conn.write_all(head.as_bytes()).await;
    let _ = conn.write_all(text.as_bytes()).await;
    let _ = conn.shutdown().await;
}

/// A parsed request.
pub struct Request {
    pub method: String,
    pub path: String,
    pub authorization: Option<String>,
    pub body: Vec<u8>,
}

async fn read_request(conn: &mut TcpStream) -> Result<Request, (u16, String)> {
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = vec![0u8; 4096];
    let head_end = loop {
        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break i + 4;
        }
        if buf.len() > MAX_HEAD {
            return Err((431, format!("request head over {MAX_HEAD} bytes")));
        }
        let n = conn.read(&mut chunk).await.map_err(|e| (400, e.to_string()))?;
        if n == 0 {
            return Err((400, "connection closed before the request head ended".into()));
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = std::str::from_utf8(&buf[..head_end]).map_err(|_| (400, "request head is not UTF-8".to_string()))?;
    let mut lines = head.split("\r\n");
    let mut first = lines.next().unwrap_or("").split(' ');
    let (method, path) = match (first.next(), first.next()) {
        (Some(m), Some(p)) if !m.is_empty() => (m.to_string(), p.to_string()),
        _ => return Err((400, "no request line".into())),
    };
    let (mut length, mut authorization) = (0usize, None);
    for l in lines {
        let Some((k, v)) = l.split_once(':') else { continue };
        let v = v.trim();
        if k.eq_ignore_ascii_case("content-length") {
            length = v.parse().map_err(|_| (400, format!("content-length {v:?}")))?;
        } else if k.eq_ignore_ascii_case("authorization") {
            authorization = Some(v.to_string());
        } else if k.eq_ignore_ascii_case("transfer-encoding") {
            return Err((411, "send a content-length, not chunked".into()));
        }
    }
    if length > MAX_BODY {
        return Err((413, format!("body over {MAX_BODY} bytes")));
    }
    let mut body = buf[head_end..].to_vec();
    while body.len() < length {
        let n = conn.read(&mut chunk).await.map_err(|e| (400, e.to_string()))?;
        if n == 0 {
            return Err((400, "connection closed before the body ended".into()));
        }
        body.extend_from_slice(&chunk[..n]);
    }
    body.truncate(length);
    Ok(Request { method, path, authorization, body })
}

/// Answer one request: `(status, JSON body)`.
pub fn handle(reg: &Registry, token_file: Option<&str>, req: &Request) -> (u16, Value) {
    let path = req.path.split('?').next().unwrap_or("");
    if path == "/healthz" {
        return if req.method == "GET" { (200, json!({ "status": "ok" })) } else { method_not_allowed() };
    }
    let Some(rest) = path.strip_prefix("/api/v1/vips") else {
        return (404, json!({ "error": format!("no such path {path}") }));
    };
    if let Some(tf) = token_file {
        if let Err((s, m)) = authorize(tf, req.authorization.as_deref()) {
            return (s, json!({ "error": m }));
        }
    }
    match (req.method.as_str(), rest) {
        ("GET", "" | "/") => (200, json!({ "vips": reg.list() })),
        (_, "" | "/") => method_not_allowed(),
        (m, name) => {
            let name = &name[1..];
            if name.contains('/') || name.is_empty() {
                return (404, json!({ "error": format!("no such path {path}") }));
            }
            match m {
                "GET" => match reg.get(name) {
                    Some(v) => (200, json!(v)),
                    None => (404, json!({ "error": format!("no VIP {name}") })),
                },
                "PUT" => {
                    let spec: VipSpec = match serde_json::from_slice(&req.body) {
                        Ok(s) => s,
                        Err(e) => return (400, json!({ "error": format!("body: {e}") })),
                    };
                    match reg.apply(name, spec) {
                        Ok(created) => (if created { 201 } else { 200 }, json!(reg.get(name))),
                        Err(e) => error(e),
                    }
                }
                "DELETE" => match reg.remove(name) {
                    Ok(()) => (200, json!({ "deleted": name })),
                    Err(e) => error(e),
                },
                _ => method_not_allowed(),
            }
        }
    }
}

fn error(e: ApplyError) -> (u16, Value) {
    let s = match e {
        ApplyError::Invalid(_) => 400,
        ApplyError::NotFound(_) => 404,
        ApplyError::Conflict(_) => 409,
        ApplyError::NotSaved(_) => 500,
    };
    (s, json!({ "error": e.to_string() }))
}

fn method_not_allowed() -> (u16, Value) {
    (405, json!({ "error": "method not allowed" }))
}

/// The token is read on every request, so it can be rotated in place.
fn authorize(token_file: &str, header: Option<&str>) -> Result<(), (u16, String)> {
    let want = std::fs::read_to_string(token_file).map_err(|e| {
        warn!("VIP API: token_file {token_file}: {e}");
        (500, "the API's token file cannot be read".to_string())
    })?;
    let want = want.trim();
    if want.is_empty() {
        warn!("VIP API: token_file {token_file} is empty");
        return Err((500, "the API's token file is empty".into()));
    }
    let got = header.and_then(|h| h.strip_prefix("Bearer ")).map(str::trim).unwrap_or("");
    // Compare in time that does not depend on where they differ.
    let same = got.len() == want.len() && got.bytes().zip(want.bytes()).fold(0u8, |a, (x, y)| a | (x ^ y)) == 0;
    if same {
        Ok(())
    } else {
        Err((401, "missing or wrong bearer token".into()))
    }
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        201 => "Created",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        405 => "Method Not Allowed",
        409 => "Conflict",
        411 => "Length Required",
        413 => "Payload Too Large",
        431 => "Request Header Fields Too Large",
        _ => "Internal Server Error",
    }
}
