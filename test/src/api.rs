//! The apiserver, as the suites use it: create, read, replace and delete
//! HTTPRoutes, Services and Endpoints in the run's namespace, every object
//! labelled `storm.io/test-run=<run id>`, and read the node's capacity.
//!
//! Authenticated with the Job's ServiceAccount token, and verified against
//! the mounted `ca.crt`. Without one (a hand run) it accepts the
//! apiserver's certificate unverified, as the router itself does today.

use std::time::Duration;

use reqwest::Method;
use serde_json::{json, Value};

use crate::env::Env;

#[derive(Clone)]
pub struct Api {
    base: String,
    http: reqwest::Client,
    token: Option<String>,
    pub ns: String,
    pub run: String,
}

impl Api {
    pub fn new(env: &Env) -> Result<Api, String> {
        if env.api.is_empty() {
            return Err("STORM_API is not set".into());
        }
        let mut b = reqwest::Client::builder().timeout(Duration::from_secs(20));
        b = match &env.ca {
            Some(pem) => b.add_root_certificate(reqwest::Certificate::from_pem(pem).map_err(|e| format!("ca.crt: {e}"))?),
            None => b.danger_accept_invalid_certs(true),
        };
        Ok(Api {
            base: env.api.trim_end_matches('/').to_string(),
            http: b.build().map_err(|e| format!("http client: {e}"))?,
            token: env.token.clone(),
            ns: env.namespace.clone(),
            run: env.run_id.clone(),
        })
    }

    async fn call(&self, m: Method, path: &str, body: Option<&Value>) -> Result<(u16, Value), String> {
        let mut rq = self.http.request(m.clone(), format!("{}{path}", self.base));
        if let Some(t) = &self.token {
            rq = rq.bearer_auth(t);
        }
        if let Some(b) = body {
            rq = rq.json(b);
        }
        let resp = rq.send().await.map_err(|e| format!("{m} {path}: {e}"))?;
        let st = resp.status().as_u16();
        let text = resp.text().await.unwrap_or_default();
        Ok((st, serde_json::from_str(&text).unwrap_or(Value::String(text))))
    }

    async fn ok(&self, m: Method, path: &str, body: Option<&Value>) -> Result<Value, String> {
        let (st, v) = self.call(m.clone(), path, body).await?;
        if (200..300).contains(&st) {
            Ok(v)
        } else {
            let msg = v["message"].as_str().map(str::to_string).unwrap_or_else(|| v.to_string());
            Err(format!("{m} {path}: {st} {}", &msg[..msg.len().min(300)]))
        }
    }

    pub async fn create(&self, coll: &str, obj: &Value) -> Result<Value, String> {
        self.ok(Method::POST, coll, Some(obj)).await
    }

    pub async fn get(&self, path: &str) -> Result<Option<Value>, String> {
        match self.call(Method::GET, path, None).await? {
            (404, _) => Ok(None),
            (st, v) if (200..300).contains(&st) => Ok(Some(v)),
            (st, v) => Err(format!("GET {path}: {st} {v}")),
        }
    }

    pub async fn put(&self, path: &str, obj: &Value) -> Result<Value, String> {
        self.ok(Method::PUT, path, Some(obj)).await
    }

    /// Delete; already gone is fine.
    pub async fn delete(&self, path: &str) -> Result<(), String> {
        match self.call(Method::DELETE, path, None).await? {
            (st, _) if st == 404 || (200..300).contains(&st) => Ok(()),
            (st, v) => Err(format!("DELETE {path}: {st} {v}")),
        }
    }

    /// This run's objects in a collection.
    pub async fn mine(&self, coll: &str) -> Result<Vec<Value>, String> {
        let sel = format!("storm.io%2Ftest-run%3D{}", self.run);
        let v = self.ok(Method::GET, &format!("{coll}?labelSelector={sel}"), None).await?;
        Ok(v["items"].as_array().cloned().unwrap_or_default())
    }

    pub fn routes(&self) -> String {
        format!("/apis/gateway.networking.k8s.io/v1/namespaces/{}/httproutes", self.ns)
    }
    pub fn services(&self) -> String {
        format!("/api/v1/namespaces/{}/services", self.ns)
    }
    pub fn endpoints(&self) -> String {
        format!("/api/v1/namespaces/{}/endpoints", self.ns)
    }

    pub fn meta(&self, name: &str) -> Value {
        json!({"name": name, "namespace": self.ns, "labels": {"storm.io/test-run": self.run}})
    }

    /// An HTTPRoute for `hosts`: to `backend` (`storm.io/backend`, the node
    /// service path) or to a Service (`backendRefs`, the cluster path).
    pub fn route(&self, name: &str, hosts: &[String], backend: Option<&str>, svc: Option<(&str, u16)>) -> Value {
        let mut meta = self.meta(name);
        if let Some(b) = backend {
            meta["annotations"] = json!({"storm.io/backend": b});
        }
        let rules = match svc {
            Some((s, p)) => json!([{"backendRefs": [{"name": s, "port": p}]}]),
            None => json!([]),
        };
        json!({
            "apiVersion": "gateway.networking.k8s.io/v1",
            "kind": "HTTPRoute",
            "metadata": meta,
            "spec": {"hostnames": hosts, "rules": rules},
        })
    }

    /// Delete everything this run made here. Returns how many objects.
    pub async fn cleanup(&self) -> Result<usize, String> {
        let mut n = 0;
        for coll in [self.routes(), self.endpoints(), self.services()] {
            for o in self.mine(&coll).await? {
                if let Some(name) = o["metadata"]["name"].as_str() {
                    self.delete(&format!("{coll}/{name}")).await?;
                    n += 1;
                }
            }
        }
        Ok(n)
    }

    /// Objects of this run still listed.
    pub async fn leftovers(&self) -> Result<usize, String> {
        let mut n = 0;
        for coll in [self.routes(), self.endpoints(), self.services()] {
            n += self.mine(&coll).await?.len();
        }
        Ok(n)
    }

    /// The node's allocatable CPUs, read from the API: the node whose name
    /// or one of whose addresses is `node`, else the only node.
    pub async fn node_cpus(&self, node: &str) -> Result<(u64, String), String> {
        let v = self.ok(Method::GET, "/api/v1/nodes", None).await?;
        let items = v["items"].as_array().cloned().unwrap_or_default();
        let is_it = |n: &Value| {
            n["metadata"]["name"].as_str() == Some(node)
                || n["status"]["addresses"].as_array().into_iter().flatten().any(|a| a["address"].as_str() == Some(node))
        };
        let n = match items.iter().find(|n| is_it(n)) {
            Some(n) => n,
            None if items.len() == 1 => &items[0],
            None => return Err(format!("no node named or addressed {node:?} among {}", items.len())),
        };
        let name = n["metadata"]["name"].as_str().unwrap_or("?").to_string();
        let cpu = n["status"]["allocatable"]["cpu"].as_str().ok_or_else(|| format!("node {name} has no allocatable cpu"))?;
        Ok((cpus(cpu).ok_or_else(|| format!("node {name}: allocatable cpu {cpu:?}"))?, name))
    }
}

/// A Kubernetes CPU quantity in whole CPUs, at least 1: `"8"`, `"7500m"`.
pub fn cpus(q: &str) -> Option<u64> {
    let n = match q.strip_suffix('m') {
        Some(m) => m.parse::<u64>().ok()? / 1000,
        None => q.parse::<f64>().ok()? as u64,
    };
    Some(n.max(1))
}

#[cfg(test)]
mod tests {
    #[test]
    fn cpu_quantities() {
        assert_eq!(super::cpus("8"), Some(8));
        assert_eq!(super::cpus("7500m"), Some(7));
        assert_eq!(super::cpus("250m"), Some(1));
        assert_eq!(super::cpus("x"), None);
    }
}
