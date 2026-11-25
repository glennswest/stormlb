# stormlb design notes

Why stormlb is shaped the way it is. The README describes what the code does.
Where this document describes something the code does **not** do yet, it says
so and links the issue.

## Why a pre-cluster VIP at all

The kube-apiserver needs one stable address before the cluster exists.
Kubelets, the other masters and `oc`/`kubectl` all need it. Cilium can't
provide it: it runs as a workload scheduled by the apiserver it would be
fronting. OpenShift on-prem solves this with `keepalived` + `haproxy` as host
services, and stormlb is that pair in one Rust binary.

The scope split follows from that. stormlb owns the control-plane VIP.
Cilium owns in-cluster Service traffic (eBPF LB, LB-IPAM, BGP / L2
announcements).

**Status:** implemented (`[vip]`, `[[backend]]`, `[health]`, `[vrrp]`,
`[bgp]`, and the VIP API `[api]`). The stormcos golden runs the router and
the API, with no VIP until stormcluster makes one. On a single node the VIP
is the node's own address, so nothing needs to float.

## A VIP whose backends change at runtime (#16)

When stormcluster forms a cluster from SNO nodes, kubelets and controllers
talk to one API endpoint that survives any single master going down: a VIP
in front of every master's `:6443` (owner's decision, stormcos#47,
2026-10-02). The set of masters changes while the cluster runs (form, join
as master, promote, demote, split), so the backends can't be fixed at start.

**API, not reload.** stormcluster needs to read the current backends before
it acts, and to know whether a change took, so it gets an HTTP API
(`PUT`/`GET`/`DELETE /api/v1/vips/{name}`) rather than a SIGHUP reload of the
TOML. Each VIP is named, so one process can serve more than one, and each
carries its own listener, pool, health spec and (optionally) VRRP instance.
The TOML `[vip]` is the VIP `default`, read-only through the API, so a file
and an API never fight over one VIP.

**A change never takes the VIP down.** A spec is validated, and any new
listener bound, before the running VIP is touched; a refusal leaves it as it
was. A backend that stays keeps its health (re-applying the same masters is
free), a new one is probed at once, and connections already proxied are
never cut. stormcluster can therefore apply its whole desired state each
time, idempotently.

**L4 pass-through.** TLS terminates on the apiservers, whose serving
certificate lists the VIP among its SANs, so stormlb never holds a key.
Health is `GET /readyz` over https, verified against the cluster CA
(`health.ca_file`), so a backend counts only when it is a real apiserver of
this cluster.

**Binding.** A VIP's listener binds the VIP address with `IP_FREEBIND`, so
every master listens before it holds the address, and a VRRP takeover needs
no rebind. It can't share `:6443` with the apiserver's wildcard bind on the
same node, which is why stormcluster puts the cluster's VIP on its
`endpoint.port` (default 7443, stormcluster#35) and fronts each master's `:6443`.

**Persistence and access.** VIPs made through the API are saved to
`state_file` and served again at start, so a stormlb restart doesn't wait for
stormcluster to notice. A change and its save happen under one lock, so the
file never lags what is served (#44). A VRRP instance that is replaced or
removed finishes (releasing the address) before its successor for the same
address starts, waited for on the successor's own thread (#44). The API is loopback by default: stormcluster runs on
every node and calls its own stormlb. Anywhere else, it needs a bearer
token.

**Moving between masters (#7).** Each VIP's VRRP instance follows RFC 5798
preemption (the highest-priority healthy master holds it), resigns with
priority 0 when it stops or loses every healthy backend, and claims the
address over rtnetlink with a raw gratuitous ARP, so the golden needs no
`ip` or `arping`. An instance listens only to advertisements for its own
VRID **and** VIP, since one link may carry several groups and VRIDs get
reused. Its Master state is only ever what it holds: a claim that fails
puts it back to Backup and it retries with a backoff (#45).

## L2 vs L3

| | L2 (VRRP) | L3 (BGP anycast) |
|---|---|---|
| VIP owner | one node at a time | every healthy node |
| Failover | master-down timer (3 × advert + skew; 3.6 s at the defaults) | route withdraw |
| Ingress | active-passive | active-active (router ECMP) |
| Fabric needs | a plain L2 segment | BGP peering on the upstream router |

Both balance the backend (the masters) the same way, through the
health-checked L4 proxy. The only difference is how the VIP is presented to
the network.

**What the code does not do yet:**

- The VRRP master-down time is 3.6 s at a 1 s advert interval. That's the RFC
  formula, not the "sub-second" earlier docs claimed. Going sub-second needs
  a sub-second advert interval, and the config only takes whole seconds.
- BGP route-withdraw failover is as fast as the health check plus 250 ms,
  now that announce and withdraw follow health as it changes rather than the
  60 s keepalive tick (#6). It also needs the session Established and
  enforces the negotiated hold time, so a dead peer is noticed within that
  time, not only when TCP fails.
- BGP offers the 4-octet AS (RFC 6793) and Multiprotocol (RFC 4760)
  capabilities (#40): any ASN but the reserved ones (0, 23456, 4294967295),
  AS_TRANS + AS4_PATH toward an old 2-octet peer, and an IPv6 VIP announced
  as a /128 in MP_REACH_NLRI with `next_hop` (this node's global IPv6). It
  originates one unicast host route; no other family, no route reflection,
  no add-path.
- VRRP owns IPv4 and IPv6 VIPs (#39: ff02::12, link-local sources,
  unsolicited Neighbor Advertisements). With no virtual MAC there is no
  virtual link-local either, so an IPv6 advertisement lists the VIP itself
  where RFC 5798 §5.2.9 wants the virtual router's link-local first: fine
  between stormlb peers, possibly not with a third-party VRRPv3 router.

**VRRP ownership follows health, not just liveness** (#7). Plain VRRP moves
the VIP only when its holder stops advertising. A holder whose backends are
all down would keep the VIP and drop every connection. So a Master with no
healthy backend resigns (priority 0, so a Backup takes over after Skew_Time,
well under a second), and a Backup with none never takes over. On the API
VIP every master checks the same set of apiservers, so this matters when a
node is cut off from the others, not when one apiserver fails. Preemption
(RFC default, `preempt = true`) gives the VIP back to the highest-priority
healthy node when it returns.

## stormlb vs a DNS load balancer

A health-checked, short-TTL DNS LB is a valid approach and can coexist. The
DNS name can point at the stormlb VIP, or stormlb can be the tighter-failover
alternative: one stable address, no dependency on resolver caches. Nothing in
stormlb depends on DNS. Backends are IP literals.

## Why the L7 router lives here

stormpump `docs/routing.md` describes inbound as: wildcard DNS
`*.storm1.<zone>` goes to a VIP, and something at the VIP demuxes on the Host
header. stormlb already owns the VIP, the health checks and the L4 path, so
the demux is here too. One component owns one request path. The alternative,
where the VIP is ours and the L7 hop is Cilium/Envoy's, gives two failure
domains and no single place to debug.

Choices that follow from the code (`src/router.rs`):

- **Per request (#26, #37).** The router is an HTTP/1.1 proxy (hyper on
  both sides), not a splice after the first head. The storefront
  (stormstorefront#1) routes `/` to web and `/api/` to api on one hostname,
  and a browser sends both on one keep-alive connection, so the backend has
  to be picked per request. Upgrades (websockets) are still a splice, after
  the backend's 101. Bodies stream, and backend connections are kept alive
  and reused.
- **The rules are the backend config (#37).** Hostnames (exact, then
  wildcard), matches (path Exact/PathPrefix, method, headers and query
  Exact) with the Gateway API's precedence, weighted backendRefs, timeouts
  and retries. Each backendRef is a pool of the Service's endpoints, from
  EndpointSlices (else Endpoints, else the clusterIP), dialled directly. A
  ClusterIP in between would hide the pods' health from the router and
  spread the requests a second time. What a rule asks for and the router
  cannot do (filters, RegularExpression matches, session persistence) is
  dropped and said in the route's status, not silently ignored.
- **Health is three things.** Readiness from the endpoint objects; an
  optional active check per Service (`storm.io/health-check-*`, with
  rise/fall); and passive ejection, where a refused connection takes the
  endpoint out for 5 s and the request moves on to another endpoint. The
  passive part is what makes "kill a pod, lose no request" hold between
  polls. It also covers an apiserver whose readiness is coarse (rustkube#347
  marks every Running pod ready).
- **Polled, not watched.** A route change is an operator action, so 5 s of
  staleness is invisible. A poll also can't be wedged by a watch
  implementation bug in a reimplemented apiserver. The last good table
  survives apiserver errors. A poll is four cluster-wide lists (routes,
  Services, EndpointSlices, Endpoints), not one request per route.
  Endpoint state (health, ejection, in-flight count) is kept across
  refreshes.
- **Status is written, not just logged.** Each poll merge-patches the
  route's `status.parents` with its own entry (`storm.io/stormlb`):
  Accepted, ResolvedRefs, and `storm.io/BackendsHealthy` with each
  backend's healthy/total. It writes only on change, keeps other
  controllers' entries, and routing never waits on it. Routes here name no
  Gateway, so the parent is the router itself (`storm.io/Router stormlb`).
- **LoadBalancer Services are not here.** The owner put them on flowsdn
  (stormlb#37, "you need a flowsdn solution"; flowsdn#475): the address,
  `status.loadBalancer`, the announcement and `externalTrafficPolicy: Local`
  belong to the CNI's datapath. The router is the L7 half behind it.
- **`storm.io/backend` before rules.** A node service is
  `127.0.0.1:<port>` on every node, so cluster manifests carry no node
  addresses.
- **`auto` listen.** It binds the node's routable address and loopback, not
  the wildcard, because stormimds holds `169.254.169.254:80`. Both must bind
  or the bind fails, naming the address (#43): a router bound on loopback
  alone passes its health probe and serves nobody.

**Cross-namespace refs need a ReferenceGrant (#42).** A route in one
namespace may name a Service in another only when the target namespace's
owner says so with a ReferenceGrant (from HTTPRoute in the route's
namespace, to Service). Without one the ref is `RefNotPermitted` and its
share answers 500. Grants are listed only when such a ref exists; when they
cannot be read the refs are refused (fail closed) and the rest of the table
still updates.

**OpenShift Routes are HTTPRoutes with another shape (#48).** `oc` finds
Prometheus through the Route `openshift-monitoring/prometheus-k8s`'s
`status.ingress[].host` (stormcos#1491), and rustkube serves Routes, so the
router reads them too and builds them into the same table rather than
running a second router: a Route is one rule (host, path prefix, weighted
Services or `storm.io/backend`). What is OpenShift's own is kept where a
client can see it: the default host `<name>-<namespace>.<domain>` (the
domain from `route_domain`, else the router certificate's wildcard, so a
node needs no new setting), the oldest Route holding a host,
`insecureEdgeTerminationPolicy` on `:80`, and `status.ingress` with
`routerName: stormlb` and `Admitted`. TLS uses the router's certificate:
edge and reencrypt terminate with it (reencrypt verifies the backend as
`<svc>.<ns>.svc` against the cluster CA, as OpenShift's service-serving
certificates are named), and passthrough is decided before any TLS is
terminated, by peeking the ClientHello's SNI, then spliced. A Route's own
inline certificate is not served; its `Admitted` message says so instead of
pretending. Routes are listed apart from HTTPRoutes: a refused Routes list
(an RBAC rule not shipped yet, stormcos#1632) keeps the last one and never
stops the HTTPRoute table.

Deliberately not done (yet): HTTPRoute filters (header modifiers, redirects, rewrites, mirrors),
RegularExpression matches, session persistence, and HTTP/2.

**Metrics follow the requests (#12, #37).** Every request is counted by
the hostname it matched and its code, with the time to the response head.
Each route backend gets its own request counter and a healthy/total gauge,
read at scrape time. The `host` label is a route's hostname or `unrouted`,
never the client's Host header, so a scanner can't grow the series set.
Metrics have their own port (`:9111`), because `/metrics` on `:80` is a
routable path. The same listener serves `/api/v1/routes`, the table with
every endpoint's state, for the console.

TLS is now a requirement, not an option: stormcos#81 has every node listener
serve TLS with a stormcert certificate and authenticate. The router
terminates TLS (`[router.tls]`, #14). The certificate is a file pair, not
something the router mints or fetches: stormcert issues it, stormcos
mounts it (stormcos#363, on in the golden since #21), and the router
re-reads it when it changes. So
the router doesn't depend on how stormcert will deliver per-route
certificates (stormcert#1, gap 3): any number of pairs, chosen by SNI,
already works. `:80` redirects once a certificate is loaded, never before,
so a node without one keeps working over plain HTTP. `/healthz` stays plain
because the health probe is the one exemption #81 allows.

**TLS to backends is per route (#13).** A route opts in with
`storm.io/backend-protocol: https`, because a backend's scheme is a fact
about that backend, not about the router. The router trusts only
`backend_ca_file` (the cluster CA), never public roots, so a route can't
be pointed at something that merely has a valid web certificate. It
verifies each endpoint's IP, or a name the route gives. Every failure is the
router's 502 with nothing sent on: a request carrying a bearer token must
never reach a backend the router couldn't verify.

**Its own identity (#9, #10).** The owner chose a ServiceAccount of
stormlb's own, with only what it reads (stormcos#203): stormcos mints the
token and writes the RBAC, and the router reads the token from a file on
every poll, so rotation needs no restart. The apiserver is verified against
the cluster CA alone. Both are files the router is pointed at, not
credentials it mints, which keeps stormlb out of certificate issuance. A
missing token stops refreshes rather than falling back to anonymous, so a
broken mount shows as a stale table and a log line, not as a silent change
of identity. The golden names `storm-lb/stormlb`'s token, which the apiserver
golden mints to `/data/stormcert/stormlb.token` (stormcos#395, #33). The
router runs only where stormcos writes `start stormlb` (sno, bastion), which
are apiserver nodes, so the file is always minted locally. Because a missing
token is not a fallback, the image that names it ships only with an
apiserver image that mints it (`ships-with rustkube-apiserver`).
