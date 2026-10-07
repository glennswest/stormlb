# Changelog

## [Unreleased]

### 2026-10-07
- **feat:** the router's identity toward the apiserver (#9, #10).
  `[router] token_file` (its ServiceAccount's bearer token, owner's choice in
  stormcos#203) is sent on every apiserver request and re-read each poll; a
  missing or empty file stops refreshes and keeps the last table.
  `[router] ca_file` verifies the apiserver against that CA only (then
  `insecure` is ignored), rebuilt when the file changes. A 401/403 names
  what the router needs; refresh errors are logged once per distinct error.
- **docs:** README (`token_file`, `ca_file`, `insecure`, route table, gaps),
  design.md, deck, example.

- **feat:** TLS to backends (#13). A route annotated
  `storm.io/backend-protocol: https` is dialled over TLS (ALPN http/1.1) and
  verified against `[router] backend_ca_file` only, for the backend's IP or
  `storm.io/backend-server-name` (also the SNI). The CA is re-read on the
  route poll when it changes. Fails closed: no CA, a wrong name or another
  CA's certificate sends nothing and counts
  `stormlb_router_upstream_errors_total{kind="tls"}`. Any other protocol
  value skips the route.
- **docs:** README (`backend_ca_file`, the annotations, router, metrics,
  gaps), design.md, deck, example.

- **feat:** Prometheus `/metrics` (#12, stormcos#64). `[metrics] listen`
  (default `auto:9104`: the node's address and loopback) serves the text
  format: `stormlb_router_requests_total{host,code}` (the backend's code or
  the router's own; `host` is a route's or `unrouted`), the time-to-first-
  byte histogram, upstream errors by kind, connections total/active by
  listener, TLS handshake errors, routes and route refreshes, certificate
  loads; per VIP, connections, no-healthy-backend and connect errors,
  backend health and VRRP state; `process_*` for stormlb itself (stormd's
  describe stormd, stormd#33); `stormlb_build_info`. Hand-rolled, no new
  crate.
- **refactor:** the router's splice reads the backend's first bytes (status,
  latency) while the client→backend half copies concurrently, instead of
  `copy_bidirectional`; an 8 MiB upload to a backend that answers only after
  the whole body is a test.
- **docs:** README (`[metrics]`, Metrics reference, ports, gaps), design.md,
  deck, example.

### 2026-10-06
- **feat:** the router terminates TLS (#14). `[router.tls]` (`listen`,
  default `auto:443`; `certs`, PEM pairs; `redirect`, default on;
  `reload_secs`, default 30) adds a TLS listener that feeds the same demux.
  The certificate is chosen per handshake by SNI (webpki's name check, so a
  wildcard covers one label; else the first pair), and files are re-read
  when they change, keeping the last good pair if a new one fails. Once a
  certificate is loaded, plain HTTP gets a 308 to https, except `/healthz`.
  TLS requests reach the backend with `X-Forwarded-Proto: https` (a client's
  own is dropped). A TLS port that can't be bound is logged, not fatal.
  `tokio-rustls` and `rustls-webpki` become direct dependencies (both
  already locked). Filed stormcos#363 to mint `*.storm1.<zone>` and mount
  it, before the golden turns it on.
- **docs:** README (`[router.tls]`, router, ports, SECURITY note, tests,
  gaps), design.md, deck, example.

- **feat:** VRRP per RFC 5798 §6.4 (#7). A Backup preempts a
  lower-priority Master (new `preempt` key, default on, in `[vrrp]` and the
  API's `vrrp`), learns the Master's advertisement interval, and waits only
  Skew_Time after a priority-0 advertisement. A Master answers priority 0 at
  once and sends priority 0 when it stops.
- **feat:** VRRP ownership follows backend health: a Master with no healthy
  backend resigns, a Backup with none never takes over, and the address
  owner starts Backup until it has one.
- **fix:** VRRP advertisements carry the RFC checksum over the IPv4
  pseudo-header (it covered the VRRP payload alone, which no RFC peer
  accepts); received ones need IP TTL 255 and a valid checksum.
- **feat:** the VIP is added and removed over rtnetlink, and announced with
  raw gratuitous ARP replies on an `AF_PACKET` socket; the interface address
  is read over rtnetlink. No `ip` or `arping` binary is used (the golden
  has neither). `libc` becomes a direct dependency (already locked).
- **docs:** README (VRRP, `preempt`, gaps), design.md, deck, example.
- **test:** `tests/vrrp-netns.sh`: two stormlb in two network namespaces on
  a veth (unprivileged user namespace): takeover, preemption, gratuitous ARP,
  health resign, preempting back, priority 0 on removal.

- **feat:** runtime VIPs and the VIP API (#16). `[api]` (default
  `127.0.0.1:9103`) serves `GET/PUT/DELETE /api/v1/vips/{name}` and
  `GET /api/v1/vips`: named VIPs, each with its own listener, backends,
  health spec and optional VRRP, for stormcluster to keep the cluster's API
  VIP in front of the masters. A change is validated and any new listener
  bound before the running VIP is touched; backends that stay keep their
  health, new ones are probed at once, proxied connections are never cut.
  `token_file` (bearer, re-read per request) is required off loopback;
  `state_file` saves API VIPs and serves them again at start. The TOML
  `[vip]` is the read-only VIP `default`. Listeners bind with `IP_FREEBIND`
  and `SO_REUSEADDR`; VRRP instances stop cleanly and release the address.
- **feat:** `health.ca_file`: https checks verify the backend against a PEM
  CA (the cluster CA for `/readyz`); unset keeps accepting any certificate.
- **docs:** README (`[api]`, "VIP API", `ca_file`, ports, start-up, gaps),
  design.md (the runtime VIP), deck, example. Filed stormcluster#35: the VIP
  can't share `:6443` with a master's apiserver.

- **fix(test):** result lines carry no literal space (compact JSON, `\u0020`
  inside strings). The first runner run (9c56c07cc4, short, C2NR0Q2) passed
  all five checks, but rustkube-node's pod `/log` cut the first three words
  off each line (rustkube-node#136), so stormcentral read no results and
  recorded an error. The text parses back unchanged.
- **test:** the test container now matches stormcentral's runner as shipped
  (#11, #17). `test/requires.toml` declares `host_network = true` for
  short, medium and long (the router dials the suites' backends at the
  node's address) and a `nodes` read for long (its waves are sized from
  allocatable CPU); stormcentral#74 and #55 made both declarable.
  `test/build.sh` builds the static musl binary into `test/out/test` on the
  build box, and `test/Containerfile` only copies it in (no `rust:1-alpine`
  stage). `test/stormlb-test.yaml` is gone: the runner never read it, and
  requires.toml now records what it did. `.gitignore` gains `test/out/` and
  `tmp/`.
- **docs:** README "Tests on a node", the deck and `test/` module docs
  describe requires.toml and build.sh; #11 leaves the gaps list.

### 2026-09-28
- **docs:** fourth refresh. Still no stormlb code change since fd72dd5; keys,
  defaults, ports and shipping re-checked and unchanged. What moved is
  stormcos#64: every node's ironprom now scrapes stormd's `127.0.0.1:180`
  for stormlb, which records process state and restarts but not the router's
  memory, CPU or fds (stormd#33), and stormcos `docs/METRICS.md` lists the
  router's own metrics as missing until #12. README (metrics, tests),
  docs/design.md and docs/presentation.md say so. No new gaps found, so no
  new issues.

### 2026-09-27
- **docs:** third refresh. Still no stormlb code change since fd72dd5; keys,
  defaults and ports unchanged. The docs now carry what was filed or moved
  since: #12 (no `/metrics`, for stormcos#64), #13 (the upstream dial is
  bare TCP, so an HTTPS-only backend can't be routed) and #14 (plain `:80`
  only, no TLS termination; stormcos#81 requires TLS on every node
  listener). Ports table marks `:80` and stormd's `:180` (stormd#32) as
  plaintext without auth. stormcos#90's `bastion` profile also writes
  `start stormlb` and runs the apiserver with `--dev-anonymous-admin`, so
  README, design.md, the deck and the test crate's not-started message say
  "sno and bastion" where they said sno. Updated README, docs/design.md,
  docs/presentation.md, test/src/lib.rs.
- **docs:** second refresh. No stormlb code changed since the first. Config
  keys, defaults and ports were re-checked against `config.rs`, `router.rs`
  and `main.rs` and still match. What moved is stormcentral: its test runner
  shipped (`stormcentral test run stormlb <suite>`), and it doesn't run
  `test/` the way the docs said. It builds one image with no build-args,
  starts `/test <suite>` in its own Job (SA `storm-test`, namespaced Role
  only), reads results from the pod log, and never reads
  `test/stormlb-test.yaml`. So stormlb's suites get no `hostNetwork` and no
  `nodes` read. Filed #11, stormcentral#74 (a suite cannot ask for
  hostNetwork); the `nodes` read is stormcentral#55. Updated: README "Tests
  on a node" and gaps, the deck, and the headers of `test/Containerfile` and
  `test/stormlb-test.yaml` (now a hand-run template). Also updated the
  `test/src` doc comments that said the Job is hostNetwork and that
  `/results` is the runner's volume.
- **docs:** refreshed from the code for everything since 2026-09-18 (the
  router's `auto`/loopback listen, `Cargo.lock`, the test container, the CRLF
  `/healthz`). The facts taken from other components were re-checked at
  their current HEADs: stormcentral's component entry and relationships,
  stormcos's boot.d stanza, sno-only start line and apiserver flags, stormd's
  port + 100, and rustkube's anonymous auth. Two things the docs implied that
  the code does not do are now stated plainly and filed.
  #9: the router reads HTTPRoutes and Services anonymously, which works only
  against the sno apiserver's `--dev-anonymous-admin`.
  #10: `insecure = false` can never work, because reqwest trusts only its
  compiled-in webpki roots and there is no CA-file key; the old text said the
  CA was "not in a trust store yet".
  Updated: README (`[router]` table, route table, gaps), `router.rs` field
  doc, the example, design.md and the deck.

### 2026-09-26
- **fix(router):** `/healthz` now answers with CRLF line endings, like the
  router's 400 and 404 (#5). The response literal spanned source lines and
  sent bare LF, which only lenient clients (curl, stormd's probe) accept; a
  strict one would have failed the probe and had stormd restart a healthy
  router. A new test checks the exact bytes over a socket, the old test's
  bare-LF request literals are CRLF, and the test container's medium suite
  now expects `healthz-crlf` to pass.
- **test:** stormlb's test container (#8), per stormcentral
  `docs/test-standard.md`: `test/` is an own-workspace crate built
  `FROM scratch` into `stormlb-test-<suite>`, with a Job template
  (`test/stormlb-test.yaml`: namespaced ServiceAccount, hostNetwork). It has
  three suites against the router on a node. `short` checks `/healthz`,
  stormd's view, and a route that appears and goes. `medium` checks every
  documented behaviour and failure path. `long` runs waves sized from the
  node's CPUs, with a trend. They print JSON lines, exit 0/1/2, and clean up
  by `storm.io/test-run`. `test/tests/harness.rs` runs all three against the
  real router and an in-memory apiserver in `sc-build`. The runner is
  stormcentral#41.
- **docs:** `docs/presentation.md`, a 12-slide Marp deck on stormlb's purpose
  and functionality (#4). It covers the problem, its place in stormcos (from
  stormcentral's relationships graph), how it works, what ships versus what
  is implemented or planned, interfaces, how it ships, and status. Linked
  from the README.
- **docs:** the README now says `start stormlb` is written only for the
  stormcos `sno` profile; before, it implied every node. The example config
  no longer claims sub-second VRRP failover (3.6 s at defaults).
- **docs:** #3 verified on the build box: `cargo test --locked` (24 passed)
  and `cargo doc --no-deps --locked` with `-D warnings` clean; #3 and #1
  closed.

### 2026-09-24
- **docs:** README rewritten from the code (#3, closes #1). Every flag and
  config key with its default, the router's and L4 half's actual behaviour,
  ports and endpoints (router `/healthz` only; no metrics), and how it ships:
  the router-only stormcentral `service` golden under stormd, started by a
  stormpump boot.d `start stormlb` line (not systemd). Design notes moved to
  `docs/design.md`, marked where the code does not do it yet. Module docs
  corrected (lib.rs lists `router`; vrrp.rs no longer says the wire path is
  missing), and the example config gained `[router]`. The gaps found are
  filed as #5 (bare-LF `/healthz`), #6 (BGP reconciles only every 60 s) and
  #7 (VRRP preemption, priority 0, health, `ip`/`arping`).
- **build:** commit `Cargo.lock` (#2). It was gitignored, so no commit said
  which dependency versions a golden was built from, and stormcentral's
  `cargo build --release --locked` refused to build at all. Generated with
  `cargo generate-lockfile` (cargo 1.95.0) on the build box; `cargo update`
  is now a deliberate commit of its own.

### 2026-08-31
- **feat(router):** the L7 half of inbound — a Host-header router over
  Gateway API HTTPRoutes, per stormpump docs/routing.md. Reads one request
  head, demuxes on Host, splices the connection (keep-alive, websockets and
  SSE ride free). Route table polled from the apiserver every 5 s, last good
  table kept across apiserver failures. Backends: the storm.io/backend
  annotation verbatim (how a node service says 127.0.0.1:port on every node
  without per-node manifests), else the first backendRef resolved to its
  Service clusterIP. \`[router]\` alone is a complete config — on a single
  node the VIP is the node's own address; \`vip\` is now optional.

### 2026-09-22
- **feat(router):** `listen = "auto:80"` binds this node's own routable
  addresses rather than the wildcard. `0.0.0.0:80` includes
  `169.254.169.254`, which the instance metadata service binds — a fixed
  address every cloud image asks and not one this router may claim. Whichever
  started second got `EADDRINUSE` and crash-looped, with nothing saying the
  two were fighting over a port. Loopback is skipped because a router nothing
  outside can reach is not a router; link-local is skipped because it is not
  an address anybody routes to us on.
