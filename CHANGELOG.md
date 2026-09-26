# Changelog

## [Unreleased]

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
