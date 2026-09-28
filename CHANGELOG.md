# Changelog

## [Unreleased]

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
