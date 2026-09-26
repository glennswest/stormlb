# CLAUDE.md — stormlb

Pre-cluster control-plane VIP load balancer (L4 + VRRP/BGP + Host-header
router). See README.md. Version: `Cargo.toml` (`version`) — the only version
location. Current: 0.1.0.

## Build

No cargo on the session VM: build/test with `sc-build` after pushing.
Goldens build with `cargo build --release --locked`, so `Cargo.lock` is
committed and must stay in sync with `Cargo.toml`. `cargo update` is a
deliberate commit of its own.

## Work plan

- [x] #2 Commit Cargo.lock — un-ignore it, generate it on the build box
      (`cargo generate-lockfile`), commit, verify `cargo build --release --locked`
      and `cargo test --locked` via sc-build, request the golden.
- [x] #3 docs rewritten from the code (covers #1 as well). Gaps filed: #5, #6, #7.
  - [x] README.md: what it is/does today, build (sc-build), every config key
        with its default, ports, health endpoint (no metrics), how it ships
        (stormcentral `service` golden under stormd, router-only config,
        boot.d `start stormlb` in stormcos).
  - [x] docs/design.md: L2/L3 VIP design, router design; marked where the
        shipped golden does not use it.
  - [x] Module doc comments: lib.rs (router missing), vrrp.rs (stale
        "decoupled"/"VRRP wiring"), config.rs `bind`, main.rs, example TOML.
  - [x] File issues for gaps found (bare-LF /healthz, BGP reconcile only on
        the 60 s keepalive tick, VRRP backup ignores priority / not tied to
        backend health, VRRP needs `ip`/`arping` absent from the golden).
  - [x] Verified via sc-build on dev: `cargo build --release --locked`
        passed at 5d1ccee; `cargo test --locked` (24 passed) and
        `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --locked` passed at
        b8ba1a7. #3 and #1 closed, golden requested.
- [x] #1 docs: README says systemd; a node has no systemd — closed by #3.
- [x] #4 docs: a presentation of its purpose and functionality.
  - [x] `docs/presentation.md`, Marp Markdown, 8–15 slides, drawn from the
        README/design.md and checked against the code: purpose, place in
        stormcos (stormcentral relationships), how it works (diagram), what
        works today vs planned, interfaces, how it ships, status/open issues.
  - [x] Link from README, CHANGELOG, sc-build, close #4, request golden.
        Verified at 5095331: `cargo test --locked` (24 passed) and `cargo doc`
        with `-D warnings` via sc-build; Marp renders it to 12 slides. Slide
        overflow not checked visually (no browser on the session VM).
- [ ] #8 test containers per stormcentral docs/test-standard.md (pattern:
      stormcast `test/`). stormcentral's runner is not built yet (filed
      stormcentral#41); the only test machine was unreachable on 2026-09-26.
  - [x] `test/`: own-workspace crate `stormlb-test` (lib + bin), static musl,
        `FROM scratch` Containerfile (SUITE/COMMIT build args), Job YAML with
        SA + namespaced Role + run-labelled ClusterRole (nodes read),
        hostNetwork (the router dials the test's own backend listener).
  - [x] short: router /healthz, stormd supervising it (:180/metrics), an
        HTTPRoute with `storm.io/backend` is routed, and 404s once deleted.
  - [x] medium: 400/404, /healthz unclaimed/claimed/CRLF (#5), Host case and
        port, per-connection stickiness, streaming unbuffered, upgrade, large
        body, 16 KiB head limit, dead backend, route update, backendRef via
        Service+Endpoints (skip without a data plane), headless skipped,
        many hosts, concurrency, stormd restarts unchanged, VIP half skip.
  - [x] long: waves sized from node allocatable CPU (API) and the fd limit;
        route-programming, request latency, drain; residue and restarts.
  - [x] Hermetic harness (`test/tests/harness.rs`): the real router
        (`stormlb::router::run`) against an in-memory apiserver, running the
        actual suites; run via sc-build with `cd test && cargo test --locked`.
  - [x] README "Tests on a node", CHANGELOG, test/Cargo.lock via dev.
  - [x] Harness passes on dev at 93d82e6 (`cd test && cargo test --locked`:
        11 unit + 3 harness; short 5, medium 22, long 4 waves). The first
        run hit a stack overflow (16 KiB read array in nested futures), fixed.
  - [ ] Image: podman build of test/Containerfile on dev, smoke run (no env
        -> exit 2). Then close #8, request golden.
