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
- [x] #8 test containers per stormcentral docs/test-standard.md (pattern:
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
  - [x] Image: podman build of test/Containerfile on dev at 93d82e6 (5.9 MB,
        scratch, labels set); run with no env -> JSON line, exit 2. Root
        `cargo build --release --locked && cargo test --locked` passes at
        091c2dc. #8 closed, golden requested. Not yet run on a node: needs
        stormcentral#41 and a reachable test machine.
- [x] #5 router `/healthz` answers with bare LF: write it with explicit CRLF
      like the 400/404, fix the unit test's bare-LF literals, add a socket
      test of the exact bytes. The harness's medium expectation flips
      `healthz-crlf` to pass; README gaps, deck and medium.rs docs drop #5.
      Verified at 62fc06e via sc-build: root release build, 25 tests (new
      `healthz_on_an_unclaimed_host_is_crlf_on_the_wire`), cargo doc -D
      warnings, and the test crate (11 unit + 3 harness; medium now asserts
      `healthz-crlf` passes).
- [x] Docs refresh from the code, changes since 2026-09-18 (2026-09-27).
      Cross-component facts re-checked at today's HEADs (stormcentral
      components/stormcos.toml + config, stormcos build-goldens.sh boot.d and
      apiserver flags, stormd port+100, rustkube auth): unchanged except what
      the docs never said. New gaps filed: #9 (router reads anonymously; works
      only against sno's --dev-anonymous-admin), #10 (insecure = false cannot
      work: webpki roots only, no CA-file key). Fix README `[router]`/route
      table, router.rs field docs, example, design.md, deck; CHANGELOG.
      Done in fd72dd5; sc-build passed there (release build, 25 tests,
      cargo doc -D warnings). #9 P2, #10 P3. Open now: #6, #7 (P2), #9 (P2),
      #10 (P3).
- [x] Docs refresh, second pass (2026-09-27, afternoon). No stormlb code
      changed since fd72dd5; config keys, defaults and ports re-checked and
      unchanged. What moved is stormcentral: its test runner shipped
      (stormcentral 02098e0/321bade/20a570b, `stormcentral test run`). It
      builds `-f test/Containerfile` from the repo root with no build-args,
      runs its own Job (`/test <suite>`, STORM_* env, SA `storm-test` with a
      namespaced Role only), and never reads `test/stormlb-test.yaml`: no
      hostNetwork, no nodes ClusterRole (stormcentral#55).
  - [x] File stormcentral issue: a suite cannot ask for hostNetwork.
  - [x] File stormlb issue: test/ vs the runner as shipped; propose it
        --after the stormcentral issues.
  - [x] README "Tests on a node", deck, test/Containerfile and
        test/stormlb-test.yaml headers, test/src/env.rs reach_ip doc; CHANGELOG.
  - [x] sc-build (root + test crate + cargo doc -D warnings), golden.
  - Done in daef750: filed stormcentral#74 and #11 (P2, proposed after
    stormcentral#74). sc-build passed there: release build, 25 tests,
    cargo doc -D warnings, test crate 11 unit + 3 harness and its cargo doc.
    Open now: #6, #7, #9, #11 (P2), #10 (P3).
    Golden NOT built yet: three `stormcentral component build stormlb`
    requests at b9da689 were each interrupted by a stormcentral restart
    (builds 6c320b5e1a19, 39c66777f85d and the first). Request it again.
- [x] Docs refresh, third pass (2026-09-27, evening). No code change since
      fd72dd5. What moved: #12 (no /metrics), #13 (upstream always plaintext),
      #14 (plaintext :80 only) were filed and the docs don't mention them;
      stormcos#90 added PROFILE=bastion, which also writes `start stormlb`
      and runs the apiserver with --dev-anonymous-admin; stormcos#81
      SECURITY.md lists :80 (stormlb#14) and stormd's :180 (stormd#32) as
      plaintext, unauthenticated listeners.
  - [x] README (router, ports, metrics, how it ships, gaps), design.md,
        presentation.md, test/src/lib.rs not-started text, CHANGELOG. No new
        issues: every gap found already has one (#12, #13, #14 by the owner).
  - [x] sc-build (cargo doc -D warnings), then request the golden (also
        still owed from the second pass). The first sc-build at 869457a
        never got a slot before the session ended; folded into the fourth pass.
- [x] Docs refresh, fourth pass (2026-09-28). Still no code change since
      fd72dd5. What moved: stormcos#64 (198d8a2) — every node's ironprom
      scrapes stormd `127.0.0.1:180` for stormlb (state/restarts only; RSS,
      CPU, fds are stormd#33, filed from #8), and stormcos docs/METRICS.md
      lists the router's own metrics as missing on `:80` until #12 names a
      port. stormd#33 is not cited in the docs yet.
  - [x] README metrics + tests "not observable", design.md, deck; CHANGELOG.
        Done in a039a06; no new gaps, so no new issues.
  - [x] sc-build passed at a039a06: release build, 25 tests, cargo doc
        -D warnings, test crate 11 unit + 3 harness. Golden requested there
        (covers the second and third passes too). Open now: #6, #7 (P3 after
        the 2026-09-28 validation: VIP half not in the shipped golden), #9,
        #11, #12, #13, #14 (P2), #10 (P3).
