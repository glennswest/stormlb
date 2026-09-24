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
- [ ] #3 docs rewritten from the code (covers #1 as well). Gaps filed: #5, #6, #7.
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
  - [ ] Verify at 5d1ccee: `cargo build --release --locked` passed on dev
        (24m 50s, build box at load 67); `cargo test --locked` and
        `RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --locked` not yet run
        to completion (both attempts stopped for memory on the session VM).
        Then close #3 and #1, and request the golden once.
- [ ] #1 docs: README says systemd; a node has no systemd — closed by #3.
- [ ] #4 docs: a presentation of its purpose and functionality.
