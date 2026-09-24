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

- [ ] #2 Commit Cargo.lock — un-ignore it, generate it on the build box
      (`cargo generate-lockfile`), commit, verify `cargo build --release --locked`
      and `cargo test --locked` via sc-build, request the golden.
- [ ] #1 docs: README says systemd; a node has no systemd.
