#!/usr/bin/env bash
# Run by stormcentral's test runner on the build box before `podman build`
# (stormcentral docs/test-standard.md), and usable by hand: builds the static
# (musl) test binary that test/Containerfile copies in as /test.
set -euo pipefail
cd "$(dirname "$0")"
source "$HOME/.cargo/env" 2>/dev/null || true
T=x86_64-unknown-linux-musl
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target}"
cargo build --release --locked --target "$T" --bin stormlb-test --quiet
mkdir -p out
cp "$CARGO_TARGET_DIR/$T/release/stormlb-test" out/test
echo "test/out/test: $(du -h out/test | cut -f1)"
