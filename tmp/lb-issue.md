Found in the docs refresh, 2026-09-27. stormcentral's test runner has shipped (`stormcentral test run stormlb <suite>`; stormcentral 02098e0, 321bade, 20a570b). It does not run `test/` the way stormlb's docs and `test/stormlb-test.yaml` describe.

What the runner does (stormcentral `src/testruns.rs`, `docs/test-standard.md`):
- It builds `podman build -f test/Containerfile <repo root>` with **no build-args**. One image serves every suite: `SUITE` defaults to `short` and `COMMIT` to `unknown` in the image labels. The suite is still right at run time, because the runner starts `/test <suite>` and sets `STORM_SUITE`, which the binary reads.
- It **never reads `test/stormlb-test.yaml`**. It creates its own Job: SA `storm-test` with a namespaced Role only, **no hostNetwork**, **no ClusterRole**.
- It runs an optional `test/build.sh` on the build box with a cargo cache. stormlb has none, so the test binary compiles inside `podman build` from `rust:1-alpine` every time.

Effect on stormlb's suites:
- **No hostNetwork.** The backend listeners are reached at the pod IP, not the node's. Routed checks pass only where the host can route to pod IPs, which has not been verified on any test machine. Needs stormcentral#74.
- **No `nodes` read.** The long suite's `capacity` falls back to the container's CPUs and says so in its result line. Needs stormcentral#55.

**Proposed resolution.**
1. Once stormcentral#55 and #74 land, declare `host_network` and the `nodes` read in their format.
2. Add `test/build.sh` (static musl build on the box) and `COPY` the binary in, as the standard recommends.
3. Drop `test/stormlb-test.yaml`, or keep it clearly marked as a hand-run reference.
4. Run all three suites through `stormcentral test run` on a test machine.
