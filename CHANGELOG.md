# Changelog

## [Unreleased]

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
