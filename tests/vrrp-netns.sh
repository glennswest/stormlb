#!/usr/bin/env bash
# VRRP on a real wire, with no root (#7): two stormlb processes, each in its
# own network namespace, joined by a veth pair, inside an unprivileged user
# namespace. Each makes the same VIP through its API, with VRRP on the veth
# and its own API port as the backend (so health can be switched off by
# pointing the backend at a closed port).
#
# Checks, against the kernel's view (`ip addr`, `ip neigh`):
#   1. A alone (priority 100) takes the VIP after master-down.
#   2. B (priority 200) starts and preempts it; A releases it.
#   3. A learns B's MAC for the VIP from the gratuitous ARP.
#   4. B loses every healthy backend: it resigns, and A has the VIP within
#      Skew_Time + slack, well under the 3.6 s master-down.
#   5. B's backend is healthy again: it preempts back.
#   6. B's VIP is deleted: B sends priority 0 and releases; A takes over fast.
#
# Run (needs a built binary; dev allows unprivileged user namespaces):
#   cargo build --release --locked && tests/vrrp-netns.sh   # or: tests/vrrp-netns.sh <path to stormlb>
set -euo pipefail

BIN=$(realpath "${1:-${CARGO_TARGET_DIR:-target}/release/stormlb}")
if [ "${STORMLB_IN_NS:-}" != 1 ]; then
    exec env STORMLB_IN_NS=1 unshare -rn "$0" "$BIN"
fi

W=$(mktemp -d)
VIP=10.9.0.100
pids=()
cleanup() { kill "${pids[@]}" 2>/dev/null || true; wait 2>/dev/null || true; rm -rf "$W"; }
trap cleanup EXIT
fail() { echo "FAIL: $*"; for n in a b; do echo "--- stormlb $n log"; tail -30 "$W/$n.log" || true; done; exit 1; }
pass() { echo "pass: $*"; }

# Two namespaces, held open by a sleeping process each, on one veth.
unshare -n sleep 600 & NA=$!; pids+=("$NA")
unshare -n sleep 600 & NB=$!; pids+=("$NB")
sleep 0.2
ip link add va type veth peer name vb
ip link set va netns "$NA"
ip link set vb netns "$NB"
ns() { local p=$1; shift; nsenter -t "$p" -n "$@"; }
ns "$NA" ip link set lo up; ns "$NA" ip addr add 10.9.0.1/24 dev va; ns "$NA" ip link set va up
ns "$NB" ip link set lo up; ns "$NB" ip addr add 10.9.0.2/24 dev vb; ns "$NB" ip link set vb up
# Let a gratuitous ARP create a neighbour entry, so check 3 can see it.
ns "$NA" sh -c 'echo 1 > /proc/sys/net/ipv4/conf/va/arp_accept' 2>/dev/null || echo "note: arp_accept not settable"

printf '[api]\nlisten = "127.0.0.1:9103"\n' > "$W/stormlb.toml"

# One API request inside a namespace: prints the body; status in $W/status.
api() {
    local p=$1 method=$2 path=$3 body=${4:-}
    ns "$p" bash -c '
        exec 3<>/dev/tcp/127.0.0.1/9103
        printf "%s %s HTTP/1.1\r\nhost: x\r\ncontent-length: %d\r\n\r\n%s" "$1" "$2" "${#3}" "$3" >&3
        cat <&3' _ "$method" "$path" "$body" > "$W/resp"
    sed -n '1s/^HTTP\/1.1 \([0-9]*\).*/\1/p' "$W/resp" > "$W/status"
    sed '1,/^\r$/d' "$W/resp"
}
spec() { # iface prio backend-port
    printf '{"address":"%s","port":7443,"backends":[{"address":"127.0.0.1","port":%s}],"health":{"mode":"tcp","interval_secs":1,"timeout_secs":1},"vrrp":{"interface":"%s","vrid":51,"priority":%s,"advert_interval_secs":1}}' \
        "$VIP" "$3" "$1" "$2"
}
holds() { ns "$1" ip -4 -o addr show dev "$2" | grep -q " $VIP/32"; }
# Wait up to $1 seconds for a command to succeed; prints how long it took.
within() {
    local limit=$1; shift
    local t0; t0=$(date +%s%N)
    while ! "$@"; do
        if [ $(( ($(date +%s%N) - t0) / 1000000 )) -gt $(( limit * 1000 )) ]; then return 1; fi
        sleep 0.05
    done
    echo $(( ($(date +%s%N) - t0) / 1000000 ))
}
start() { # name pid iface
    ns "$2" "$BIN" --config "$W/stormlb.toml" > "$W/$1.log" 2>&1 &
    pids+=("$!")
    within 5 ns "$2" bash -c 'exec 3<>/dev/tcp/127.0.0.1/9103' > /dev/null || fail "stormlb $1 API did not come up"
}

# 1. A alone takes the VIP after master-down (3.6 s at priority 100).
start a "$NA" va
api "$NA" PUT /api/v1/vips/api "$(spec va 100 9103)" > /dev/null
[ "$(cat "$W/status")" = 201 ] || fail "A: PUT answered $(cat "$W/status")"
ms=$(within 8 holds "$NA" va) || fail "A never took the VIP"
pass "A (100) took the VIP after ${ms} ms"
api "$NA" GET /api/v1/vips/api | grep -q '"vrrp":"master"' || fail "A's API does not say master"

# 2. B (200) starts and preempts.
start b "$NB" vb
api "$NB" PUT /api/v1/vips/api "$(spec vb 200 9103)" > /dev/null
ms=$(within 8 holds "$NB" vb) || fail "B (200) never preempted A (100)"
within 2 not_holds "$NA" va > /dev/null || fail "A kept the VIP after B took it"
pass "B (200) preempted A in ${ms} ms; A released it"
api "$NA" GET /api/v1/vips/api | grep -q '"vrrp":"backup"' || fail "A's API does not say backup"

# 3. The gratuitous ARP reached A: its neighbour entry for the VIP is B's MAC.
BMAC=$(ns "$NB" cat /sys/class/net/vb/address 2>/dev/null || ns "$NB" ip -o link show vb | sed -n 's/.*link\/ether \([0-9a-f:]*\).*/\1/p')
neigh_is_b() { ns "$NA" ip neigh show "$VIP" dev va | grep -qi "$BMAC"; }
if within 3 neigh_is_b > /dev/null; then
    pass "A's neighbour entry for $VIP is B's MAC ($BMAC): gratuitous ARP"
else
    fail "A has no neighbour entry for $VIP at B's MAC ($BMAC): $(ns "$NA" ip neigh show "$VIP" dev va)"
fi

# 4. B's backend goes unhealthy: B resigns, A takes over fast.
api "$NB" PUT /api/v1/vips/api "$(spec vb 200 1)" > /dev/null
ms=$(within 4 holds "$NA" va) || fail "A did not take over when B lost its backends"
holds "$NB" vb && fail "B kept the VIP with no healthy backend"
pass "B resigned on no healthy backend; A took the VIP ${ms} ms after the change"
api "$NB" GET /api/v1/vips/api | grep -q '"healthy":0' || fail "B's API does not show 0 healthy"

# 5. B's backend is back: B preempts again.
api "$NB" PUT /api/v1/vips/api "$(spec vb 200 9103)" > /dev/null
ms=$(within 8 holds "$NB" vb) || fail "B did not preempt back once healthy"
pass "B healthy again: preempted back in ${ms} ms"
within 2 not_holds "$NA" va > /dev/null || fail "A kept the VIP after B took it back"

# 6. B's VIP is deleted: priority 0, release; A takes over in about Skew_Time
#    (0.6 s at priority 100), not master-down (3.6 s).
api "$NB" DELETE /api/v1/vips/api > /dev/null
ms=$(within 3 holds "$NA" va) || fail "A did not take over after B's VIP was deleted"
holds "$NB" vb && fail "B kept the VIP after its VIP was deleted"
[ "$ms" -lt 2500 ] || fail "A took ${ms} ms: priority 0 should cut the wait to Skew_Time"
pass "B's VIP deleted: priority 0, released; A took the VIP in ${ms} ms"

echo "vrrp-netns: all passed"
