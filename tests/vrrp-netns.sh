#!/usr/bin/env bash
# VRRP on a real wire, with no root (#7): two stormlb processes, each in its
# own network namespace, joined by a veth pair, inside an unprivileged user
# namespace. Each makes the same VIP through its API, with VRRP on the veth
# and its own API port as the backend (so health can be switched off by
# pointing the backend at a closed port).
#
# Checks, against the kernel's view (`ip addr`, `ip neigh`), once for an IPv4
# VIP (10.9.0.100) and once for an IPv6 one (fd09::100, #39: VRRP over IPv6,
# link-local sources, ff02::12, unsolicited Neighbor Advertisements):
#   1. A alone (priority 100) takes the VIP after master-down.
#   2. B (priority 200) makes the same VIP and preempts; A releases it.
#   3. A learns B's MAC for the VIP from the gratuitous ARP / the NA.
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
HAVE6=0
[ -d /proc/sys/net/ipv6 ] && HAVE6=1
for x in "$NA va 10.9.0.1 fd09::1" "$NB vb 10.9.0.2 fd09::2"; do
    set -- $x
    ns "$1" ip link set lo up
    ns "$1" ip addr add "$3/24" dev "$2"
    if [ "$HAVE6" = 1 ]; then
        # No DAD on the test link: the link-local is usable (not tentative)
        # the moment the veth is up.
        ns "$1" sh -c "echo 0 > /proc/sys/net/ipv6/conf/$2/accept_dad" 2>/dev/null || true
        ns "$1" ip -6 addr add "$4/64" dev "$2" nodad
    fi
    ns "$1" ip link set "$2" up
done
# Let a gratuitous ARP / an unsolicited NA create a neighbour entry, so check
# 3 can see it.
ns "$NA" sh -c 'echo 1 > /proc/sys/net/ipv4/conf/va/arp_accept' 2>/dev/null || echo "note: arp_accept not settable"
NA6=1
if [ "$HAVE6" = 1 ]; then
    ns "$NA" sh -c 'echo 1 > /proc/sys/net/ipv6/conf/va/accept_untracked_na' 2>/dev/null || {
        NA6=0; echo "note: accept_untracked_na not settable (kernel < 5.19): IPv6 check 3 skipped"; }
fi

printf '[api]\nlisten = "127.0.0.1:9109"\n' > "$W/stormlb.toml"

# One API request inside a namespace: prints the body; status in $W/status.
api() {
    local p=$1 method=$2 path=$3 body=${4:-}
    ns "$p" bash -c '
        exec 3<>/dev/tcp/127.0.0.1/9109
        printf "%s %s HTTP/1.1\r\nhost: x\r\ncontent-length: %d\r\n\r\n%s" "$1" "$2" "${#3}" "$3" >&3
        cat <&3' _ "$method" "$path" "$body" > "$W/resp"
    sed -n '1s/^HTTP\/1.1 \([0-9]*\).*/\1/p' "$W/resp" > "$W/status"
    sed '1,/^\r$/d' "$W/resp"
}
# The VIP under test ($VIP, $FAM 4 or 6, $PLEN 32 or 128) is set per pass.
spec() { # iface prio backend-port
    printf '{"address":"%s","port":7443,"backends":[{"address":"127.0.0.1","port":%s}],"health":{"mode":"tcp","interval_secs":1,"timeout_secs":1},"vrrp":{"interface":"%s","vrid":51,"priority":%s,"advert_interval_secs":1}}' \
        "$VIP" "$3" "$1" "$2"
}
holds() { ns "$1" ip -"$FAM" -o addr show dev "$2" | grep -q " $VIP/$PLEN"; }
not_holds() { ! holds "$@"; }
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
start() { # name pid
    ns "$2" "$BIN" --config "$W/stormlb.toml" > "$W/$1.log" 2>&1 &
    pids+=("$!")
    within 5 ns "$2" bash -c 'exec 3<>/dev/tcp/127.0.0.1/9109' > /dev/null || fail "stormlb $1 API did not come up"
}

start a "$NA"
start b "$NB"
BMAC=$(ns "$NB" cat /sys/class/net/vb/address 2>/dev/null || ns "$NB" ip -o link show vb | sed -n 's/.*link\/ether \([0-9a-f:]*\).*/\1/p')

# One pass of the six checks for VIP $VIP, VIP name $1, IPv$FAM.
checks() {
    local name=$1 P="IPv$FAM"
    local path=/api/v1/vips/$name

    # 1. A alone (B has no such VIP) takes it after master-down (3.6 s at
    #    priority 100).
    api "$NA" PUT "$path" "$(spec va 100 9109)" > /dev/null
    [ "$(cat "$W/status")" = 201 ] || fail "$P A: PUT answered $(cat "$W/status"): $(cat "$W/resp")"
    ms=$(within 8 holds "$NA" va) || fail "$P: A never took the VIP"
    pass "$P: A (100) took the VIP after ${ms} ms"
    api "$NA" GET "$path" | grep -q '"vrrp":"master"' || fail "$P: A's API does not say master"

    # 2. B (200) makes the same VIP and preempts.
    api "$NB" PUT "$path" "$(spec vb 200 9109)" > /dev/null
    ms=$(within 8 holds "$NB" vb) || fail "$P: B (200) never preempted A (100)"
    within 2 not_holds "$NA" va > /dev/null || fail "$P: A kept the VIP after B took it"
    pass "$P: B (200) preempted A in ${ms} ms; A released it"
    api "$NA" GET "$path" | grep -q '"vrrp":"backup"' || fail "$P: A's API does not say backup"

    # 3. The announcement reached A: its neighbour entry for the VIP is B's MAC.
    neigh_is_b() { ns "$NA" ip -"$FAM" neigh show "$VIP" dev va | grep -qi "$BMAC"; }
    if [ "$FAM" = 6 ] && [ "$NA6" = 0 ]; then
        echo "skip: $P check 3 (accept_untracked_na not settable)"
    elif within 3 neigh_is_b > /dev/null; then
        pass "$P: A's neighbour entry for $VIP is B's MAC ($BMAC)"
    else
        fail "$P: A has no neighbour entry for $VIP at B's MAC ($BMAC): $(ns "$NA" ip -"$FAM" neigh show "$VIP" dev va)"
    fi

    # 4. B's backend goes unhealthy: B resigns, A takes over fast.
    api "$NB" PUT "$path" "$(spec vb 200 1)" > /dev/null
    ms=$(within 4 holds "$NA" va) || fail "$P: A did not take over when B lost its backends"
    holds "$NB" vb && fail "$P: B kept the VIP with no healthy backend"
    pass "$P: B resigned on no healthy backend; A took the VIP ${ms} ms after the change"
    api "$NB" GET "$path" | grep -q '"healthy":0' || fail "$P: B's API does not show 0 healthy"

    # 5. B's backend is back: B preempts again.
    api "$NB" PUT "$path" "$(spec vb 200 9109)" > /dev/null
    ms=$(within 8 holds "$NB" vb) || fail "$P: B did not preempt back once healthy"
    pass "$P: B healthy again: preempted back in ${ms} ms"
    within 2 not_holds "$NA" va > /dev/null || fail "$P: A kept the VIP after B took it back"

    # 6. B's VIP is deleted: priority 0, release; A takes over in about
    #    Skew_Time (0.6 s at priority 100), not master-down (3.6 s).
    api "$NB" DELETE "$path" > /dev/null
    ms=$(within 3 holds "$NA" va) || fail "$P: A did not take over after B's VIP was deleted"
    holds "$NB" vb && fail "$P: B kept the VIP after its VIP was deleted"
    [ "$ms" -lt 2500 ] || fail "$P: A took ${ms} ms: priority 0 should cut the wait to Skew_Time"
    pass "$P: B's VIP deleted: priority 0, released; A took the VIP in ${ms} ms"
}

VIP=10.9.0.100 FAM=4 PLEN=32
checks api4
if [ "$HAVE6" = 1 ]; then
    VIP=fd09::100 FAM=6 PLEN=128
    checks api6
else
    echo "SKIP: IPv6 pass (no IPv6 in this kernel)"
fi

echo "vrrp-netns: all passed"
