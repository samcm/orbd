#!/usr/bin/env bash
# End to end check of orbd's container image as deployed (see the README): a coordinator
# container and a privileged, zero-config agent container joined with `orbd join-token`,
# jobs sent through the coordinator's dispatch socket with `orbd dispatch` (as `amp orb
# provider` runs them), then a real `docker stop` / `docker start` of the agent with a
# running QEMU orb and a running Firecracker orb.
#
#   container-e2e.sh IMAGE ROOTFS
#
# ROOTFS is the KVM test root disk (kvm-guest/build-rootfs.sh), served through the
# agent's `images.static_images`. Its counter process prints `counter pid=<pid> n=<n>`
# every second; it must keep its pid and count through pause/wake and through the agent
# container's restart. Its stand-in bootstrap probes the TCP targets in `E2E_PROBE`: the
# Firecracker orb must reach the internet but not the agent's namespace. The agent sets a
# non-default ORBD_GUEST_NETWORK, which both orbs must be addressed in. Needs Docker,
# /dev/kvm and the internet; nested virtualization is not needed.
set -euo pipefail
image=$1
rootfs=$(realpath "$2")
id=orbd-e2e-$$
coord=$id-coordinator agent=$id-agent
orb_image=example.invalid/orbd-e2e:latest
runtimes="qemu firecracker"
work=$(mktemp -d)

strip() { sed 's/\x1b\[[0-9;]*m//g'; }
# The serial console of the runtime's orb (QEMU disks are qcow2, Firecracker's raw).
console() { # RUNTIME
    local disk=disk.qcow2
    [ "$1" = qemu ] || disk=disk.raw
    docker run --rm --network none -v "$agent:/s:ro" --entrypoint sh "$image" -c \
        "for d in /s/orbs/*/; do [ ! -e \$d/$disk ] || cat \$d/logs/console.log; done" 2>/dev/null || true
}
cleanup() {
    local status=$?
    if [ "$status" != 0 ]; then
        for c in "$coord" "$agent"; do
            echo "==== docker logs $c (last 60 lines)"
            docker logs --tail 60 "$c" 2>&1 | strip || true
        done
        for rt in $runtimes; do
            echo "==== $rt orb console (last 40 lines)"
            console "$rt" | tail -n 40
        done
    fi
    docker rm -f "$coord" "$agent" >/dev/null 2>&1 || true
    docker volume rm "$coord" "$agent" >/dev/null 2>&1 || true
    docker network rm "$id" >/dev/null 2>&1 || true
    rm -rf "$work"
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }
# Not `grep -q`: it exits at the first match, and under pipefail the writer's SIGPIPE
# then fails the whole pipeline.
has() { grep "$@" >/dev/null; }
wait_for() { # SECONDS WHAT COMMAND...
    local deadline=$((SECONDS + $1)) what=$2
    shift 2
    until "$@" >/dev/null 2>&1; do
        [ "$SECONDS" -lt "$deadline" ] || fail "timed out waiting for $what"
        sleep 0.5
    done
}

# One Amp lifecycle job for the runtime's orb, with the environment `amp orb provider`
# gives `orbd dispatch`.
dispatch() { # OPERATION RUNTIME
    local orb=e2e-$2
    local env=(-e ORBD_DISPATCH_SOCKET=/var/lib/orbd/dispatch.sock -e "ORBD_OFFERING_ID=$2-1vcpu-2gb"
        -e "ORB_OPERATION=$1" -e "ORB_ID=$orb" -e "ORB_SANDBOX_ID=sandbox-$orb"
        -e AMP_URL=https://ampcode.com -e "AMP_THREAD_ID=T-$orb" -e "AMP_SANDBOX_ID=orb-provider:e2e:$orb")
    [ "$1" != spawn ] || env+=(-e "ORB_IMAGE=$orb_image" -e ORB_TOKEN=e2e-orb-token
        -e "E2E_PROBE=1.1.1.1:53 $agent_ip:9"
        -e "ORB_ENVS=AMP_URL ORB_TOKEN AMP_THREAD_ID AMP_SANDBOX_ID E2E_PROBE")
    local t=$SECONDS
    docker exec "${env[@]}" "$coord" orbd dispatch || fail "$1 $orb failed"
    echo "== $1 $orb: ok in $((SECONDS - t))s"
}

# Prints the latest counter sample as "PID N" after checking that every sample so far
# came from one process with a strictly increasing count: a count that restarts means
# processes were lost, one that repeats or goes back means stale RAM was replayed.
counter() { # RUNTIME
    console "$1" | grep -aoE '^counter pid=[0-9]+ n=[0-9]+' | awk -F'[= ]' '
        NR > 1 && ($3 != pid || $5 <= n) {
            printf "counter broke: pid=%s n=%s after pid=%s n=%s\n", $3, $5, pid, n > "/dev/stderr"
            bad = 1
        }
        { pid = $3; n = $5 }
        END { if (bad || NR == 0) exit 1; print pid, n }'
}
# Waits until the counter has advanced by two past N; prints "PID N".
wait_counter() { # RUNTIME N
    local deadline=$((SECONDS + 90)) pid n
    while :; do
        read -r pid n < <(counter "$1") || fail "$1 orb counter missing or not continuous"
        [ "$n" -lt $(($2 + 2)) ] || { echo "$pid $n"; return; }
        [ "$SECONDS" -lt "$deadline" ] || fail "$1 orb counter stuck at $n"
        sleep 1
    done
}
vmms() { docker top "$agent" -eo pid,args | grep -cE '[q]emu-system|[f]irecracker --api-sock' || true; }
connected() { docker exec "$coord" orbd agent list | has -P '^agent-1\tconnected\t'; }
probe() { # RUNTIME TARGET -> open|refused|dropped (last report)
    console "$1" | grep -aoE "^probe $2 [a-z]+" | tail -n 1 | cut -d' ' -f3
}

# CI runners have little free disk: hold less of it per running orb and in reserve.
cat >"$work/orbd.yaml" <<EOF
public_url: https://$coord:7443
running_orb_disk_headroom_gib: 1
offerings:
  - id: qemu-1vcpu-2gb
    runtime: qemu
    resources: { vcpus: 1, memory_mib: 2048, disk_gib: 4 }
  - id: firecracker-1vcpu-2gb
    runtime: firecracker
    resources: { vcpus: 1, memory_mib: 2048, disk_gib: 4 }
EOF
# No runtime section: the agent detects QEMU and Firecracker.
cat >"$work/agent.yaml" <<EOF
images:
  static_images:
    - { image_ref: "$orb_image", rootfs: /fixture/rootfs.ext4 }
EOF
chmod 0644 "$work"/*.yaml

docker network create "$id" >/dev/null
docker run -d --name "$coord" --network "$id" -v "$coord:/var/lib/orbd" \
    -v "$work/orbd.yaml:/etc/orbd/orbd.yaml:ro" "$image" coordinator >/dev/null
wait_for 30 "the dispatch socket" docker exec "$coord" test -S /var/lib/orbd/dispatch.sock
token=$(docker exec "$coord" orbd join-token agent-1)
docker run -d --name "$agent" --network "$id" --privileged --stop-timeout 300 \
    -v "$agent:/var/lib/orbd" -v "$work/agent.yaml:/etc/orbd/agent.yaml:ro" \
    -v "$rootfs:/fixture/rootfs.ext4:ro" -e "ORBD_JOIN=$token" -e ORBD_DISK_RESERVE_GIB=2 \
    -e ORBD_AGENT_CONFIG=/etc/orbd/agent.yaml -e ORBD_GUEST_NETWORK=10.200.0.0/24 \
    "$image" agent >/dev/null
wait_for 60 "the agent to connect" connected
agent_ip=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$agent")
echo "== agent-1 connected ($agent_ip)"

declare -A pid n
for rt in $runtimes; do
    dispatch spawn "$rt"
    read -r "pid[$rt]" "n[$rt]" < <(wait_counter "$rt" 0)
    echo "== $rt counter pid=${pid[$rt]} n=${n[$rt]}"
done

# Firecracker orbs reach the internet through NAT, never the agent's own namespace.
probed() { [ -n "$(probe firecracker "$agent_ip:9")" ]; }
wait_for 30 "the Firecracker orb's network probes" probed
egress=$(probe firecracker 1.1.1.1:53) inward=$(probe firecracker "$agent_ip:9")
echo "== firecracker orb: internet $egress, agent namespace $inward"
[ "$egress" = open ] || fail "the Firecracker orb cannot reach the internet ($egress)"
[ "$inward" = dropped ] || fail "the Firecracker orb reaches the agent's namespace ($inward)"
console firecracker | has -F '/dev/kvm is missing' || fail "the Firecracker orb has /dev/kvm"
# Both orbs use the agent's guest network: addressed in it, and out through its gateway.
for rt in $runtimes; do
    console "$rt" | has -F '10.200.0.15/24 via 10.200.0.2' ||
        fail "the $rt orb is not addressed in ORBD_GUEST_NETWORK"
done
qemu_probed() { [ -n "$(probe qemu 1.1.1.1:53)" ]; }
wait_for 30 "the QEMU orb's network probes" qemu_probed
[ "$(probe qemu 1.1.1.1:53)" = open ] || fail "the QEMU orb cannot reach the internet"

for rt in $runtimes; do dispatch pause "$rt"; done
[ "$(vmms)" = 0 ] || fail "a paused orb still has a VMM process"
for rt in $runtimes; do
    dispatch spawn "$rt"
    read -r p n[$rt] < <(wait_counter "$rt" "${n[$rt]}")
    [ "$p" = "${pid[$rt]}" ] || fail "$rt counter pid changed across pause/wake: ${pid[$rt]} -> $p"
    echo "== $rt after wake: counter pid=$p n=${n[$rt]}"
done

# docker stop: SIGTERM, then SIGKILL after --stop-timeout. The agent must drain (save
# the running orbs) and exit by itself.
t=$SECONDS
docker stop "$agent" >/dev/null
echo "== docker stop took $((SECONDS - t))s"
code=$(docker inspect -f '{{.State.ExitCode}}' "$agent")
[ "$code" = 0 ] || fail "agent container exited with $code"
docker logs "$agent" 2>&1 | strip | grep -E 'drain' | sed 's/^/   /'
docker logs "$agent" 2>&1 | strip | has -E 'drain finished saved=2 not_running=0 failed=0' ||
    fail "the agent did not save the running orbs on stop"
disconnected() { ! connected; }
wait_for 30 "the coordinator to drop the agent" disconnected

t=$SECONDS
docker start "$agent" >/dev/null
wait_for 120 "the agent to reconnect" connected
echo "== docker start: agent connected after $((SECONDS - t))s"
docker logs "$agent" 2>&1 | strip | has -E 'maintenance restore finished restored=2 not_restored=0' ||
    fail "the agent did not restore the orbs on start"
[ "$(vmms)" = 2 ] || fail "the restored orbs do not both have a VMM process"
for rt in $runtimes; do
    read -r p n[$rt] < <(wait_counter "$rt" "${n[$rt]}")
    [ "$p" = "${pid[$rt]}" ] || fail "$rt counter pid changed across docker stop/start: ${pid[$rt]} -> $p"
    echo "== $rt after docker stop/start: counter pid=$p n=${n[$rt]}"
done

for rt in $runtimes; do
    dispatch spawn "$rt"
    dispatch delete "$rt"
done
[ "$(vmms)" = 0 ] || fail "a deleted orb still has a VMM process"
echo "PASS"
