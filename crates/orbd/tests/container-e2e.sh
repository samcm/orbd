#!/usr/bin/env bash
# End to end check of orbd's container image as deployed (see the README): a coordinator
# container and a privileged agent container joined with `orbd join-token`, jobs sent
# through the coordinator's dispatch socket with `orbd dispatch` (as `amp orb provider`
# runs them), then a real `docker stop` / `docker start` of the agent with a running orb.
#
#   container-e2e.sh IMAGE ROOTFS
#
# ROOTFS is the KVM test root disk (kvm-guest/build-rootfs.sh), served through the
# agent's `static_images`. Its counter process prints `counter pid=<pid> n=<n>` every
# second; it must keep its pid and count through pause/wake and through the agent
# container's restart. Needs Docker and /dev/kvm; nested virtualization is not needed.
set -euo pipefail
image=$1
rootfs=$(realpath "$2")
id=orbd-e2e-$$
coord=$id-coordinator agent=$id-agent
offering=qemu-2vcpu-2gb
orb_image=example.invalid/orbd-e2e:latest
work=$(mktemp -d)

strip() { sed 's/\x1b\[[0-9;]*m//g'; }
console() {
    docker run --rm --network none -v "$agent:/s:ro" --entrypoint sh "$image" \
        -c 'cat /s/orbs/*/logs/console.log 2>/dev/null' || true
}
cleanup() {
    local status=$?
    if [ "$status" != 0 ]; then
        for c in "$coord" "$agent"; do
            echo "==== docker logs $c (last 60 lines)"
            docker logs --tail 60 "$c" 2>&1 | strip || true
        done
        echo "==== orb console (last 40 lines)"
        console | tail -n 40
    fi
    docker rm -f "$coord" "$agent" >/dev/null 2>&1 || true
    docker volume rm "$coord" "$agent" >/dev/null 2>&1 || true
    docker network rm "$id" >/dev/null 2>&1 || true
    rm -rf "$work"
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }
wait_for() { # SECONDS WHAT COMMAND...
    local deadline=$((SECONDS + $1)) what=$2
    shift 2
    until "$@" >/dev/null 2>&1; do
        [ "$SECONDS" -lt "$deadline" ] || fail "timed out waiting for $what"
        sleep 0.5
    done
}

# One Amp lifecycle job, with the environment `amp orb provider` gives `orbd dispatch`.
dispatch() { # OPERATION ORB
    local env=(-e ORBD_DISPATCH_SOCKET=/var/lib/orbd/dispatch.sock -e "ORBD_OFFERING_ID=$offering"
        -e "ORB_OPERATION=$1" -e "ORB_ID=$2" -e "ORB_SANDBOX_ID=sandbox-$2"
        -e AMP_URL=https://ampcode.com -e "AMP_THREAD_ID=T-$2" -e "AMP_SANDBOX_ID=orb-provider:e2e:$2")
    [ "$1" != spawn ] || env+=(-e "ORB_IMAGE=$orb_image" -e ORB_TOKEN=e2e-orb-token
        -e "ORB_ENVS=AMP_URL ORB_TOKEN AMP_THREAD_ID AMP_SANDBOX_ID")
    local t=$SECONDS
    docker exec "${env[@]}" "$coord" orbd dispatch || fail "$1 $2 failed"
    echo "== $1 $2: ok in $((SECONDS - t))s"
}

# Prints the latest counter sample as "PID N" after checking that every sample so far
# came from one process with a strictly increasing count: a count that restarts means
# processes were lost, one that repeats or goes back means stale RAM was replayed.
counter() {
    console | grep -aoE '^counter pid=[0-9]+ n=[0-9]+' | awk -F'[= ]' '
        NR > 1 && ($3 != pid || $5 <= n) {
            printf "counter broke: pid=%s n=%s after pid=%s n=%s\n", $3, $5, pid, n > "/dev/stderr"
            bad = 1
        }
        { pid = $3; n = $5 }
        END { if (bad || NR == 0) exit 1; print pid, n }'
}
# Waits until the counter has advanced by two past N; prints "PID N".
wait_counter() { # N
    local deadline=$((SECONDS + 90)) pid n
    while :; do
        read -r pid n < <(counter) || fail "orb counter missing or not continuous"
        [ "$n" -lt $(($1 + 2)) ] || { echo "$pid $n"; return; }
        [ "$SECONDS" -lt "$deadline" ] || fail "orb counter stuck at $n"
        sleep 1
    done
}
vmms() { docker top "$agent" -eo pid,args | grep -c '[q]emu-system' || true; }
connected() { docker exec "$coord" orbd agent list | grep -qP '^agent-1\tconnected\t'; }

cat >"$work/orbd.yaml" <<EOF
public_url: https://$coord:7443
offerings:
  - id: $offering
    runtime: qemu
    resources: { vcpus: 2, memory_mib: 2048, disk_gib: 4 }
EOF
cat >"$work/agent.yaml" <<EOF
runtime:
  qemu:
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
    -v "$rootfs:/fixture/rootfs.ext4:ro" -e "ORBD_JOIN=$token" \
    -e ORBD_AGENT_CONFIG=/etc/orbd/agent.yaml "$image" agent >/dev/null
wait_for 60 "the agent to connect" connected
echo "== agent-1 connected"

dispatch spawn e2e-1
read -r pid n < <(wait_counter 0)
echo "== counter pid=$pid n=$n"

dispatch pause e2e-1
[ "$(vmms)" = 0 ] || fail "a paused orb still has a QEMU process"
dispatch spawn e2e-1
read -r pid2 n < <(wait_counter "$n")
[ "$pid2" = "$pid" ] || fail "counter pid changed across pause/wake: $pid -> $pid2"
echo "== after wake: counter pid=$pid n=$n"

# docker stop: SIGTERM, then SIGKILL after --stop-timeout. The agent must drain (save
# the running orb) and exit by itself.
read -r _ n < <(counter)
t=$SECONDS
docker stop "$agent" >/dev/null
echo "== docker stop took $((SECONDS - t))s"
code=$(docker inspect -f '{{.State.ExitCode}}' "$agent")
[ "$code" = 0 ] || fail "agent container exited with $code"
docker logs "$agent" 2>&1 | strip | grep -E 'drain' | sed 's/^/   /'
docker logs "$agent" 2>&1 | strip | grep -qE 'drain finished saved=1 not_running=0 failed=0' ||
    fail "the agent did not save the running orb on stop"
disconnected() { ! connected; }
wait_for 30 "the coordinator to drop the agent" disconnected

t=$SECONDS
docker start "$agent" >/dev/null
wait_for 120 "the agent to reconnect" connected
echo "== docker start: agent connected after $((SECONDS - t))s"
docker logs "$agent" 2>&1 | strip | grep -qE 'maintenance restore finished restored=1 not_restored=0' ||
    fail "the agent did not restore the orb on start"
[ "$(vmms)" = 1 ] || fail "the restored orb has no QEMU process"
read -r pid2 n < <(wait_counter "$n")
[ "$pid2" = "$pid" ] || fail "counter pid changed across docker stop/start: $pid -> $pid2"
echo "== after docker stop/start: counter pid=$pid n=$n"

dispatch spawn e2e-1
dispatch delete e2e-1
[ "$(vmms)" = 0 ] || fail "a deleted orb still has a QEMU process"
echo "PASS"
