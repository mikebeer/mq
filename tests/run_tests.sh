#!/usr/bin/env bash
# Build the REXX programs and run the rxmq tests against a local RabbitMQ.
#
#   CREXX_HOME   installed cREXX prefix            (default /opt/crexx)
#   MQ_PLUGIN    directory with rx_rxmq.rxplugin   (default <repo>/build/bin)
#   MQ_BROKER    broker address                    (default localhost)
set -u
REPO="$(cd "$(dirname "$0")/.." && pwd)"
CREXX_HOME="${CREXX_HOME:-/opt/crexx}"
MQ_PLUGIN="${MQ_PLUGIN:-$REPO/build/bin}"
MQ_BROKER="${MQ_BROKER:-localhost}"
OUT="$REPO/build/t"
export PATH="$CREXX_HOME/bin:$PATH"
mkdir -p "$OUT"
cd "$OUT" || exit 1
cp "$MQ_PLUGIN/rx_rxmq.rxplugin" .

fails=0
ts() { date +%H:%M:%S; }

build() {   # build <name> <source>
    rxc -i "$CREXX_HOME/bin" -i "$MQ_PLUGIN" -o "$1" "$2" >"$1.build.log" 2>&1 \
        && rxas -o "$1" "$1" >>"$1.build.log" 2>&1 \
        || { echo "[$(ts)] BUILD FAILED: $2"; cat "$1.build.log"; exit 1; }
}
run() {     # run <name> [args...]
    local n="$1"; shift
    rxvm "$n.rxbin" rx_rxmq "$CREXX_HOME/bin/library" -a "$@"
}
check() {   # check <label> <expected> <actual>
    if [ "$2" = "$3" ]; then echo "[$(ts)] ok    $1"
    else echo "[$(ts)] FAIL  $1: expected [$2] got [$3]"; fails=$((fails + 1)); fi
}

build mq_smoke  "$REPO/tests/mq_smoke.crexx"
build mqserver  "$REPO/rabbitmq-crexx-server/mqserver.crexx"
build mqclient  "$REPO/rabbitmq-crexx-client/mqclient.crexx"
echo "[$(ts)] built mq_smoke, mqserver, mqclient"

out=$(rxvm mq_smoke.rxbin rx_rxmq "$CREXX_HOME/bin/library" 2>&1)
check "smoke test" "PASS: rxmq smoke" "$out"

Q=rxmq.test.requests
C="run mqclient -b $MQ_BROKER -t 8000"

# fresh queue state
$C purge $Q >/dev/null 2>&1

# server handles 8 requests then stops by itself
run mqserver -b "$MQ_BROKER" -n 8 $Q >server.log 2>&1 &
SERVER=$!
sleep 2

check "rpc PING"          "PONG"                "$($C rpc $Q PING 2>&1)"
check "rpc ECHO"          "hello rabbit"        "$($C rpc $Q echo hello rabbit 2>&1)"
check "rpc UPPER"         "HELLO WORLD"         "$($C rpc $Q upper hello world 2>&1)"
check "rpc LOWER"         "abc"                 "$($C rpc $Q LOWER ABC 2>&1)"
check "rpc REVERSE"       "cba"                 "$($C rpc $Q reverse abc 2>&1)"
check "rpc WORDS"         "4"                   "$($C rpc $Q words one two three four 2>&1)"
check "rpc unknown verb"  "ERROR unknown verb FROB" "$($C rpc $Q frob x 2>&1)"
check "rpc STOP"          "stopping"            "$($C rpc $Q stop 2>&1)"

# bounded wait: never let a stuck server hang the test run
for _ in $(seq 1 20); do kill -0 $SERVER 2>/dev/null || break; sleep 0.5; done
if kill -0 $SERVER 2>/dev/null; then
    kill $SERVER 2>/dev/null; wait $SERVER 2>/dev/null
    echo "[$(ts)] FAIL  server did not stop by itself"; fails=$((fails + 1))
else
    wait $SERVER; check "server exit code" "0" "$?"
fi
check "server stop line" "mqserver: stopped after 8 request(s)" "$(tail -1 server.log)"

# no server any more: an rpc must time out with exit code 1
$C -t 700 rpc $Q PING >/dev/null 2>&1
check "rpc timeout exit code" "1" "$?"
$C purge $Q >/dev/null 2>&1

# plain send / get round trip through a durable queue
G=rxmq.test.mailbox
$C purge $G >/dev/null 2>&1
check "send" "sent to $G" "$($C send $G first message 2>&1)"
check "get"  "first message" "$($C get $G 2>&1)"
$C -t 500 get $G >/dev/null 2>&1
check "get on empty queue exit code" "1" "$?"

# a broker side error must not stall the client (regression: missing channel.close-ok)
rabbitmqctl delete_queue rxmq.test.absent >/dev/null 2>&1
start=$SECONDS
$C purge rxmq.test.absent >/dev/null 2>&1
check "purge of missing queue returns in under 5s" "1" "$([ $((SECONDS - start)) -lt 5 ] && echo 1 || echo 0)"

# the broker silently drops a consumer when its queue is deleted: the server must notice,
# re-create the queue, resubscribe and keep serving (idle check runs every ~5 s)
D=rxmq.test.doomed
$C purge $D >/dev/null 2>&1
run mqserver -b "$MQ_BROKER" -n 1 $D >server2.log 2>&1 &
SERVER2=$!
sleep 2
rabbitmqctl delete_queue $D >/dev/null 2>&1
sleep 8
check "rpc after queue deletion" "PONG" "$($C rpc $D PING 2>&1)"
for _ in $(seq 1 10); do kill -0 $SERVER2 2>/dev/null || break; sleep 0.5; done
kill $SERVER2 2>/dev/null; wait $SERVER2 2>/dev/null
check "server noticed the lost queue" "1" "$(grep -c 'is gone - re-creating' server2.log)"
$C purge $D >/dev/null 2>&1

# bad broker address must fail cleanly
$C -b 127.0.0.1 -P 1 get $G >/dev/null 2>&1
check "unreachable broker exit code" "1" "$?"

if [ "$fails" -eq 0 ]; then echo "[$(ts)] ALL TESTS PASSED"; exit 0; fi
echo "[$(ts)] $fails TEST(S) FAILED"; exit 1
