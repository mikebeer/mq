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
bg() {      # bg <name> [args...]   start in the background, pid in BGPID (the real rxvm, not a subshell)
    local n="$1"; shift
    rxvm "$n.rxbin" rx_rxmq "$CREXX_HOME/bin/library" -a "$@" &
    BGPID=$!
}
check() {   # check <label> <expected> <actual>
    if [ "$2" = "$3" ]; then echo "[$(ts)] ok    $1"
    else echo "[$(ts)] FAIL  $1: expected [$2] got [$3]"; fails=$((fails + 1)); fi
}

build mq_smoke  "$REPO/tests/mq_smoke.crexx"
build mqserver  "$REPO/rabbitmq-crexx-server/mqserver.crexx"
build mqclient  "$REPO/rabbitmq-crexx-client/mqclient.crexx"
build mq_pubsub "$REPO/tests/mq_pubsub.crexx"
build mq_dead    "$REPO/tests/mq_dead.crexx"
build ex_hello  "$REPO/examples/hello.crexx"
build ex_worker "$REPO/examples/worker.crexx"
build ex_caller "$REPO/examples/caller.crexx"
echo "[$(ts)] built mq_smoke, mq_pubsub, mqserver, mqclient and the examples"

out=$(rxvm mq_smoke.rxbin rx_rxmq "$CREXX_HOME/bin/library" 2>&1)
check "smoke test" "PASS: rxmq smoke" "$out"
out=$(rxvm mq_pubsub.rxbin rx_rxmq "$CREXX_HOME/bin/library" 2>&1)
check "fanout publish/subscribe, dropped message" "PASS: rxmq publish/subscribe" "$out"

Q=rxmq.test.requests
C="run mqclient -b $MQ_BROKER -t 8000"

# fresh queue state
$C purge $Q >/dev/null 2>&1

# server handles 8 requests then stops by itself
bg mqserver -b "$MQ_BROKER" -n 8 $Q >server.log 2>&1
SERVER=$BGPID
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
bg mqserver -b "$MQ_BROKER" -n 1 $D >server2.log 2>&1
SERVER2=$BGPID
sleep 2
rabbitmqctl delete_queue $D >/dev/null 2>&1
sleep 8
check "rpc after queue deletion" "PONG" "$($C rpc $D PING 2>&1)"
for _ in $(seq 1 10); do kill -0 $SERVER2 2>/dev/null || break; sleep 0.5; done
kill $SERVER2 2>/dev/null; wait $SERVER2 2>/dev/null
check "server noticed the lost queue" "1" "$(grep -c 'is gone - re-creating' server2.log)"
$C purge $D >/dev/null 2>&1

# two servers on one queue share the work (the broker hands out one request at a time)
W=rxmq.test.shared
$C purge $W >/dev/null 2>&1
bg mqserver -b "$MQ_BROKER" -V $W >w1.log 2>&1
W1=$BGPID
bg mqserver -b "$MQ_BROKER" -V $W >w2.log 2>&1
W2=$BGPID
sleep 2
for n in 1 2 3 4 5 6 7 8; do $C rpc $W echo job$n >/dev/null 2>&1; done
sleep 1
kill $W1 $W2 2>/dev/null; wait $W1 $W2 2>/dev/null
n1=$(grep -c '^request:' w1.log); n2=$(grep -c '^request:' w2.log)
check "two servers served all 8 requests" "8" "$((n1 + n2))"
check "both servers took part" "1" "$([ "$n1" -gt 0 ] && [ "$n2" -gt 0 ] && echo 1 || echo 0)"
$C purge $W >/dev/null 2>&1

# the example programs of the manual
rabbitmqctl delete_queue hello >/dev/null 2>&1
out=$(rxvm ex_hello.rxbin rx_rxmq "$CREXX_HOME/bin/library" 2>&1)
check "example hello" "received: Hello, broker" "$out"
J=rxmq.test.examples
$C purge $J >/dev/null 2>&1
bg mqserver -b "$MQ_BROKER" -n 1 $J >server3.log 2>&1
S3=$BGPID
sleep 2
out=$(rxvm ex_caller.rxbin rx_rxmq "$CREXX_HOME/bin/library" -a $J 2>&1)
check "example caller" "reply: HELLO (correlation id req-1)" "$out"
kill $S3 2>/dev/null; wait $S3 2>/dev/null
bg ex_worker $J >worker.log 2>&1
W3=$BGPID
sleep 2
$C send $J first job >/dev/null 2>&1
sleep 1
kill $W3 2>/dev/null; wait $W3 2>/dev/null
check "example worker" "1" "$(grep -c 'working on: first job' worker.log)"
$C purge $J >/dev/null 2>&1

# the broker closes the connection under a waiting program: every call must say so (-6), none may look like a timeout
bg mq_dead >dead.log 2>&1
DEAD=$BGPID
sleep 2.5
rabbitmqctl close_all_connections "rxmq test: close under a waiting program" >/dev/null 2>&1
for _ in $(seq 1 30); do kill -0 $DEAD 2>/dev/null || break; sleep 0.5; done
kill $DEAD 2>/dev/null; wait $DEAD 2>/dev/null
check "connection closed by the broker" "receive -6 publish -6 queue -6 receive again -6|disconnect 0" "$(paste -sd'|' dead.log)"

# a server whose connection the broker closes must stop with exit code 1
bg mqserver -b "$MQ_BROKER" rxmq.test.dead2 >server4.log 2>&1
S4=$BGPID
sleep 2.5
rabbitmqctl close_all_connections "rxmq test: close under a server" >/dev/null 2>&1
for _ in $(seq 1 20); do kill -0 $S4 2>/dev/null || break; sleep 0.5; done
if kill -0 $S4 2>/dev/null; then kill $S4 2>/dev/null; wait $S4 2>/dev/null; echo "[$(ts)] FAIL  server did not stop after the broker closed its connection"; fails=$((fails + 1))
else wait $S4; check "server exit code after connection loss" "1" "$?"; fi

# with publisher confirms a request to a queue that does not exist fails at once instead of timing out
rabbitmqctl delete_queue rxmq.test.nobody >/dev/null 2>&1
start=$SECONDS
out=$($C rpc rxmq.test.nobody PING 2>&1)
check "rpc to a missing queue fails fast" "1" "$([ $((SECONDS - start)) -lt 4 ] && echo 1 || echo 0)"
check "rpc to a missing queue says why" "1" "$(echo "$out" | grep -c 'not delivered to any queue')"

# bad broker address must fail cleanly
$C -b 127.0.0.1 -P 1 get $G >/dev/null 2>&1
check "unreachable broker exit code" "1" "$?"

if [ "$fails" -eq 0 ]; then echo "[$(ts)] ALL TESTS PASSED"; exit 0; fi
echo "[$(ts)] $fails TEST(S) FAILED"; exit 1
