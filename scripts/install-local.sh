#!/usr/bin/env bash
# Install cREXX and RabbitMQ locally (Debian/Ubuntu) and build the rxmq plugin.
#
#   scripts/install-local.sh            build + install everything
#
# Environment:
#   CREXX_SRC     cREXX checkout       (default $HOME/crexx-src, cloned if missing)
#   CREXX_BUILD   cREXX build tree     (default $HOME/crexx-build)
#   CREXX_HOME    install prefix       (default /opt/crexx)
set -eu
REPO="$(cd "$(dirname "$0")/.." && pwd)"
CREXX_SRC="${CREXX_SRC:-$HOME/crexx-src}"
CREXX_BUILD="${CREXX_BUILD:-$HOME/crexx-build}"
CREXX_HOME="${CREXX_HOME:-/opt/crexx}"
SUDO=""; [ "$(id -u)" -eq 0 ] || SUDO="sudo"
step() { echo "[$(date +%H:%M:%S)] $*"; }

step "1/5 system packages: RabbitMQ broker, librabbitmq, build tools"
export DEBIAN_FRONTEND=noninteractive
$SUDO apt-get update -qq
$SUDO apt-get install -y --no-install-recommends \
    rabbitmq-server librabbitmq-dev build-essential cmake git pkg-config

step "2/5 cREXX source"
[ -d "$CREXX_SRC/.git" ] || git clone --depth 1 https://github.com/adesutherland/crexx "$CREXX_SRC"

step "3/5 build and install cREXX into $CREXX_HOME (this takes a while)"
cmake -S "$CREXX_SRC" -B "$CREXX_BUILD" -DCMAKE_BUILD_TYPE=Release
cmake --build "$CREXX_BUILD" --target stage-product --parallel "$(nproc)"
cmake --build "$CREXX_BUILD" --target crexx-examples --parallel "$(nproc)"   # install step needs these
$SUDO cmake --install "$CREXX_BUILD" --prefix "$CREXX_HOME"

step "4/5 start the RabbitMQ broker"
if rabbitmqctl status >/dev/null 2>&1; then
    echo "broker already running"
elif command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
    $SUDO systemctl enable --now rabbitmq-server
else
    $SUDO su -s /bin/bash rabbitmq -c 'HOME=/var/lib/rabbitmq rabbitmq-server -detached'
fi
for _ in $(seq 1 60); do rabbitmqctl await_startup >/dev/null 2>&1 && break; sleep 2; done

step "5/5 build the rxmq plugin"
cmake -S "$REPO" -B "$REPO/build" -DCREXX_SOURCE_DIR="$CREXX_SRC" -DCREXX_BUILD_DIR="$CREXX_BUILD"
cmake --build "$REPO/build"

echo
echo "Done.  cREXX: $CREXX_HOME/bin   plugin: $REPO/build/bin/rx_rxmq.rxplugin"
echo "Run the tests with: CREXX_HOME=$CREXX_HOME $REPO/tests/run_tests.sh"
