#!/usr/bin/env bash
# Dev environment for måqtt: a Mosquitto Broker in a container plus the traffic generator.
#
#   dev/env.sh up      start the Broker (detached) and run trafficgen in the foreground
#   dev/env.sh broker  start the Broker only
#   dev/env.sh down    stop and remove the Broker
#   dev/env.sh logs    follow the Broker's logs
#
# Extra arguments after `up` are passed to trafficgen, e.g. `dev/env.sh up --fleet 10000`.
# Set CONTAINER=docker to use Docker instead of Podman.
set -euo pipefail

CONTAINER="${CONTAINER:-podman}"
IMAGE="docker.io/library/eclipse-mosquitto:2"
NAME="maqtt-dev-broker"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BROKER_DIR="$ROOT/dev/broker"
GEN="$BROKER_DIR/generated"
USERNAME="maqtt"
# Host ports are offset from the MQTT defaults so the dev Broker can run next to a local Mosquitto.
PORT_TCP=21883 PORT_TCP_AUTH=21884 PORT_TLS=28883 PORT_MTLS=28884
PASSWORD="maqtt"

# SELinux hosts (Fedora) need volumes relabelled; macOS does not understand the flag.
VOLUME_OPTS="ro"
if [[ "$(uname)" == "Linux" ]]; then VOLUME_OPTS="ro,Z"; fi

generate() {
  [[ -f "$GEN/passwd" && -f "$GEN/client.crt" ]] && return
  echo "env: generating certificates and password file in $GEN"
  mkdir -p "$GEN"
  local days=3650 subj="/O=maqtt dev"

  openssl req -x509 -new -nodes -newkey rsa:2048 -days "$days" \
    -keyout "$GEN/ca.key" -out "$GEN/ca.crt" -subj "$subj/CN=maqtt dev CA" 2>/dev/null

  openssl req -new -nodes -newkey rsa:2048 \
    -keyout "$GEN/server.key" -out "$GEN/server.csr" -subj "$subj/CN=localhost" 2>/dev/null
  openssl x509 -req -in "$GEN/server.csr" -CA "$GEN/ca.crt" -CAkey "$GEN/ca.key" -CAcreateserial \
    -days "$days" -out "$GEN/server.crt" \
    -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1,IP:::1") 2>/dev/null

  openssl req -new -nodes -newkey rsa:2048 \
    -keyout "$GEN/client.key" -out "$GEN/client.csr" -subj "$subj/CN=device-01" 2>/dev/null
  openssl x509 -req -in "$GEN/client.csr" -CA "$GEN/ca.crt" -CAkey "$GEN/ca.key" -CAcreateserial \
    -days "$days" -out "$GEN/client.crt" \
    -extfile <(printf "extendedKeyUsage=clientAuth") 2>/dev/null

  rm -f "$GEN"/*.csr "$GEN"/*.srl
  "$CONTAINER" run --rm -v "$GEN:/gen:${VOLUME_OPTS/ro/rw}" "$IMAGE" \
    mosquitto_passwd -b -c /gen/passwd "$USERNAME" "$PASSWORD"
  # Mosquitto runs as an unprivileged user inside the container and must be able to read these.
  # Dev-only material, so world-readable is acceptable.
  chmod 644 "$GEN"/*
}

broker() {
  generate
  "$CONTAINER" run -d --replace --name "$NAME" \
    -p "$PORT_TCP:1883" -p "$PORT_TCP_AUTH:1884" -p "$PORT_TLS:8883" -p "$PORT_MTLS:8884" \
    -v "$BROKER_DIR/mosquitto.conf:/mosquitto/config/mosquitto.conf:$VOLUME_OPTS" \
    -v "$GEN:/mosquitto/generated:$VOLUME_OPTS" \
    "$IMAGE" >/dev/null
  echo "env: Broker '$NAME' listening on"
  echo "       $PORT_TCP  TCP, anonymous"
  echo "       $PORT_TCP_AUTH  TCP, $USERNAME / $PASSWORD"
  echo "       $PORT_TLS  TLS, $USERNAME / $PASSWORD, CA $GEN/ca.crt"
  echo "       $PORT_MTLS  mTLS, client cert $GEN/client.crt + client.key (username device-01)"
}

case "${1:-up}" in
  up)
    shift || true
    broker
    exec cargo run --quiet --manifest-path "$ROOT/Cargo.toml" -p trafficgen -- --port "$PORT_TCP" "$@"
    ;;
  broker) broker ;;
  down) "$CONTAINER" rm -f "$NAME" >/dev/null && echo "env: Broker removed" ;;
  logs) exec "$CONTAINER" logs -f "$NAME" ;;
  *) sed -n '2,11p' "$0"; exit 1 ;;
esac
