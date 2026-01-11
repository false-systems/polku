#!/bin/bash
# Start Polku gateway with AHTI emitter

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export POLKU_EMIT_AHTI_ENDPOINT="http://[::1]:50052"
export POLKU_METRICS_ADDR="127.0.0.1:9095"
export POLKU_LOG_LEVEL="info"

cd "$SCRIPT_DIR"
exec ./target/release/polku-gateway
