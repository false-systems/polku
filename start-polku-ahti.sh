#!/bin/bash
# Start Polku gateway with AHTI emitter

export POLKU_EMIT_AHTI_ENDPOINT="http://[::1]:50052"
export POLKU_METRICS_ADDR="127.0.0.1:9095"
export POLKU_LOG_LEVEL="info"

cd /Users/yair/projects/polku
exec ./target/release/polku-gateway
