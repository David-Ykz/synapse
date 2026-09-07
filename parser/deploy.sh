#!/usr/bin/env bash
# Generates k8s manifests from a swarm config and applies them to the current
# kubectl context. Usage: deploy.sh <swarm.yaml> <output_dir>
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 <swarm.yaml> <output_dir>" >&2
  exit 1
fi

SWARM_CONFIG="$1"
OUTPUT_DIR="$2"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

go run "$SCRIPT_DIR" "$SWARM_CONFIG" "$OUTPUT_DIR"
kubectl apply -f "$OUTPUT_DIR/generated"
