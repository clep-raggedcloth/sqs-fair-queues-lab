#!/usr/bin/env bash

set -euo pipefail

cd /workspace

mkdir -p build results
go mod download

endpoint="${AWS_ENDPOINT:-http://ministack:4566}"

for attempt in $(seq 1 30); do
  if aws --endpoint-url "${endpoint}" sqs list-queues >/dev/null 2>&1; then
    echo "Ministack is ready: ${endpoint}"
    exit 0
  fi
  sleep 1
done

echo "Ministack did not become ready within 30 seconds: ${endpoint}" >&2
exit 1
