#!/bin/bash
set -e
echo "[dag-processor] Running migrate (safe no-op if already done)..."
airflow db migrate
echo "[dag-processor] Starting dag-processor..."
exec airflow dag-processor
