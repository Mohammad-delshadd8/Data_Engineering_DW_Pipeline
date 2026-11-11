#!/bin/bash
set -e
echo "[scheduler] Running migrate (safe no-op if already done)..."
airflow db migrate
echo "[scheduler] Starting scheduler..."
exec airflow scheduler
