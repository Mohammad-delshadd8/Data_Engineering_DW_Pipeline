#!/bin/bash
# ============================================================
#  Airflow APISERVER wrapper
#  1. migrate metadata DB
#  2. create admin (idempotent)
#  3. start api-server (background)
#  4. wait for API health + DAGs
#  5. unpause all 3 DAGs + trigger full_load (once)
#  6. foreground api-server
# ============================================================
set -e

echo "[apiserver] ════════════════════════════════════════════════"
echo "[apiserver]  Step 1 — migrate metadata DB"
echo "[apiserver] ════════════════════════════════════════════════"
airflow db migrate

echo "[apiserver] Step 2 — create admin user (idempotent)..."
airflow users create \
  --username admin --password admin \
  --firstname Admin --lastname User \
  --role Admin --email admin@example.com || true

echo "[apiserver] Step 3 — start api-server in background..."
airflow api-server &
API_PID=$!
echo "[apiserver] api-server PID=$API_PID"

trap 'echo "[apiserver] Shutdown signal"; kill -TERM $API_PID 2>/dev/null; wait $API_PID; exit' TERM INT

echo "[apiserver] Step 4 — waiting for API health (max 3 min)..."
API_OK=0
for i in $(seq 1 90); do
  if ! kill -0 $API_PID 2>/dev/null; then
    echo "[apiserver] ❌ api-server process died"
    exit 1
  fi
  if python3 -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/api/v2/version', timeout=2)" 2>/dev/null; then
    echo "[apiserver] ✅ API healthy after ${i} checks"
    API_OK=1
    break
  fi
  sleep 2
done

if [ "$API_OK" != "1" ]; then
  echo "[apiserver] ⚠ API not healthy in 180s — continuing anyway"
fi

echo "[apiserver] Step 5 — waiting for DAGs to be parsed..."
DAG_OK=0
for i in $(seq 1 60); do
  if airflow dags list 2>/dev/null | grep -q "students_full_load"; then
    echo "[apiserver] ✅ DAGs detected after ${i} checks"
    DAG_OK=1
    break
  fi
  sleep 5
done

if [ "$DAG_OK" != "1" ]; then
  echo "[apiserver] ⚠ DAGs not detected — bootstrap skipped"
else
  echo "[apiserver] Step 6 — unpausing all 3 DAGs..."
  airflow dags unpause students_full_load 2>/dev/null || true
  airflow dags unpause students_dims_scd  2>/dev/null || true
  airflow dags unpause students_facts_cdc 2>/dev/null || true

  # Only trigger full_load if no successful run exists
  LAST_SUCCESS=$(airflow dags list-runs students_full_load -o plain 2>/dev/null | grep "success" | tail -1 || true)

  if [ -z "$LAST_SUCCESS" ]; then
    echo "[apiserver] Step 7 — first run — triggering full_load"
    airflow dags trigger students_full_load 2>/dev/null || true
    echo "[apiserver] ✅ full_load DAG triggered"
  else
    echo "[apiserver] Step 7 — full_load already succeeded — skipping trigger"
  fi
fi

echo "[apiserver] ════════════════════════════════════════════════"
echo "[apiserver] ✅ Bootstrap complete — api-server running"
echo "[apiserver] ════════════════════════════════════════════════"

wait $API_PID
