#!/bin/bash
# ============================================================
#  MSSQL Entrypoint — starts SQL Server, runs init, then foregrounds
#  Idempotent: marker file prevents re-init
# ============================================================
set +e

echo "[mssql-init] Starting SQL Server in background..."
/opt/mssql/bin/sqlservr &
MSSQL_PID=$!

trap 'echo "[mssql-init] Shutting down..."; kill -TERM $MSSQL_PID 2>/dev/null; wait $MSSQL_PID; exit' TERM INT

# --- Skip init if already done ---
if [ -f /var/opt/mssql/.initialized ]; then
  echo "[mssql-init] Already initialized — skipping init"
  wait $MSSQL_PID
  exit $?
fi

# --- Wait for readiness ---
echo "[mssql-init] Waiting for SQL Server readiness..."
READY=0
for i in $(seq 1 90); do
  if /opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -Q "SELECT 1" >/dev/null 2>&1; then
    READY=1
    echo "[mssql-init] ✅ SQL Server ready after ${i}s"
    break
  fi
  sleep 1
done

if [ "$READY" != "1" ]; then
  echo "[mssql-init] ⚠ SQL Server did not become ready in 90s — skipping init"
  wait $MSSQL_PID
  exit $?
fi

# --- Run init (never fails the container) ---
if [ -f /backup/restore.sh ]; then
  bash /backup/restore.sh
else
  echo "[mssql-init] ⚠ /backup/restore.sh not found — skipping"
fi

touch /var/opt/mssql/.initialized
echo "[mssql-init] ✅ Initialization complete"

# --- Keep container alive ---
wait $MSSQL_PID
