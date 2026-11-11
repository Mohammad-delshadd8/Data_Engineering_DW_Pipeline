#!/bin/bash
# ============================================================
#  Restore .bak + enable CDC — idempotent, never fails
# ============================================================
set +e

SQLCMD=/opt/mssql-tools18/bin/sqlcmd
SERVER=localhost
USER=sa
PASS="${MSSQL_SA_PASSWORD:-YourStrong!Passw0rd}"
DIR=/backup

echo "[restore] ═══════════════════════════════════════════════"
echo "[restore]  Checking current state"
echo "[restore] ═══════════════════════════════════════════════"

STU=$(/opt/mssql-tools18/bin/sqlcmd -S $SERVER -U $USER -P "$PASS" -C -h -1 -W -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.databases WHERE name='Students'" 2>/dev/null | tr -d ' \r\n')
DW=$(/opt/mssql-tools18/bin/sqlcmd -S $SERVER -U $USER -P "$PASS" -C -h -1 -W -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM sys.databases WHERE name='StudentsDW'" 2>/dev/null | tr -d ' \r\n')

echo "[restore] Students: $([ "$STU" = "1" ] && echo 'EXISTS' || echo 'MISSING')"
echo "[restore] StudentsDW: $([ "$DW" = "1" ] && echo 'EXISTS' || echo 'MISSING')"

# --- Both exist → only check CDC ---
if [ "$STU" = "1" ] && [ "$DW" = "1" ]; then
  CDC=$(/opt/mssql-tools18/bin/sqlcmd -S $SERVER -U $USER -P "$PASS" -C -h -1 -W -Q "SET NOCOUNT ON; SELECT is_cdc_enabled FROM sys.databases WHERE name='Students'" 2>/dev/null | tr -d ' \r\n')
  if [ "$CDC" = "1" ]; then
    echo "[restore] ✅ Both DBs + CDC already in place — nothing to do"
    exit 0
  else
    echo "[restore] Enabling CDC (idempotent)..."
    /opt/mssql-tools18/bin/sqlcmd -S $SERVER -U $USER -P "$PASS" -C -d Students -i "$DIR/enable_native_cdc.sql" 2>&1 | grep -E "CDC enabled|already" || true
    exit 0
  fi
fi

# --- Check .bak availability ---
if [ ! -f "$DIR/Students.bak" ] || [ ! -f "$DIR/Delshad_studentsDW.bak" ]; then
  echo "[restore] ╔════════════════════════════════════════════════════════╗"
  echo "[restore] ║  ⚠  .bak files NOT FOUND                              ║"
  echo "[restore] ║                                                        ║"
  echo "[restore] ║  Copy these files to init-scripts/mssql/ on host:    ║"
  echo "[restore] ║    - Students.bak              (~260 MB)              ║"
  echo "[restore] ║    - Delshad_studentsDW.bak    (~1.2 GB)              ║"
  echo "[restore] ║                                                        ║"
  echo "[restore] ║  Then: docker compose restart mssql                   ║"
  echo "[restore] ╚════════════════════════════════════════════════════════╝"
  exit 0
fi

# --- Restore Students ---
if [ "$STU" = "0" ]; then
  echo "[restore] Restoring Students..."
  /opt/mssql-tools18/bin/sqlcmd -S $SERVER -U $USER -P "$PASS" -C -b -Q "
    RESTORE DATABASE Students FROM DISK = '$DIR/Students.bak'
    WITH MOVE 'StudentDatabase' TO '/var/opt/mssql/data/Students.mdf',
         MOVE 'StudentDatabase_log' TO '/var/opt/mssql/data/Students_log.ldf', REPLACE"
  [ $? -eq 0 ] && echo "[restore] ✅ Students restored" || echo "[restore] ⚠ restore failed"
fi

# --- Restore StudentsDW ---
if [ "$DW" = "0" ]; then
  echo "[restore] Restoring StudentsDW..."
  /opt/mssql-tools18/bin/sqlcmd -S $SERVER -U $USER -P "$PASS" -C -b -Q "
    RESTORE DATABASE StudentsDW FROM DISK = '$DIR/Delshad_studentsDW.bak'
    WITH MOVE 'StudentsDW' TO '/var/opt/mssql/data/StudentsDW.mdf',
         MOVE 'StudentsDW_log' TO '/var/opt/mssql/data/StudentsDW_log.ldf', REPLACE"
  [ $? -eq 0 ] && echo "[restore] ✅ StudentsDW restored" || echo "[restore] ⚠ restore failed"
fi

# --- Fix sa login + ownership ---
/opt/mssql-tools18/bin/sqlcmd -S $SERVER -U $USER -P "$PASS" -C -Q "
  ALTER LOGIN [sa] WITH DEFAULT_DATABASE = [master];
  ALTER DATABASE Students SET MULTI_USER WITH ROLLBACK IMMEDIATE;
  ALTER AUTHORIZATION ON DATABASE::Students TO sa;" 2>&1 | grep -v "Changed database" || true

# --- Enable CDC ---
echo "[restore] Enabling CDC..."
/opt/mssql-tools18/bin/sqlcmd -S $SERVER -U $USER -P "$PASS" -C -d Students -i "$DIR/enable_native_cdc.sql" 2>&1 | grep -E "CDC enabled|already" || true

echo "[restore] ✅ Complete"
exit 0
