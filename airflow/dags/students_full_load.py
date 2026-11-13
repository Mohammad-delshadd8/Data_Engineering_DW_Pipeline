"""
DAG 1: Full Load
================
This DAG acts as the foundational pipeline. It extracts all raw data from MSSQL,
transforms small dimensions via Spark, and loads large tables via Python (baseline).

It uses `overwrite` and `TRUNCATE` logic internally to ensure no duplicate data
is created if this DAG is re-run.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

SPARK_JOBS = "/opt/airflow/spark-jobs"
JARS = f"{SPARK_JOBS}/jars"

# JDBC JARs required for Spark to connect to MSSQL, PostgreSQL, and ClickHouse
MSSQL_JAR    = f"{JARS}/mssql-jdbc-12.8.1.jre11.jar"
POSTGRES_JAR = f"{JARS}/postgresql-42.7.4.jar"
CH_SPARK_JAR = f"{JARS}/clickhouse-spark-runtime-3.5_2.12-0.9.0.jar"
CH_JDBC_JAR  = f"{JARS}/clickhouse-jdbc-0.9.4-all.jar"

# Crucial: Setting env variables because Airflow's BashOperator overrides the system env by default.
# Without this, spark-submit will fail with "JAVA_HOME is not set".
ENV_BASE = {
    "JAVA_HOME": "/opt/java",
    "SPARK_HOME": "/opt/spark",
    "PATH": "/opt/java/bin:/opt/spark/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
    "HOME": "/home/airflow",
}

with DAG(
    dag_id="students_full_load",
    description="Full load: MSSQL → staging → ClickHouse (all 12 tables)",
    start_date=datetime(2026, 1, 1),
    schedule=None,          # Triggered manually or by a parent orchestrator
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "students-dwbi",
        "retries": 1,
        "retry_delay": timedelta(minutes=3),
    },
    tags=["students", "full-load"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Task 1: Extract all 14 tables from MSSQL to PostgreSQL staging
    extract = BashOperator(
        task_id="extract_mssql_to_staging",
        execution_timeout=timedelta(minutes=60),
        env=ENV_BASE,
        bash_command=f"""
set -Eeuo pipefail
echo "=== Extract: 14 tables from MSSQL to Staging ==="
/opt/spark/bin/spark-submit \\
  --master "local[*]" \\
  --jars "{MSSQL_JAR},{POSTGRES_JAR}" \\
  --driver-class-path "{MSSQL_JAR},{POSTGRES_JAR}" \\
  "{SPARK_JOBS}/extract_mssql_to_staging.py"
""",
    )

    # Task 2: Spark transforms small dimension tables to ClickHouse
    transform = BashOperator(
        task_id="transform_staging_to_dw",
        execution_timeout=timedelta(minutes=30),
        env=ENV_BASE,
        bash_command=f"""
set -Eeuo pipefail
echo "=== Transform: small tables → ClickHouse ==="
/opt/spark/bin/spark-submit \\
  --master "local[*]" \\
  --jars "{POSTGRES_JAR},{CH_SPARK_JAR},{CH_JDBC_JAR}" \\
  --driver-class-path "{POSTGRES_JAR},{CH_SPARK_JAR},{CH_JDBC_JAR}" \\
  --conf spark.sql.catalog.clickhouse=com.clickhouse.spark.ClickHouseCatalog \\
  --conf spark.sql.catalog.clickhouse.host=clickhouse \\
  --conf spark.sql.catalog.clickhouse.protocol=http \\
  --conf spark.sql.catalog.clickhouse.http_port=8123 \\
  --conf spark.sql.catalog.clickhouse.user=default \\
  --conf spark.sql.catalog.clickhouse.password= \\
  --conf spark.sql.catalog.clickhouse.database=StudentsDW \\
  --conf spark.clickhouse.write.format=json \\
  --conf spark.clickhouse.ignoreUnsupportedTransform=true \\
  "{SPARK_JOBS}/transform_staging_to_dw.py"
""",
    )

    # Task 3: Python baseline loader for big fact tables (Truncates and Inserts to avoid duplicates)
    baseline = BashOperator(
        task_id="load_baseline",
        execution_timeout=timedelta(minutes=30),
        env={
            "PATH": "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
            "HOME": "/home/airflow",
        },
        bash_command=f"""
set -Eeuo pipefail
echo "=== Baseline: big tables → ClickHouse ==="
python3 "{SPARK_JOBS}/baseline_all.py"
""",
    )

    end = EmptyOperator(task_id="end")

    start >> extract >> transform >> baseline >> end