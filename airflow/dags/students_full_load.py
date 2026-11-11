"""
DAG 1: Full Load
================
One-shot (or weekly) to populate all 12 ClickHouse tables from scratch.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

SPARK_JOBS = "/opt/airflow/spark-jobs"
JARS = f"{SPARK_JOBS}/jars"

MSSQL_JAR    = f"{JARS}/mssql-jdbc-12.8.1.jre11.jar"
POSTGRES_JAR = f"{JARS}/postgresql-42.7.4.jar"
CH_SPARK_JAR = f"{JARS}/clickhouse-spark-runtime-3.5_2.12-0.9.0.jar"
CH_JDBC_JAR  = f"{JARS}/clickhouse-jdbc-0.9.4-all.jar"

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
    schedule=None,
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

    extract = BashOperator(
        task_id="extract_mssql_to_staging",
        execution_timeout=timedelta(minutes=60),
        env=ENV_BASE,
        bash_command=f"""
set -Eeuo pipefail
echo "=== Extract: 14 tables ==="
/opt/spark/bin/spark-submit \\
  --master "local[*]" \\
  --jars "{MSSQL_JAR},{POSTGRES_JAR}" \\
  --driver-class-path "{MSSQL_JAR},{POSTGRES_JAR}" \\
  "{SPARK_JOBS}/extract_mssql_to_staging.py"
""",
    )

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
