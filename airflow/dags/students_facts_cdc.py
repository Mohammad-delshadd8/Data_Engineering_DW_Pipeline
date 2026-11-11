"""
DAG 3: Facts CDC
================
Runs every 15 min. Updates FactApplication and FactEnrollment incrementally.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

SPARK_JOBS = "/opt/airflow/spark-jobs"
JARS = f"{SPARK_JOBS}/jars"

MSSQL_JAR    = f"{JARS}/mssql-jdbc-12.8.1.jre11.jar"
POSTGRES_JAR = f"{JARS}/postgresql-42.7.4.jar"

with DAG(
    dag_id="students_facts_cdc",
    description="Incremental CDC for FactApplication + FactEnrollment",
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "students-dwbi",
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["students", "cdc", "facts"],
) as dag:

    start = EmptyOperator(task_id="start")

    extract_deltas = BashOperator(
        task_id="extract_deltas_for_facts",
        execution_timeout=timedelta(minutes=10),
        env={
            "CDC_CONSUMER": "facts",
            "CDC_TABLES": "Applicants,CourseEnrollments",
            "JAVA_HOME": "/opt/java",
            "SPARK_HOME": "/opt/spark",
            "PATH": "/opt/java/bin:/opt/spark/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
            "HOME": "/home/airflow",
        },
        bash_command=f"""
set -Eeuo pipefail
echo "=== Extract CDC deltas (consumer=facts) ==="
echo "JAVA_HOME=$JAVA_HOME"

/opt/spark/bin/spark-submit \\
  --master "local[*]" \\
  --jars "{MSSQL_JAR},{POSTGRES_JAR}" \\
  --driver-class-path "{MSSQL_JAR},{POSTGRES_JAR}" \\
  "{SPARK_JOBS}/extract_cdc_to_staging.py"
""",
    )

    upsert_facts = BashOperator(
        task_id="upsert_facts",
        execution_timeout=timedelta(minutes=15),
        env={
            "PATH": "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
            "HOME": "/home/airflow",
        },
        bash_command=f"""
set -Eeuo pipefail
echo "=== Upsert Facts ==="
python3 "{SPARK_JOBS}/facts_cdc.py"
""",
    )

    end = EmptyOperator(task_id="end")

    start >> extract_deltas >> upsert_facts >> end
