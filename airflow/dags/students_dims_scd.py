"""
DAG 2: Dimensions SCD Type 2
============================
Runs daily. Updates DimStudent and DimApplicant with SCD2 history.

  1. extract_deltas_for_scd : CDC delta (Students, Applicants) → staging
                              consumer='scd'
  2. apply_scd2             : Python SCD2 → DimStudent, DimApplicant
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
    dag_id="students_dims_scd",
    description="Daily SCD Type 2 for DimStudent + DimApplicant",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "students-dwbi",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
    tags=["students", "scd", "dims"],
) as dag:

    start = EmptyOperator(task_id="start")

    extract_deltas = BashOperator(
        task_id="extract_deltas_for_scd",
        execution_timeout=timedelta(minutes=10),
        env={
            "CDC_CONSUMER": "scd",
            "CDC_TABLES": "Students,Applicants",

            "JAVA_HOME": "/opt/java",
            "SPARK_HOME": "/opt/spark",
            "PATH": "/opt/java/bin:/opt/spark/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
            "HOME": "/home/airflow",
        },
        bash_command=f"""
set -Eeuo pipefail

echo "=== Extract CDC deltas (consumer=scd) ==="
echo "JAVA_HOME=$JAVA_HOME"
echo "SPARK_HOME=$SPARK_HOME"

/opt/spark/bin/spark-submit \\
  --master "local[*]" \\
  --jars "{MSSQL_JAR},{POSTGRES_JAR}" \\
  --driver-class-path "{MSSQL_JAR},{POSTGRES_JAR}" \\
  "{SPARK_JOBS}/extract_cdc_to_staging.py"
""",
    )

    apply_scd2 = BashOperator(
        task_id="apply_scd2",
        execution_timeout=timedelta(minutes=10),
        env={
            "PATH": "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
            "HOME": "/home/airflow",
        },
        bash_command=f"""
set -Eeuo pipefail
echo "=== Apply SCD2 ==="
python3 "{SPARK_JOBS}/scd2_pure_python.py"
""",
    )

    end = EmptyOperator(task_id="end")

    start >> extract_deltas >> apply_scd2 >> end
