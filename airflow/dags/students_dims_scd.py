"""
DAG 2: Dimensions SCD Type 2
============================
Runs daily. Updates DimStudent and DimApplicant with SCD2 history.

Dependency Logic:
Uses a PythonSensor with Airflow REST API to check if `students_full_load` 
has at least one successful run.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.sensors.python import PythonSensor
import requests

SPARK_JOBS = "/opt/airflow/spark-jobs"
JARS = f"{SPARK_JOBS}/jars"

MSSQL_JAR    = f"{JARS}/mssql-jdbc-12.8.1.jre11.jar"
POSTGRES_JAR = f"{JARS}/postgresql-42.7.4.jar"

def check_full_load_success():
    """Check via Airflow REST API if there is at least one successful run."""
    url = "http://airflow-apiserver:8080/api/v2/dags/students_full_load/dagRuns?state=success&limit=1"
    try:
        response = requests.get(url, auth=("admin", "admin"), timeout=5)
        if response.status_code == 200:
            data = response.json()
            return len(data.get("dag_runs", [])) > 0
    except Exception as e:
        print(f"API check failed: {e}")
    return False

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

    wait_for_full_load = PythonSensor(
        task_id="wait_for_full_load",
        python_callable=check_full_load_success,
        mode="reschedule",
        poke_interval=60,
        timeout=86400,
    )

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

    start >> wait_for_full_load >> extract_deltas >> apply_scd2 >> end