"""
MSSQL to PostgreSQL Full Extraction
===================================
This Spark job extracts all operational tables from MSSQL (Students DB) 
and loads them into PostgreSQL staging tables (raw_*).

Key Features:
- Uses JDBC for both source (MSSQL) and target (PostgreSQL).
- Employs `mode="overwrite"` to ensure a clean baseline before Transform steps.
- Iterates through 14 tables, logging row counts for each.

Environment: Called by Airflow `students_full_load` DAG.
"""
import sys
from pyspark.sql import SparkSession

# ============================================================
# Configuration & Tables
# ============================================================

# All source tables to be extracted for the Data Warehouse
TABLES = [
    "Applicants", "Students", "Departments", "Courses",
    "CourseOfferings", "CourseEnrollments", "Terms",
    "Degrees", "DegreeTypes", "DegreeLevels",
    "ClassStandings", "Grades", "Sessions", "DegreeRequirements",
]

# --- Source (MSSQL) Connection Details ---
MSSQL_URL = (
    "jdbc:sqlserver://mssql:1433;"
    "databaseName=Students;"
    "encrypt=true;"
    "trustServerCertificate=true"
)
MSSQL_PROPS = {
    "user": "sa",
    "password": "YourStrong!Passw0rd",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

# --- Target (PostgreSQL Staging) Connection Details ---
PG_URL = "jdbc:postgresql://postgres-staging:5432/staging"
PG_PROPS = {
    "user": "staging",
    "password": "staging",
    "driver": "org.postgresql.Driver",
}

# ============================================================
# Main Execution
# ============================================================

def main():
    # Initialize Spark Session (JARs are provided via spark-submit in Airflow)
    spark = (
        SparkSession.builder
        .appName("ExtractMSSQLtoStaging")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        for table_name in TABLES:
            source_table = f"dbo.{table_name}"
            target_table = f"staging.raw_{table_name.lower()}"

            print(f"\n>>> Extracting {source_table} ...")

            # 1. Read from MSSQL
            df = spark.read.jdbc(
                url=MSSQL_URL,
                table=source_table,
                properties=MSSQL_PROPS,
            )

            # Cache the count to avoid double execution of the query
            row_count = df.count()
            print(f"    Extracted {row_count:,} rows from {table_name}")

            # 2. Write to PostgreSQL (Full Refresh / Overwrite)
            df.write.jdbc(
                url=PG_URL,
                table=target_table,
                mode="overwrite",
                properties=PG_PROPS,
            )

            print(f"    Wrote {row_count:,} rows -> {target_table}")

        print("\n✅ MSSQL -> PostgreSQL staging completed successfully.")

    except Exception as exc:
        print(f"\n❌ ETL failed: {exc}", file=sys.stderr)
        raise

    finally:
        spark.stop()

if __name__ == "__main__":
    main()