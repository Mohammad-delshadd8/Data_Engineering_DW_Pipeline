"""Extract MSSQL Students -> PostgreSQL staging (full refresh)."""

import sys
from pyspark.sql import SparkSession


# --------------------------------------------------
# Tables
# --------------------------------------------------

TABLES = [
    "Applicants",
    "Students",
    "Departments",
    "Courses",
    "CourseOfferings",
    "CourseEnrollments",
    "Terms",
    "Degrees",
    "DegreeTypes",
    "DegreeLevels",
    "ClassStandings",
    "Grades",
    "Sessions",
    "DegreeRequirements",
]


# --------------------------------------------------
# MSSQL connection
# --------------------------------------------------

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


# --------------------------------------------------
# PostgreSQL staging connection
# --------------------------------------------------

PG_URL = "jdbc:postgresql://postgres-staging:5432/staging"

PG_PROPS = {
    "user": "staging",
    "password": "staging",
    "driver": "org.postgresql.Driver",
}


# --------------------------------------------------
# ETL
# --------------------------------------------------

def main():
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

            # Extract from MSSQL
            df = spark.read.jdbc(
                url=MSSQL_URL,
                table=source_table,
                properties=MSSQL_PROPS,
            )

            row_count = df.count()

            print(f"    extracted {row_count} rows")

            # Load into PostgreSQL staging
            df.write.jdbc(
                url=PG_URL,
                table=target_table,
                mode="overwrite",
                properties=PG_PROPS,
            )

            print(f"    wrote {row_count} rows -> {target_table}")

        print("\nMSSQL -> PostgreSQL staging completed successfully.")

    except Exception as exc:
        print(f"\nETL failed: {exc}", file=sys.stderr)
        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()