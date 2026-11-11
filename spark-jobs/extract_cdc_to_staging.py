"""
Native CDC extract: MSSQL change tables -> PostgreSQL staging deltas.

Environment variables (both optional):
  CDC_CONSUMER  : name for watermark isolation ('scd', 'facts', ...)
                  default: 'default'
  CDC_TABLES    : comma-separated list of tables to process
                  (Students, Applicants, CourseEnrollments)
                  default: all three

Reads watermark per consumer, extracts deltas, updates watermark.
"""
import os
import sys
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


MSSQL_URL = (
    "jdbc:sqlserver://mssql:1433;databaseName=Students;"
    "encrypt=true;trustServerCertificate=true"
)
MSSQL_PROPS = {
    "user": "sa",
    "password": "YourStrong!Passw0rd",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
}

PG_URL = "jdbc:postgresql://postgres-staging:5432/staging"
PG_PROPS = {
    "user": "staging",
    "password": "staging",
    "driver": "org.postgresql.Driver",
}

ZERO_LSN = "0x00000000000000000000"
ALL_TABLES = [
    ("Students",          "dbo_Students"),
    ("Applicants",        "dbo_Applicants"),
    ("CourseEnrollments", "dbo_CourseEnrollments"),
]


def get_tables():
    """Read CDC_TABLES env var and filter."""
    env = os.environ.get("CDC_TABLES", "").strip()
    if not env:
        return ALL_TABLES
    wanted = {t.strip().lower() for t in env.split(",") if t.strip()}
    return [(name, cap) for (name, cap) in ALL_TABLES if name.lower() in wanted]


def get_consumer():
    return os.environ.get("CDC_CONSUMER", "default").strip() or "default"


def get_watermark(consumer, table):
    conn = psycopg2.connect(host="postgres-staging", dbname="staging",
                            user="staging", password="staging")
    cur = conn.cursor()
    cur.execute(
        "SELECT last_lsn FROM etl.cdc_watermark WHERE consumer = %s AND table_name = %s",
        (consumer, table),
    )
    row = cur.fetchone()
    cur.close(); conn.close()
    return row[0] if row else ZERO_LSN


def set_watermark(consumer, table, lsn):
    conn = psycopg2.connect(host="postgres-staging", dbname="staging",
                            user="staging", password="staging")
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO etl.cdc_watermark (consumer, table_name, last_lsn, updated_at)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (consumer, table_name)
        DO UPDATE SET last_lsn = EXCLUDED.last_lsn, updated_at = NOW()
    """, (consumer, table, lsn))
    conn.commit(); cur.close(); conn.close()


def get_lsn_value(spark, sql_expr):
    df = spark.read.jdbc(
        url=MSSQL_URL,
        table=f"(SELECT {sql_expr} AS v) AS t",
        properties=MSSQL_PROPS,
    )
    row = df.first()
    v = row["v"]
    if v is None:
        return ZERO_LSN
    return "0x" + v.hex().upper()


def main():
    consumer = get_consumer()
    tables = get_tables()
    print(f"[CDC] consumer={consumer!r} tables={[t for t,_ in tables]}")

    spark = SparkSession.builder.appName(f"CDC_{consumer}").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        to_lsn = get_lsn_value(spark, "sys.fn_cdc_get_max_lsn()")
        print(f"[CDC] max LSN = {to_lsn}")

        for table, capture in tables:
            from_lsn = get_watermark(consumer, table)
            if from_lsn == ZERO_LSN:
                from_lsn = get_lsn_value(
                    spark, f"sys.fn_cdc_get_min_lsn('{capture}')"
                )
                print(f"\n[CDC] {table}: no watermark, using min_lsn={from_lsn}")
            else:
                print(f"\n[CDC] {table}: from={from_lsn} to={to_lsn}")

            if from_lsn == to_lsn:
                print(f"       no changes")
                continue

            query = (
                f"(SELECT * FROM cdc.fn_cdc_get_all_changes_{capture}"
                f"(CONVERT(binary(10), '{from_lsn}', 1),"
                f" CONVERT(binary(10), '{to_lsn}', 1),"
                f" 'all update old')) AS t"
            )
            df = spark.read.jdbc(url=MSSQL_URL, table=query, properties=MSSQL_PROPS)
            n = df.count()
            print(f"       delta rows: {n}")

            if n == 0:
                set_watermark(consumer, table, to_lsn)
                continue

            for c in ("__$start_lsn", "__$seqval", "__$update_mask"):
                if c in df.columns:
                    df = df.withColumn(c, F.hex(F.col(c)))

            target = f"staging.cdc_{table.lower()}_delta"
            df.write.jdbc(url=PG_URL, table=target, mode="overwrite",
                          properties=PG_PROPS)
            print(f"       wrote -> {target}")

            set_watermark(consumer, table, to_lsn)

        print(f"\n[CDC] consumer={consumer!r} done")
    except Exception as exc:
        print(f"[CDC] FAILED: {exc}", file=sys.stderr)
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
