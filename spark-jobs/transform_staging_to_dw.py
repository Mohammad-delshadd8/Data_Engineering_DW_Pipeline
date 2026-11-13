"""
PostgreSQL staging -> ClickHouse StudentsDW (Spark).
Uses ClickHouse Spark connector.
Enforces TRUNCATE via HTTP before write to prevent duplicate data.
"""
import sys
import urllib.request
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

CH_CATALOG = "clickhouse"
CH_DB      = "StudentsDW"
CH_URL     = "http://clickhouse:8123"
PG_URL     = "jdbc:postgresql://postgres-staging:5432/staging"

def spark_session():
    return SparkSession.builder.appName("StagingToDW").getOrCreate()

def truncate_ch(table):
    """Force truncate via HTTP to guarantee no duplicates."""
    url = f"{CH_URL}/?database={CH_DB}"
    data = f"TRUNCATE TABLE {table}".encode('utf-8')
    req = urllib.request.Request(url, data=data, method='POST')
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            print(f"  -> truncated {table}")
    except Exception as e:
        print(f"  -> failed to truncate {table}: {e}")

def read_pg(spark, name):
    return (spark.read.format("jdbc")
            .option("url", PG_URL)
            .option("dbtable", f"staging.raw_{name}")
            .option("user", "staging")
            .option("password", "staging")
            .option("driver", "org.postgresql.Driver")
            .load())

def write_ch(df, table):
    truncate_ch(table)
    df.writeTo(f"{CH_CATALOG}.{CH_DB}.{table}").append()
    print(f"  -> wrote {table}")

def sk(order, alias):
    return F.row_number().over(Window.orderBy(*order)).cast("long").alias(alias)

def main():
    spark = spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        # ---------- DimDepartment ----------
        print("\n>>> DimDepartment")
        deps = read_pg(spark, "departments")
        dim_dept = deps.select(
            sk(["DepartmentCode"], "DepartmentKey"),
            F.col("DepartmentCode"),
            F.col("DepartmentName"),
        )
        write_ch(dim_dept, "DimDepartment")

        # ---------- DimCourse ----------
        print("\n>>> DimCourse")
        courses = read_pg(spark, "courses").alias("c")
        dim_course = (courses
            .join(dim_dept.alias("d"),
                  F.col("c.DepartmentCode") == F.col("d.DepartmentCode"), "left")
            .select(
                sk(["c.DepartmentCode", "c.CourseNumber"], "CourseKey"),
                F.col("c.DepartmentCode"),
                F.col("d.DepartmentName"),
                F.col("c.CourseNumber").cast("int"),
                F.col("c.CourseTitle"),
                F.col("c.CourseDescription"),
                F.col("c.Credits").cast("decimal(3,1)"),
                F.col("c.MaximumSectionSize").cast("int"),
            ))
        write_ch(dim_course, "DimCourse")

        # ---------- DimTerm ----------
        print("\n>>> DimTerm")
        terms    = read_pg(spark, "terms").alias("t")
        sessions = read_pg(spark, "sessions").alias("s")
        dim_term = (terms
            .join(sessions, F.col("t.SessionCode") == F.col("s.SessionCode"), "left")
            .select(
                sk(["t.TermCode"], "TermKey"),
                F.col("t.TermCode"),
                F.col("t.SessionCode"),
                F.col("s.SessionName"),
                F.col("t.Year").cast("int").alias("AcademicYear"),
            ))
        write_ch(dim_term, "DimTerm")

        # ---------- DimGrade ----------
        print("\n>>> DimGrade")
        grades = read_pg(spark, "grades")
        dim_grade = grades.select(
            sk(["Grade"], "GradeKey"),
            F.col("Grade").alias("GradeCode"),
            F.col("Points").cast("decimal(2,1)").alias("GradePoints"),
            F.col("IncludeInGpa").cast("boolean").alias("IncludeInGpa"),
        )
        write_ch(dim_grade, "DimGrade")

        # ---------- DimClassStanding ----------
        print("\n>>> DimClassStanding")
        cs = read_pg(spark, "classstandings")
        dim_cs = cs.select(
            sk(["ClassStandingCode"], "ClassStandingKey"),
            F.col("ClassStandingCode"),
            F.col("ClassStandingName"),
            F.col("DegreeLevelCode"),
            F.col("RequiredCredits").cast("int"),
        )
        write_ch(dim_cs, "DimClassStanding")

        # ---------- DimDegree ----------
        print("\n>>> DimDegree")
        dg = read_pg(spark, "degrees").alias("dg")
        dt = read_pg(spark, "degreetypes").alias("dt")
        dl = read_pg(spark, "degreelevels").alias("dl")
        dim_degree = (dg
            .join(dt, F.col("dg.DegreeTypeCode") == F.col("dt.DegreeTypeCode"), "left")
            .join(dl, F.col("dt.DegreeLevelCode") == F.col("dl.DegreeLevelCode"), "left")
            .join(dim_dept.alias("d"),
                  F.col("dg.DepartmentCode") == F.col("d.DepartmentCode"), "left")
            .select(
                sk(["dg.DegreeId"], "DegreeKey"),
                F.col("dg.DegreeId").cast("int"),
                F.col("dg.DegreeName"),
                F.col("dg.DegreeTypeCode"),
                F.col("dt.DegreeTypeName"),
                F.col("dl.DegreeLevelCode"),
                F.col("dl.DegreeLevelName"),
                F.col("dg.DepartmentCode"),
                F.col("d.DepartmentName"),
            ))
        write_ch(dim_degree, "DimDegree")

        # ---------- DimDate ----------
        print("\n>>> DimDate")
        dates = spark.sql("""
            SELECT CAST(d AS DATE) AS FullDate FROM (
                SELECT sequence(to_date('2020-01-01'),
                                to_date('2030-12-31'),
                                interval 1 day) AS arr
            ) LATERAL VIEW explode(arr) t AS d
        """)
        dim_date = dates.select(
            F.date_format("FullDate", "yyyyMMdd").cast("int").alias("DateKey"),
            F.col("FullDate"),
            F.dayofmonth("FullDate").cast("int").alias("DayNumber"),
            F.date_format("FullDate", "EEEE").alias("DayName"),
            F.dayofweek("FullDate").cast("int").alias("DayOfWeek"),
            F.weekofyear("FullDate").cast("int").alias("WeekOfYear"),
            F.month("FullDate").cast("int").alias("MonthNumber"),
            F.date_format("FullDate", "MMMM").alias("MonthName"),
            F.quarter("FullDate").cast("int").alias("QuarterNumber"),
            F.concat(F.lit("Q"), F.quarter("FullDate").cast("string")).alias("QuarterName"),
            F.year("FullDate").cast("int").alias("YearNumber"),
            F.dayofweek("FullDate").isin([1, 7]).alias("IsWeekend"),
        )
        write_ch(dim_date, "DimDate")

        # ---------- FactCourseOffering ----------
        print("\n>>> FactCourseOffering")
        o = read_pg(spark, "courseofferings").alias("o")
        fact_offering = (o
            .join(dim_course.alias("c"),
                  (F.col("o.DepartmentCode") == F.col("c.DepartmentCode")) &
                  (F.col("o.CourseNumber") == F.col("c.CourseNumber")), "left")
            .join(dim_dept.alias("d"),
                  F.col("o.DepartmentCode") == F.col("d.DepartmentCode"), "left")
            .join(dim_term.alias("t"),
                  F.col("o.TermCode") == F.col("t.TermCode"), "left")
            .select(
                sk(["o.CourseOfferingId"], "CourseOfferingFactKey"),
                F.col("o.CourseOfferingId").cast("int"),
                F.coalesce(F.col("c.CourseKey"), F.lit(0)).cast("long").alias("CourseKey"),
                F.coalesce(F.col("d.DepartmentKey"), F.lit(0)).cast("long").alias("DepartmentKey"),
                F.coalesce(F.col("t.TermKey"), F.lit(0)).cast("long").alias("TermKey"),
                F.col("o.Section").cast("int").alias("SectionNumber"),
                F.col("o.Capacity").cast("int"),
                F.lit(1).alias("OfferingCount"),
            ))
        write_ch(fact_offering, "FactCourseOffering")

        print("\n=== Transform done (dims + small facts) ===")

    except Exception as exc:
        print(f"Transform failed: {exc}", file=sys.stderr)
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()