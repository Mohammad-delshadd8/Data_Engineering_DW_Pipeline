"""
Pipeline B — Facts (CDC + Type1 upsert)
========================================
Reads staging.cdc_<table>_delta and upserts Fact tables.
Facts have no history — history comes from role-playing DateKeys to DimDate.

Uses:
- SCD2 dims (DimStudent, DimApplicant): read_ch_scd2 (filter IsCurrent=1)
- Type1 dims (DimCourse, DimDepartment, DimTerm, etc.): read_ch_type1 (no filter)
"""
import sys
import json
from datetime import date

import psycopg2
import requests

PG = dict(host="postgres-staging", dbname="staging",
          user="staging", password="staging")
CH = "http://clickhouse:8123"
DB = "StudentsDW"


def ch_run(sql, is_select=False):
    params = {"database": DB}
    if is_select:
        params["default_format"] = "JSONEachRow"
        r = requests.get(CH, params={**params, "query": sql})
    else:
        r = requests.post(CH, data=sql, params=params)
    if not r.ok:
        raise Exception(f"CH error {r.status_code}: {r.text[:300]}")
    return r.text


def read_delta(table):
    conn = psycopg2.connect(**PG)
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT * FROM staging.cdc_{table}_delta")
    except psycopg2.errors.UndefinedTable:
        conn.rollback(); cur.close(); conn.close()
        return []
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    cur.close(); conn.close()
    return rows


def read_pg(sql):
    conn = psycopg2.connect(**PG)
    cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    cur.close(); conn.close()
    return rows


def read_ch_scd2(dim_table):
    """Read current version only from SCD2 dimension (has IsCurrent)."""
    txt = ch_run(f"SELECT * FROM {dim_table} WHERE IsCurrent = 1", is_select=True)
    return [json.loads(l) for l in txt.strip().split("\n") if l]


def read_ch_type1(dim_table):
    """Read all rows from Type1 lookup dimension (no IsCurrent filter)."""
    txt = ch_run(f"SELECT * FROM {dim_table}", is_select=True)
    return [json.loads(l) for l in txt.strip().split("\n") if l]


def sql_val(v):
    if v is None: return "NULL"
    if isinstance(v, bool): return "true" if v else "false"
    if isinstance(v, (int, float)): return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def insert_row(fact_table, row, cols):
    ch_run(f"INSERT INTO {fact_table} ({', '.join(cols)}) VALUES ({', '.join(sql_val(row.get(c)) for c in cols)})")


def date_key(d):
    if not d: return 0
    try:
        return int(str(d).replace("-", "")[:8])
    except Exception:
        return 0


# ============================================================
# FactApplication
# ============================================================
def process_fact_application():
    print("\n>>> FactApplication")
    delta = read_delta("applicants")
    if not delta:
        print("    no delta"); return

    dim_terms = {r["TermCode"]: int(r["TermKey"]) for r in read_ch_type1("DimTerm")}

    latest = {}
    for row in delta:
        if row.get("__$operation") not in (2, 4): continue
        bk = row["ApplicantId"]
        if bk not in latest or row["__$start_lsn"] > latest[bk]["__$start_lsn"]:
            latest[bk] = row

    print(f"    business keys: {len(latest)}")

    cols = [
        "ApplicationFactKey", "ApplicationId", "ApplicantKey",
        "ApplicationReceivedDateKey", "EntryTermStartDateKey", "EntryTermKey",
        "ApplicationStatusCode", "ApplicationTypeCode",
        "GPA", "SATMathScore", "SATReadingScore", "SATWritingScore",
        "EssayScore", "AdmissionPoints", "ApplicationCount",
    ]

    inserted = 0
    for bk, raw in latest.items():
        received_date_key = date_key(raw.get("ApplicationReceivedDate"))
        entry_term_code = raw.get("EntryTerm")
        entry_term_key = dim_terms.get(entry_term_code, 0)

        entry_term_start_date_key = 0
        if entry_term_code and len(str(entry_term_code)) >= 6:
            try:
                entry_term_start_date_key = int(str(entry_term_code)[:4] + "0101")
            except Exception:
                pass

        row = {
            "ApplicationFactKey": int(bk),
            "ApplicationId": int(bk),
            "ApplicantKey": int(bk),
            "ApplicationReceivedDateKey": received_date_key,
            "EntryTermStartDateKey": entry_term_start_date_key,
            "EntryTermKey": entry_term_key,
            "ApplicationStatusCode": raw.get("ApplicationStatus"),
            "ApplicationTypeCode": raw.get("ApplicationType"),
            "GPA": raw.get("Gpa"),
            "SATMathScore": raw.get("SatMathScore"),
            "SATReadingScore": raw.get("SatReadingScore"),
            "SATWritingScore": raw.get("SatWritingScore"),
            "EssayScore": raw.get("EssayScore"),
            "AdmissionPoints": raw.get("AdmissionPoints"),
            "ApplicationCount": 1,
        }
        insert_row("FactApplication", row, cols)
        inserted += 1
    print(f"    INSERTED={inserted}")


# ============================================================
# FactEnrollment
# ============================================================
def process_fact_enrollment():
    print("\n>>> FactEnrollment")
    delta = read_delta("courseenrollments")
    if not delta:
        print("    no delta"); return

    # SCD2 dim: DimStudent (filter IsCurrent=1)
    dim_students = {r["StudentId"]: r for r in read_ch_scd2("DimStudent")}

    # Type1 lookup dims (no filter)
    dim_courses  = read_ch_type1("DimCourse")
    dim_depts    = read_ch_type1("DimDepartment")
    dim_terms    = {r["TermCode"]: r for r in read_ch_type1("DimTerm")}
    dim_grades   = {r["GradeCode"]: r for r in read_ch_type1("DimGrade")}
    dim_cs       = {r["ClassStandingCode"]: r for r in read_ch_type1("DimClassStanding")}

    course_by_nat = {(r["DepartmentCode"], int(r["CourseNumber"])): r for r in dim_courses}
    dept_by_code = {r["DepartmentCode"]: r for r in dim_depts}

    offerings = {r["CourseOfferingId"]: r for r in read_pg(
        'SELECT "CourseOfferingId", "DepartmentCode", "CourseNumber", "TermCode", "Section" FROM staging.raw_courseofferings'
    )}

    latest = {}
    for row in delta:
        if row.get("__$operation") not in (2, 4): continue
        bk = row["CourseEnrollmentId"]
        if bk not in latest or row["__$start_lsn"] > latest[bk]["__$start_lsn"]:
            latest[bk] = row

    print(f"    business keys: {len(latest)}")

    cols = [
        "EnrollmentFactKey", "CourseEnrollmentId",
        "StudentKey", "CourseKey", "DepartmentKey", "TermKey", "GradeKey",
        "DegreeKey", "ClassStandingKey",
        "EnrollmentDateKey", "GradeAssignedDateKey",
        "EnrollmentCount", "EarnedCredits", "GradePointsEarned",
    ]

    inserted = 0
    for bk, raw in latest.items():
        offering_id = raw.get("CourseOfferingId")
        offering = offerings.get(offering_id, {})
        student_id = raw.get("StudentId")
        student = dim_students.get(student_id, {})

        course = {}
        if offering:
            course = course_by_nat.get((offering["DepartmentCode"], int(offering["CourseNumber"])), {})
        dept = dept_by_code.get(offering.get("DepartmentCode") if offering else None, {})
        term = dim_terms.get(offering.get("TermCode") if offering else None, {})
        grade = dim_grades.get(raw.get("Grade"), {})
        cs = dim_cs.get(student.get("ClassStandingCode"), {})

        enrollment_date_key = 0
        if term and term.get("TermCode"):
            tc = str(term["TermCode"])
            if len(tc) >= 6:
                try:
                    enrollment_date_key = int(tc[:4] + "0101")
                except Exception:
                    pass

        grade_assigned_date_key = int(date.today().strftime("%Y%m%d"))

        row = {
            "EnrollmentFactKey": int(bk),
            "CourseEnrollmentId": int(bk),
            "StudentKey": int(student.get("StudentKey", 0)),
            "CourseKey": int(course.get("CourseKey", 0)),
            "DepartmentKey": int(dept.get("DepartmentKey", 0)),
            "TermKey": int(term.get("TermKey", 0)),
            "GradeKey": int(grade.get("GradeKey", 0)),
            "DegreeKey": 0,
            "ClassStandingKey": int(cs.get("ClassStandingKey", 0)),
            "EnrollmentDateKey": enrollment_date_key,
            "GradeAssignedDateKey": grade_assigned_date_key,
            "EnrollmentCount": 1,
            "EarnedCredits": None,
            "GradePointsEarned": None,
        }
        insert_row("FactEnrollment", row, cols)
        inserted += 1
    print(f"    INSERTED={inserted}")


def main():
    try:
        process_fact_application()
        process_fact_enrollment()
        print("\n=== Facts CDC done ===")
    except Exception as e:
        print(f"FATAL: {e}", file=sys.stderr)
        import traceback; traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
