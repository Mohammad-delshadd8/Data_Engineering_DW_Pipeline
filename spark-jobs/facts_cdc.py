"""
Facts CDC Processor (with Dedup Check)
=======================================
Reads CDC deltas for facts, checks existing rows with FINAL, 
only inserts new or changed rows. Prevents duplicates on re-runs.
"""
import sys, json
from datetime import date
from decimal import Decimal
import psycopg2, requests

PG = dict(host="postgres-staging", dbname="staging", user="staging", password="staging")
CH = "http://clickhouse:8123"
DB = "StudentsDW"

# ============ Helpers ============
def get_ci(d, key, default=None):
    if d is None: return default
    if key in d: return d[key]
    for k, v in d.items():
        if k.lower() == key.lower(): return v
    return default

def ch_run(sql, is_select=False):
    params = {"database": DB}
    if is_select:
        params["default_format"] = "JSONEachRow"
        r = requests.get(CH, params={**params, "query": sql})
    else:
        r = requests.post(CH, data=sql, params=params)
    if not r.ok: raise Exception(f"CH error {r.status_code}: {r.text[:300]}")
    return r.text

def read_delta(table):
    conn = psycopg2.connect(**PG); cur = conn.cursor()
    try: cur.execute(f"SELECT * FROM staging.cdc_{table.lower()}_delta")
    except psycopg2.errors.UndefinedTable:
        conn.rollback(); cur.close(); conn.close(); return []
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    cur.close(); conn.close(); return rows

def read_pg(sql):
    conn = psycopg2.connect(**PG); cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    cur.close(); conn.close(); return rows

def read_ch_final(table, where_clause=""):
    sql = f"SELECT * FROM {table} FINAL"
    if where_clause: sql += f" WHERE {where_clause}"
    txt = ch_run(sql, is_select=True)
    return [json.loads(l) for l in txt.strip().split("\n") if l]

def sql_val(v):
    if v is None: return "NULL"
    if isinstance(v, bool): return "true" if v else "false"
    if isinstance(v, (int, float, Decimal)): return str(v)
    return "'" + str(v).replace("'", "''") + "'"

def insert_row(table, row, cols):
    ch_run(f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(sql_val(row.get(c)) for c in cols)})")

def date_key(d):
    if not d: return 0
    try: return int(str(d).replace("-", "")[:8])
    except: return 0

# ============ FactCourseOffering ============
def process_fact_course_offering():
    print("\n>>> FactCourseOffering")
    delta = read_delta("courseofferings")
    if not delta:
        print("    no delta"); return

    dim_courses = read_ch_final("DimCourse")
    dim_depts   = read_ch_final("DimDepartment")
    dim_terms   = {get_ci(r, "TermCode"): r for r in read_ch_final("DimTerm")}
    
    course_by_nat = {(get_ci(r, "DepartmentCode"), int(get_ci(r, "CourseNumber", 0))): r for r in dim_courses}
    dept_by_code  = {get_ci(r, "DepartmentCode"): r for r in dim_depts}

    latest = {}
    for row in delta:
        if get_ci(row, "__$operation") not in (2, 4): continue
        bk = get_ci(row, "CourseOfferingId")
        if bk not in latest or get_ci(row, "__$start_lsn", "") > latest[bk].get("__$start_lsn", ""):
            latest[bk] = row

    # ⭐ خواندن ردیف‌های موجود با FINAL
    keys_list = ",".join(str(k) for k in latest.keys())
    existing = {get_ci(r, "CourseOfferingId"): r 
                for r in read_ch_final("FactCourseOffering", f"CourseOfferingId IN ({keys_list})")}

    cols = ["CourseOfferingFactKey", "CourseOfferingId", "CourseKey", "DepartmentKey", "TermKey", "SectionNumber", "Capacity", "OfferingCount"]
    n_ins, n_skip = 0, 0

    for bk, raw in latest.items():
        off_dept = get_ci(raw, "DepartmentCode")
        off_course_num = get_ci(raw, "CourseNumber")
        off_term = get_ci(raw, "TermCode")

        course = course_by_nat.get((off_dept, int(off_course_num) if off_course_num else 0), {})
        dept = dept_by_code.get(off_dept, {})
        term = dim_terms.get(off_term, {})

        row = {
            "CourseOfferingFactKey": int(bk), "CourseOfferingId": int(bk),
            "CourseKey": int(get_ci(course, "CourseKey", 0)),
            "DepartmentKey": int(get_ci(dept, "DepartmentKey", 0)),
            "TermKey": int(get_ci(term, "TermKey", 0)),
            "SectionNumber": int(get_ci(raw, "Section", 0)),
            "Capacity": int(get_ci(raw, "Capacity", 0)),
            "OfferingCount": 1,
        }

        # ⭐ Skip if identical
        if bk in existing:
            old = existing[bk]
            if all(str(row.get(c, "")) == str(old.get(c, "")) for c in ["CourseKey","DepartmentKey","TermKey","SectionNumber","Capacity"]):
                n_skip += 1; continue

        insert_row("FactCourseOffering", row, cols)
        n_ins += 1
    print(f"    INSERTED={n_ins}, SKIPPED={n_skip}")

# ============ FactApplication ============
def process_fact_application():
    print("\n>>> FactApplication")
    delta = read_delta("applicants")
    if not delta:
        print("    no delta"); return

    dim_terms = {get_ci(r, "TermCode"): int(get_ci(r, "TermKey", 0)) for r in read_ch_final("DimTerm")}
    latest = {}
    for row in delta:
        if get_ci(row, "__$operation") not in (2, 4): continue
        bk = get_ci(row, "ApplicantId")
        if bk not in latest or get_ci(row, "__$start_lsn", "") > latest[bk].get("__$start_lsn", ""):
            latest[bk] = row

    keys_list = ",".join(str(k) for k in latest.keys())
    existing = {get_ci(r, "ApplicationId"): r 
                for r in read_ch_final("FactApplication", f"ApplicationId IN ({keys_list})")}

    cols = ["ApplicationFactKey", "ApplicationId", "ApplicantKey", "ApplicationReceivedDateKey", "EntryTermStartDateKey", "EntryTermKey", "ApplicationStatusCode", "ApplicationTypeCode", "GPA", "SATMathScore", "SATReadingScore", "SATWritingScore", "EssayScore", "AdmissionPoints", "ApplicationCount"]
    n_ins, n_skip = 0, 0

    for bk, raw in latest.items():
        entry_term = get_ci(raw, "EntryTerm")
        entry_term_start = 0
        if entry_term and len(str(entry_term)) >= 6:
            try: entry_term_start = int(str(entry_term)[:4] + "0101")
            except: pass

        row = {
            "ApplicationFactKey": int(bk), "ApplicationId": int(bk), "ApplicantKey": int(bk),
            "ApplicationReceivedDateKey": date_key(get_ci(raw, "ApplicationReceivedDate")),
            "EntryTermStartDateKey": entry_term_start,
            "EntryTermKey": dim_terms.get(entry_term, 0),
            "ApplicationStatusCode": get_ci(raw, "ApplicationStatus"),
            "ApplicationTypeCode": get_ci(raw, "ApplicationType"),
            "GPA": get_ci(raw, "Gpa"), "SATMathScore": get_ci(raw, "SatMathScore"),
            "SATReadingScore": get_ci(raw, "SatReadingScore"), "SATWritingScore": get_ci(raw, "SatWritingScore"),
            "EssayScore": get_ci(raw, "EssayScore"), "AdmissionPoints": get_ci(raw, "AdmissionPoints"),
            "ApplicationCount": 1,
        }

        if bk in existing:
            old = existing[bk]
            if all(str(row.get(c, "")) == str(old.get(c, "")) for c in ["GPA","ApplicationStatusCode","ApplicationTypeCode","AdmissionPoints"]):
                n_skip += 1; continue

        insert_row("FactApplication", row, cols)
        n_ins += 1
    print(f"    INSERTED={n_ins}, SKIPPED={n_skip}")

# ============ FactEnrollment ============
def process_fact_enrollment():
    print("\n>>> FactEnrollment")
    delta = read_delta("courseenrollments")
    if not delta:
        print("    no delta"); return

    dim_students = {get_ci(r, "StudentId"): r for r in read_ch_final("DimStudent", "IsCurrent=1")}
    dim_courses  = read_ch_final("DimCourse")
    dim_depts    = read_ch_final("DimDepartment")
    dim_terms    = {get_ci(r, "TermCode"): r for r in read_ch_final("DimTerm")}
    dim_grades   = {get_ci(r, "GradeCode"): r for r in read_ch_final("DimGrade")}
    dim_cs       = {get_ci(r, "ClassStandingCode"): r for r in read_ch_final("DimClassStanding")}

    course_by_nat = {(get_ci(r, "DepartmentCode"), int(get_ci(r, "CourseNumber", 0))): r for r in dim_courses}
    dept_by_code  = {get_ci(r, "DepartmentCode"): r for r in dim_depts}
    offerings = {r["CourseOfferingId"]: r for r in read_pg('SELECT "CourseOfferingId", "DepartmentCode", "CourseNumber", "TermCode", "Section" FROM staging.raw_courseofferings')}

    latest = {}
    for row in delta:
        if get_ci(row, "__$operation") not in (2, 4): continue
        bk = get_ci(row, "CourseEnrollmentId")
        if bk not in latest or get_ci(row, "__$start_lsn", "") > latest[bk].get("__$start_lsn", ""):
            latest[bk] = row

    keys_list = ",".join(str(k) for k in latest.keys())
    existing = {get_ci(r, "CourseEnrollmentId"): r 
                for r in read_ch_final("FactEnrollment", f"CourseEnrollmentId IN ({keys_list})")}

    cols = ["EnrollmentFactKey", "CourseEnrollmentId", "StudentKey", "CourseKey", "DepartmentKey", "TermKey", "GradeKey", "DegreeKey", "ClassStandingKey", "EnrollmentDateKey", "GradeAssignedDateKey", "EnrollmentCount", "EarnedCredits", "GradePointsEarned"]
    n_ins, n_skip = 0, 0

    for bk, raw in latest.items():
        offering_id = get_ci(raw, "CourseOfferingId")
        offering = offerings.get(offering_id, {})
        student = dim_students.get(get_ci(raw, "StudentId"), {})

        course = course_by_nat.get((get_ci(offering, "DepartmentCode"), int(get_ci(offering, "CourseNumber", 0))), {}) if offering else {}
        dept = dept_by_code.get(get_ci(offering, "DepartmentCode") if offering else None, {})
        term = dim_terms.get(get_ci(offering, "TermCode") if offering else None, {})
        grade = dim_grades.get(get_ci(raw, "Grade"), {})
        cs = dim_cs.get(get_ci(student, "ClassStandingCode"), {})

        enr_date = 0
        if term and get_ci(term, "TermCode"):
            tc = str(get_ci(term, "TermCode"))
            if len(tc) >= 6:
                try: enr_date = int(tc[:4] + "0101")
                except: pass

        row = {
            "EnrollmentFactKey": int(bk), "CourseEnrollmentId": int(bk),
            "StudentKey": int(get_ci(student, "StudentKey", 0)),
            "CourseKey": int(get_ci(course, "CourseKey", 0)),
            "DepartmentKey": int(get_ci(dept, "DepartmentKey", 0)),
            "TermKey": int(get_ci(term, "TermKey", 0)),
            "GradeKey": int(get_ci(grade, "GradeKey", 0)), "DegreeKey": 0,
            "ClassStandingKey": int(get_ci(cs, "ClassStandingKey", 0)),
            "EnrollmentDateKey": enr_date,
            "GradeAssignedDateKey": int(date.today().strftime("%Y%m%d")),
            "EnrollmentCount": 1, "EarnedCredits": None, "GradePointsEarned": None,
        }

        if bk in existing:
            old = existing[bk]
            if all(str(row.get(c, "")) == str(old.get(c, "")) for c in ["StudentKey","CourseKey","DepartmentKey","TermKey","GradeKey"]):
                n_skip += 1; continue

        insert_row("FactEnrollment", row, cols)
        n_ins += 1
    print(f"    INSERTED={n_ins}, SKIPPED={n_skip}")

# ============ Main ============
def main():
    process_fact_course_offering()
    process_fact_application()
    process_fact_enrollment()
    print("\n=== Facts CDC Pipeline done ===")

if __name__ == "__main__":
    try: main()
    except Exception as e:
        print(f"FATAL: {e}", file=sys.stderr); import traceback; traceback.print_exc(); sys.exit(1)