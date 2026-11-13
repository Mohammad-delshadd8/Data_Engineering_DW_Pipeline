
import sys, hashlib
from datetime import date
import psycopg2, requests

CH = "http://clickhouse:8123"
DB = "StudentsDW"
PG = dict(host="postgres-staging", dbname="staging", user="staging", password="staging")
TODAY = date.today().isoformat()
FUTURE = "2149-06-06"

def ch_run(sql):
    r = requests.post(CH, params={"database": DB, "query": sql})
    if not r.ok: raise Exception(f"CH error {r.status_code}: {r.text[:300]}")
    return r.text

def ch_count(table):
    try:
        r = requests.get(CH, params={"database": DB, "query": f"SELECT count() FROM {table}"})
        return int(r.text.strip() or 0) if r.ok else 0
    except: return 0

def ch_insert(table, rows):
    if not rows: return
    body = "\n".join(rows)
    r = requests.post(CH, params={"database": DB, "query": f"INSERT INTO {table} FORMAT JSONEachRow"}, data=body, timeout=180)
    if not r.ok: raise Exception(f"INSERT {table} failed: {r.status_code} {r.text[:300]}")

def read_pg(sql):
    conn = psycopg2.connect(**PG); cur = conn.cursor(); cur.execute(sql)
    cols = [d[0] for d in cur.description]; rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    cur.close(); conn.close(); return rows

def hsh(*vals): return hashlib.sha256("|".join(str(v or "") for v in vals).encode()).hexdigest()
def es(s): return "" if s is None else str(s).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n").replace("\r", "\\r")
def to_json(r):
    parts = []
    for k, v in r.items():
        if isinstance(v, bool): val = "true" if v else "false"
        elif v is None: val = "null"
        elif isinstance(v, (int, float)): val = str(v)
        else: val = f'"{es(v)}"'
        parts.append(f'"{k}":{val}')
    return "{" + ",".join(parts) + "}"
def batch_insert(table, rows, size=10000):
    for i in range(0, len(rows), size):
        ch_insert(table, [to_json(r) for r in rows[i:i+size]])
def di(v, d=0):
    try: return int(v)
    except: return d
def df(v, d="1900-01-01"):
    try: return str(v)[:10]
    except: return d

def load_table(table_name, pg_query, transform_fn):
    print(f"\n=== {table_name} (now: {ch_count(table_name)}) ===")
    
    ch_run(f"TRUNCATE TABLE {table_name}")
    rows = read_pg(pg_query)
    out = [transform_fn(i, r) for i, r in enumerate(rows, 1)]
    batch_insert(table_name, out)
    print(f"  done: {ch_count(table_name)}")

def main():
    # Applicant
    def tr_app(i, r):
        return {"ApplicantKey": i, "ApplicantId": di(r.get("ApplicantId")), "FirstName": es(r.get("FirstName")), "MiddleName": es(r.get("MiddleName")), "LastName": es(r.get("LastName")), "Gender": es(r.get("Gender")), "City": es(r.get("City")), "State": es(r.get("State")), "ZipCode": es(r.get("ZipCode")), "Telephone": es(r.get("Telephone")), "Email": es(r.get("Email")), "DateOfBirth": df(r.get("DateOfBirth")), "ApplicationTypeCode": es(r.get("ApplicationType")), "EffectiveStartDate": TODAY, "EffectiveEndDate": FUTURE, "IsCurrent": True, "RowHash": hsh(r.get("FirstName"), r.get("LastName"), r.get("Email"), r.get("City"), r.get("State"))}
    load_table("DimApplicant", "SELECT * FROM staging.raw_applicants", tr_app)

    # Student
    degrees = {r["DegreeId"]: r for r in read_pg("SELECT * FROM staging.raw_degrees")}
    dts = {r["DegreeTypeCode"]: r for r in read_pg("SELECT * FROM staging.raw_degreetypes")}
    dls = {r["DegreeLevelCode"]: r for r in read_pg("SELECT * FROM staging.raw_degreelevels")}
    depts = {r["DepartmentCode"]: r for r in read_pg("SELECT * FROM staging.raw_departments")}
    css = {r["ClassStandingCode"]: r for r in read_pg("SELECT * FROM staging.raw_classstandings")}
    def tr_stu(i, r):
        dg = degrees.get(r.get("DegreePursued"), {}); dt = dts.get(dg.get("DegreeTypeCode"), {})
        dl = dls.get(dt.get("DegreeLevelCode"), {}); dp = depts.get(dg.get("DepartmentCode"), {})
        cs = css.get(r.get("ClassStandingCode"), {})
        return {"StudentKey": i, "StudentId": di(r.get("StudentId")), "FirstName": es(r.get("FirstName")), "MiddleName": es(r.get("MiddleName")), "LastName": es(r.get("LastName")), "Gender": es(r.get("Gender")), "StreetAddress": es(r.get("StreetAddress")), "City": es(r.get("City")), "State": es(r.get("State")), "ZipCode": es(r.get("ZipCode")), "Telephone": es(r.get("Telephone")), "Email": es(r.get("email")), "DateOfBirth": df(r.get("DateOfBirth")), "DegreeId": di(r.get("DegreePursued")), "DegreeName": es(dg.get("DegreeName")), "DegreeTypeCode": es(dg.get("DegreeTypeCode")), "DegreeTypeName": es(dt.get("DegreeTypeName")), "DegreeLevelCode": es(dt.get("DegreeLevelCode")), "DegreeLevelName": es(dl.get("DegreeLevelName")), "DepartmentCode": es(dg.get("DepartmentCode")), "DepartmentName": es(dp.get("DepartmentName")), "ClassStandingCode": es(r.get("ClassStandingCode")) or None, "ClassStandingName": es(cs.get("ClassStandingName")) or None, "StudentStatus": es(r.get("StudentStatus")), "EnrollmentTermCode": es(r.get("EnrollmentTerm")), "EnrollmentYear": None, "EffectiveStartDate": TODAY, "EffectiveEndDate": FUTURE, "IsCurrent": True, "RowHash": hsh(r.get("FirstName"), r.get("LastName"), r.get("email"), r.get("DegreePursued"), r.get("ClassStandingCode"))}
    load_table("DimStudent", "SELECT * FROM staging.raw_students", tr_stu)

    # FactApplication
    terms = sorted(read_pg("SELECT * FROM staging.raw_terms"), key=lambda x: x["TermCode"])
    term_keys = {t["TermCode"]: i+1 for i, t in enumerate(terms)}
    def tr_fapp(i, r):
        recv = df(r.get("ApplicationReceivedDate"))
        return {"ApplicationFactKey": i, "ApplicationId": di(r.get("ApplicantId")), "ApplicantKey": i, "ApplicationReceivedDateKey": di(recv.replace("-", "")), "EntryTermStartDateKey": 0, "EntryTermKey": term_keys.get(r.get("EntryTerm"), 0), "ApplicationStatusCode": es(r.get("ApplicationStatus")), "ApplicationTypeCode": es(r.get("ApplicationType")), "GPA": float(r["Gpa"]) if r.get("Gpa") is not None else None, "SATMathScore": di(r.get("SatMathScore")) or None, "SATReadingScore": di(r.get("SatReadingScore")) or None, "SATWritingScore": di(r.get("SatWritingScore")) or None, "EssayScore": di(r.get("EssayScore")) or None, "AdmissionPoints": float(r["AdmissionPoints"]) if r.get("AdmissionPoints") is not None else None, "ApplicationCount": 1}
    load_table("FactApplication", "SELECT * FROM staging.raw_applicants", tr_fapp)

    # FactEnrollment
    offerings = {r["CourseOfferingId"]: r for r in read_pg("SELECT * FROM staging.raw_courseofferings")}
    courses = {(r["DepartmentCode"], r["CourseNumber"]): r for r in read_pg("SELECT * FROM staging.raw_courses")}
    depts_l = sorted(read_pg("SELECT * FROM staging.raw_departments"), key=lambda x: x["DepartmentCode"])
    grades_l = sorted(read_pg("SELECT * FROM staging.raw_grades"), key=lambda x: x["Grade"])
    students_l = sorted(read_pg("SELECT * FROM staging.raw_students"), key=lambda x: x["StudentId"])
    css_l = sorted(read_pg("SELECT * FROM staging.raw_classstandings"), key=lambda x: x["ClassStandingCode"])
    course_keys = {k: i+1 for i, k in enumerate(sorted(courses.keys()))}
    dept_keys = {r["DepartmentCode"]: i+1 for i, r in enumerate(depts_l)}
    grade_keys = {r["Grade"]: i+1 for i, r in enumerate(grades_l)}
    stu_keys = {r["StudentId"]: i+1 for i, r in enumerate(students_l)}
    cs_keys = {r["ClassStandingCode"]: i+1 for i, r in enumerate(css_l)}
    students_map = {r["StudentId"]: r for r in students_l}

    def tr_fenr(i, r):
        off = offerings.get(r.get("CourseOfferingId"), {}); stu = students_map.get(r.get("StudentId"), {})
        return {"EnrollmentFactKey": i, "CourseEnrollmentId": di(r.get("CourseEnrollmentId")), "StudentKey": stu_keys.get(r.get("StudentId"), 0), "CourseKey": course_keys.get((off.get("DepartmentCode"), off.get("CourseNumber")), 0), "DepartmentKey": dept_keys.get(off.get("DepartmentCode"), 0), "TermKey": term_keys.get(off.get("TermCode"), 0), "GradeKey": grade_keys.get(r.get("Grade"), 0), "DegreeKey": 0, "ClassStandingKey": cs_keys.get(stu.get("ClassStandingCode"), 0), "EnrollmentDateKey": 0, "GradeAssignedDateKey": int(date.today().strftime("%Y%m%d")), "EnrollmentCount": 1, "EarnedCredits": None, "GradePointsEarned": None}
    load_table("FactEnrollment", "SELECT * FROM staging.raw_courseenrollments", tr_fenr)

if __name__ == "__main__":
    try: main()
    except Exception as e:
        print(f"FATAL: {e}", file=sys.stderr); import traceback; traceback.print_exc(); sys.exit(1)