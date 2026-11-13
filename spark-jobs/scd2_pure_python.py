"""
Universal SCD Processor (Type 1 + Type 2 + Raw Fallback + Dedup)
=================================================================
- Type 1: Uses ReplacingMergeTree -> Read with FINAL to dedupe.
- Type 2: Uses MergeTree -> Read without FINAL, filter IsCurrent=1.
- Safe Data Handling: Ensures no NULLs for non-nullable columns (like DateOfBirth).
- Robust Error Logging: Prints full traceback if a dimension fails.
"""
import sys, hashlib, json, traceback
from datetime import date, datetime
from decimal import Decimal
import psycopg2, requests

PG = dict(host="postgres-staging", dbname="staging", user="staging", password="staging")
CH = "http://clickhouse:8123"
DB = "StudentsDW"
TODAY = date.today().isoformat()
FUTURE = "2149-06-06"

# ============ Helpers ============
def get_ci(d, key, default=None):
    """Case-insensitive dictionary lookup."""
    if d is None: return default
    if key in d: return d[key]
    for k, v in d.items():
        if k.lower() == key.lower(): return v
    return default

def ch_run(sql, is_select=False):
    params = {"database": DB}
    if is_select:
        params["default_format"] = "JSONEachRow"
        r = requests.get(CH, params={**params, "query": sql}, timeout=30)
    else:
        r = requests.post(CH, data=sql, params=params, timeout=30)
    if not r.ok: raise Exception(f"CH error {r.status_code}: {r.text[:300]}")
    return r.text

def ch_mutation(sql):
    params = {"database": DB, "mutations_sync": "1"}
    r = requests.post(CH, data=sql, params=params, timeout=60)
    if not r.ok: raise Exception(f"CH mutation error {r.status_code}: {r.text[:300]}")

def read_delta(table):
    conn = psycopg2.connect(**PG); cur = conn.cursor()
    try: cur.execute(f"SELECT * FROM staging.cdc_{table.lower()}_delta")
    except psycopg2.errors.UndefinedTable:
        conn.rollback(); cur.close(); conn.close(); return []
    except Exception as e:
        print(f"    [WARN] Cannot read delta for {table}: {e}")
        conn.close(); return []
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    cur.close(); conn.close(); return rows

def read_raw(table):
    conn = psycopg2.connect(**PG); cur = conn.cursor()
    try: cur.execute(f"SELECT * FROM staging.raw_{table.lower()}")
    except psycopg2.errors.UndefinedTable:
        conn.rollback(); cur.close(); conn.close(); return []
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    cur.close(); conn.close()
    for r in rows: r["__$operation"] = 2
    return rows

def read_ch_table(table, use_final=False, where_clause=""):
    final = " FINAL" if use_final else ""
    sql = f"SELECT * FROM {table}{final}"
    if where_clause: sql += f" WHERE {where_clause}"
    txt = ch_run(sql, is_select=True)
    return [json.loads(l) for l in txt.strip().split("\n") if l]

def get_max_key(table, key_col):
    txt = ch_run(f"SELECT max({key_col}) FROM {table}", is_select=True).strip()
    try: return int(txt) if txt else 0
    except: return 0

def sql_val(v):
    if v is None: return "NULL"
    if isinstance(v, bool): return "true" if v else "false"
    if isinstance(v, (int, float, Decimal)): return str(v)
    if isinstance(v, (date, datetime)): return f"'{v.isoformat()}'"
    s = str(v)
    if s == "": return "NULL"
    return "'" + s.replace("'", "''") + "'"

def safe_date(v):
    if v is None or str(v).strip() == "": return "1900-01-01"
    return str(v)[:10]

def insert_row(table, row, cols):
    sql = f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({', '.join(sql_val(row.get(c)) for c in cols)})"
    try:
        ch_run(sql)
    except Exception as e:
        print(f"    [ERROR] INSERT failed on {table}: {e}")
        print(f"    [SQL] {sql[:500]}")
        raise

def compute_hash(row, cols):
    s = "|".join(str(row.get(c, "") or "") for c in cols)
    return hashlib.sha256(s.encode()).hexdigest()

# ============ Type 1 ============
def process_type1(dim_table, delta_name, source_key, target_key, surrogate_key, tracked_cols, extractor):
    print(f"\n>>> {dim_table} (Type 1)")
    delta = read_delta(delta_name)
    
    if not delta:
        print(f"    No CDC delta. Falling back to raw...")
        delta = read_raw(delta_name)
        if not delta:
            print(f"    No data. Skipping.")
            return

    latest = {}
    for row in delta:
        if get_ci(row, "__$operation") not in (2, 4): continue
        bk = get_ci(row, source_key)
        if bk is None: continue
        if bk not in latest or get_ci(row, "__$start_lsn", "") > latest[bk].get("__$start_lsn", ""):
            latest[bk] = row

    existing_rows = read_ch_table(dim_table, use_final=True)  # ✅ ReplacingMergeTree supports FINAL
    existing = {get_ci(r, target_key): r for r in existing_rows}
    max_key = max([r[surrogate_key] for r in existing_rows], default=0)

    compare_cols = [c for c in tracked_cols if c != surrogate_key]
    n_insert, n_skip = 0, 0

    for bk, raw in latest.items():
        row_data = extractor(raw)
        if row_data is None: continue
        
        if bk in existing:
            old_row = existing[bk]
            changed = any(str(row_data.get(c, "")) != str(old_row.get(c, "")) for c in compare_cols if c in row_data)
            if not changed:
                n_skip += 1; continue
            row_data[surrogate_key] = old_row[surrogate_key]
        else:
            max_key += 1
            row_data[surrogate_key] = max_key
        
        insert_row(dim_table, row_data, tracked_cols)
        n_insert += 1
    
    print(f"    INSERTED={n_insert}, SKIPPED={n_skip}")

# ============ Type 2 ============
def process_type2(dim_table, delta_name, bk_col, key_col, tracked, no_meta, enrich_fn):
    print(f"\n>>> {dim_table} (Type 2)")
    delta = read_delta(delta_name)
    if not delta:
        print(f"    No CDC delta. Skipping.")
        return

    latest = {}
    for row in delta:
        if get_ci(row, "__$operation") not in (2, 4): continue
        bk = get_ci(row, bk_col)
        if bk is None: continue
        if bk not in latest or get_ci(row, "__$start_lsn", "") > latest[bk].get("__$start_lsn", ""):
            latest[bk] = row
    print(f"    business keys: {len(latest)}")

    existing_rows = read_ch_table(dim_table, use_final=False, where_clause="IsCurrent=1")  # ✅ MergeTree doesn't support FINAL
    current = {get_ci(r, bk_col): r for r in existing_rows}
    max_key = get_max_key(dim_table, key_col)

    lookups = {
        "degrees": {str(get_ci(r, "DegreeId")): r for r in read_ch_table("DimDegree", use_final=True)},
        "depts":   {get_ci(r, "DepartmentCode"): r for r in read_ch_table("DimDepartment", use_final=True)},
        "css":     {get_ci(r, "ClassStandingCode"): r for r in read_ch_table("DimClassStanding", use_final=True)},
    }

    n_ins, n_upd, n_skip = 0, 0, 0
    for bk, raw in latest.items():
        try:
            enriched = enrich_fn(raw, lookups)
            new_hash = compute_hash(enriched, no_meta)
            cur = current.get(bk)

            if not cur:
                max_key += 1
                enriched.update({key_col: max_key, "EffectiveStartDate": TODAY, "EffectiveEndDate": FUTURE, "IsCurrent": True, "RowHash": new_hash})
                insert_row(dim_table, enriched, tracked)
                n_ins += 1
            elif cur.get("RowHash") != new_hash:
                ch_mutation(f"ALTER TABLE {dim_table} UPDATE IsCurrent=false, EffectiveEndDate='{TODAY}' WHERE {bk_col}={bk} AND IsCurrent=true")
                max_key += 1
                enriched.update({key_col: max_key, "EffectiveStartDate": TODAY, "EffectiveEndDate": FUTURE, "IsCurrent": True, "RowHash": new_hash})
                insert_row(dim_table, enriched, tracked)
                n_upd += 1
            else:
                n_skip += 1
        except Exception as e:
            print(f"    [ERROR] Failed processing {bk}: {e}")
            traceback.print_exc()
            raise
    print(f"    INSERT={n_ins}, UPDATE={n_upd}, SKIP={n_skip}")

# ============ Extractors ============
def ex_dept(r): return {"DepartmentCode": get_ci(r, "DepartmentCode"), "DepartmentName": get_ci(r, "DepartmentName")}
def ex_term(r): return {"TermCode": get_ci(r, "TermCode"), "SessionCode": get_ci(r, "SessionCode"), "AcademicYear": get_ci(r, "Year")}
def ex_grade(r): return {"GradeCode": get_ci(r, "Grade"), "GradePoints": get_ci(r, "Points"), "IncludeInGpa": get_ci(r, "IncludeInGpa")}
def ex_cs(r): return {"ClassStandingCode": get_ci(r, "ClassStandingCode"), "ClassStandingName": get_ci(r, "ClassStandingName"), "DegreeLevelCode": get_ci(r, "DegreeLevelCode"), "RequiredCredits": get_ci(r, "RequiredCredits")}

def ex_course(r):
    dept_code = get_ci(r, "DepartmentCode")
    depts = {get_ci(d, "DepartmentCode"): d.get("DepartmentName", "") for d in read_ch_table("DimDepartment", use_final=True)}
    return {
        "DepartmentCode": dept_code, "DepartmentName": depts.get(dept_code, ""),
        "CourseNumber": get_ci(r, "CourseNumber"), "CourseTitle": get_ci(r, "CourseTitle"),
        "CourseDescription": get_ci(r, "CourseDescription"), "Credits": get_ci(r, "Credits"),
        "MaximumSectionSize": get_ci(r, "MaximumSectionSize")
    }

def ex_degree(r):
    return {
        "DegreeId": get_ci(r, "DegreeId"), "DegreeName": get_ci(r, "DegreeName"),
        "DegreeTypeCode": get_ci(r, "DegreeTypeCode"), "DegreeTypeName": "",
        "DegreeLevelCode": get_ci(r, "DegreeLevelCode"), "DegreeLevelName": "",
        "DepartmentCode": get_ci(r, "DepartmentCode"), "DepartmentName": ""
    }

def enrich_student(raw, lookups):
    dg = lookups["degrees"].get(str(get_ci(raw, "DegreePursued")), {})
    dp = lookups["depts"].get(get_ci(dg, "DepartmentCode"), {})
    cs = lookups["css"].get(get_ci(raw, "ClassStandingCode"), {})
    return {
        "StudentId": get_ci(raw, "StudentId"), "FirstName": get_ci(raw, "FirstName"), "MiddleName": get_ci(raw, "MiddleName"),
        "LastName": get_ci(raw, "LastName"), "Gender": get_ci(raw, "Gender"), "StreetAddress": get_ci(raw, "StreetAddress"),
        "City": get_ci(raw, "City"), "State": get_ci(raw, "State"), "ZipCode": get_ci(raw, "ZipCode"),
        "Telephone": get_ci(raw, "Telephone"), "Email": get_ci(raw, "email"), 
        "DateOfBirth": safe_date(get_ci(raw, "DateOfBirth")),
        "DegreeId": get_ci(raw, "DegreePursued"), "DegreeName": dg.get("DegreeName", ""),
        "DegreeTypeCode": dg.get("DegreeTypeCode", ""), "DegreeTypeName": "", "DegreeLevelCode": dg.get("DegreeLevelCode", ""),
        "DegreeLevelName": "", "DepartmentCode": dg.get("DepartmentCode", ""), "DepartmentName": dp.get("DepartmentName", ""),
        "ClassStandingCode": get_ci(raw, "ClassStandingCode"), "ClassStandingName": cs.get("ClassStandingName", ""),
        "StudentStatus": get_ci(raw, "StudentStatus"), "EnrollmentTermCode": get_ci(raw, "EnrollmentTerm"), "EnrollmentYear": None,
    }

def enrich_applicant(raw, lookups):
    return {
        "ApplicantId": get_ci(raw, "ApplicantId"), "FirstName": get_ci(raw, "FirstName"), "MiddleName": get_ci(raw, "MiddleName"),
        "LastName": get_ci(raw, "LastName"), "Gender": get_ci(raw, "Gender"), "City": get_ci(raw, "City"),
        "State": get_ci(raw, "State"), "ZipCode": get_ci(raw, "ZipCode"), "Telephone": get_ci(raw, "Telephone"),
        "Email": get_ci(raw, "Email"), 
        "DateOfBirth": safe_date(get_ci(raw, "DateOfBirth")),
        "ApplicationTypeCode": get_ci(raw, "ApplicationType"),
    }

# ============ Main ============
def main():
    process_type1("DimDepartment", "departments", "DepartmentCode", "DepartmentCode", "DepartmentKey",
                  ["DepartmentKey", "DepartmentCode", "DepartmentName"], ex_dept)
    process_type1("DimTerm", "terms", "TermCode", "TermCode", "TermKey",
                  ["TermKey", "TermCode", "SessionCode", "AcademicYear"], ex_term)
    process_type1("DimGrade", "grades", "Grade", "GradeCode", "GradeKey",
                  ["GradeKey", "GradeCode", "GradePoints", "IncludeInGpa"], ex_grade)
    process_type1("DimClassStanding", "classstandings", "ClassStandingCode", "ClassStandingCode", "ClassStandingKey",
                  ["ClassStandingKey", "ClassStandingCode", "ClassStandingName", "DegreeLevelCode", "RequiredCredits"], ex_cs)
    process_type1("DimCourse", "courses", "CourseNumber", "CourseNumber", "CourseKey",
                  ["CourseKey", "DepartmentCode", "DepartmentName", "CourseNumber", "CourseTitle", "CourseDescription", "Credits", "MaximumSectionSize"], ex_course)
    process_type1("DimDegree", "degrees", "DegreeId", "DegreeId", "DegreeKey",
                  ["DegreeKey", "DegreeId", "DegreeName", "DegreeTypeCode", "DegreeTypeName", "DegreeLevelCode", "DegreeLevelName", "DepartmentCode", "DepartmentName"], ex_degree)

    tracked_s = ["StudentKey","StudentId","FirstName","MiddleName","LastName","Gender","StreetAddress","City","State","ZipCode","Telephone","Email","DateOfBirth","DegreeId","DegreeName","DegreeTypeCode","DegreeTypeName","DegreeLevelCode","DegreeLevelName","DepartmentCode","DepartmentName","ClassStandingCode","ClassStandingName","StudentStatus","EnrollmentTermCode","EnrollmentYear","EffectiveStartDate","EffectiveEndDate","IsCurrent","RowHash"]
    process_type2("DimStudent", "students", "StudentId", "StudentKey", tracked_s, tracked_s[1:-4], enrich_student)

    tracked_a = ["ApplicantKey","ApplicantId","FirstName","MiddleName","LastName","Gender","City","State","ZipCode","Telephone","Email","DateOfBirth","ApplicationTypeCode","EffectiveStartDate","EffectiveEndDate","IsCurrent","RowHash"]
    process_type2("DimApplicant", "applicants", "ApplicantId", "ApplicantKey", tracked_a, tracked_a[1:-4], enrich_applicant)

    print("\n=== SCD Pipeline done ===")

if __name__ == "__main__":
    try: main()
    except Exception as e:
        print(f"\n❌ FATAL: {e}")
        traceback.print_exc()
        sys.exit(1)