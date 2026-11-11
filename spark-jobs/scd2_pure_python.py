"""
Pure Python SCD Type 2 (no Spark).
Uses ALTER TABLE UPDATE (mutation) to close old versions — works with MergeTree.
"""
import sys
import hashlib
import json
from datetime import date

import psycopg2
import requests

PG = dict(host="postgres-staging", dbname="staging",
          user="staging", password="staging")
CH = "http://clickhouse:8123"
DB = "StudentsDW"
TODAY = date.today().isoformat()
FUTURE = "2149-06-06"


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


def ch_mutation(sql):
    """Run ALTER TABLE UPDATE and wait for completion."""
    params = {"database": DB, "mutations_sync": "1"}
    r = requests.post(CH, data=sql, params=params)
    if not r.ok:
        raise Exception(f"CH mutation error {r.status_code}: {r.text[:300]}")
    return r.text


def read_delta(table):
    conn = psycopg2.connect(**PG)
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT * FROM staging.cdc_{table}_delta")
    except psycopg2.errors.UndefinedTable:
        conn.rollback()
        cur.close(); conn.close()
        return []
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    cur.close(); conn.close()
    return rows


def get_current(dim_table, bk_col, bk_val):
    sql = f"SELECT * FROM {dim_table} WHERE {bk_col} = {bk_val} AND IsCurrent = 1 LIMIT 1"
    txt = ch_run(sql, is_select=True).strip()
    return json.loads(txt.split("\n")[0]) if txt else None


def get_max_key(dim_table, key_col):
    txt = ch_run(f"SELECT max({key_col}) FROM {dim_table}", is_select=True).strip()
    try:
        return int(txt) if txt else 0
    except Exception:
        return 0


def sql_val(v):
    if v is None:
        return "NULL"
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (int, float)):
        return str(v)
    return "'" + str(v).replace("'", "''") + "'"


def insert_row(dim_table, row, tracked):
    cols = ", ".join(tracked)
    vals = ", ".join(sql_val(row.get(c)) for c in tracked)
    ch_run(f"INSERT INTO {dim_table} ({cols}) VALUES ({vals})")


def compute_hash(row, cols):
    s = "|".join(str(row.get(c, "") or "") for c in cols)
    return hashlib.sha256(s.encode()).hexdigest()


def enrich_student(row):
    return {
        "StudentId": row["StudentId"],
        "FirstName": row.get("FirstName"),
        "MiddleName": row.get("MiddleName"),
        "LastName": row.get("LastName"),
        "Gender": row.get("Gender"),
        "StreetAddress": row.get("StreetAddress"),
        "City": row.get("City"),
        "State": row.get("State"),
        "ZipCode": row.get("ZipCode"),
        "Telephone": row.get("Telephone"),
        "Email": row.get("email"),
        "DateOfBirth": row.get("DateOfBirth"),
        "DegreeId": row.get("DegreePursued"),
        "DegreeName": "", "DegreeTypeCode": "", "DegreeTypeName": "",
        "DegreeLevelCode": "", "DegreeLevelName": "",
        "DepartmentCode": "", "DepartmentName": "",
        "ClassStandingCode": row.get("ClassStandingCode"),
        "ClassStandingName": "",
        "StudentStatus": row.get("StudentStatus"),
        "EnrollmentTermCode": row.get("EnrollmentTerm"),
        "EnrollmentYear": None,
    }


def enrich_applicant(row):
    return {
        "ApplicantId": row["ApplicantId"],
        "FirstName": row.get("FirstName"),
        "MiddleName": row.get("MiddleName"),
        "LastName": row.get("LastName"),
        "Gender": row.get("Gender"),
        "City": row.get("City"),
        "State": row.get("State"),
        "ZipCode": row.get("ZipCode"),
        "Telephone": row.get("Telephone"),
        "Email": row.get("Email"),
        "DateOfBirth": row.get("DateOfBirth"),
        "ApplicationTypeCode": row.get("ApplicationType"),
    }


def process_dim(dim_table, delta_name, bk_col, key_col, tracked, tracked_no_meta, enrich_fn):
    print(f"\n>>> {dim_table}")
    delta = read_delta(delta_name)
    if not delta:
        print("    no delta")
        return

    latest = {}
    for row in delta:
        op = row.get("__$operation")
        if op not in (2, 4):
            continue
        bk = row[bk_col]
        lsn = row.get("__$start_lsn", "")
        if bk not in latest or lsn > latest[bk]["__$start_lsn"]:
            latest[bk] = row
    print(f"    business keys: {len(latest)}")

    max_key = get_max_key(dim_table, key_col)
    n_ins, n_upd, n_skip = 0, 0, 0

    for bk, raw in latest.items():
        enriched = enrich_fn(raw)
        new_hash = compute_hash(enriched, tracked_no_meta)

        cur = get_current(dim_table, bk_col, bk)
        if not cur:
            max_key += 1
            row = dict(enriched)
            row[key_col] = max_key
            row["EffectiveStartDate"] = TODAY
            row["EffectiveEndDate"] = FUTURE
            row["IsCurrent"] = True
            row["RowHash"] = new_hash
            insert_row(dim_table, row, tracked)
            n_ins += 1
        elif cur.get("RowHash") != new_hash:
            # Close old current row via mutation
            ch_mutation(
                f"ALTER TABLE {dim_table} "
                f"UPDATE IsCurrent = false, EffectiveEndDate = '{TODAY}' "
                f"WHERE {bk_col} = {bk} AND IsCurrent = true"
            )
            # Insert new current row
            max_key += 1
            row = dict(enriched)
            row[key_col] = max_key
            row["EffectiveStartDate"] = TODAY
            row["EffectiveEndDate"] = FUTURE
            row["IsCurrent"] = True
            row["RowHash"] = new_hash
            insert_row(dim_table, row, tracked)
            n_upd += 1
        else:
            n_skip += 1

    print(f"    INSERT={n_ins}, UPDATE={n_upd}, SKIP={n_skip}")


def main():
    tracked_student = [
        "StudentKey", "StudentId", "FirstName", "MiddleName", "LastName",
        "Gender", "StreetAddress", "City", "State", "ZipCode", "Telephone",
        "Email", "DateOfBirth", "DegreeId", "DegreeName", "DegreeTypeCode",
        "DegreeTypeName", "DegreeLevelCode", "DegreeLevelName",
        "DepartmentCode", "DepartmentName", "ClassStandingCode",
        "ClassStandingName", "StudentStatus", "EnrollmentTermCode",
        "EnrollmentYear", "EffectiveStartDate", "EffectiveEndDate",
        "IsCurrent", "RowHash",
    ]
    tracked_no_meta = [
        "StudentId", "FirstName", "MiddleName", "LastName", "Gender",
        "StreetAddress", "City", "State", "ZipCode", "Telephone", "Email",
        "DateOfBirth", "DegreeId", "DegreeName", "DegreeTypeCode",
        "DegreeTypeName", "DegreeLevelCode", "DegreeLevelName",
        "DepartmentCode", "DepartmentName", "ClassStandingCode",
        "ClassStandingName", "StudentStatus", "EnrollmentTermCode",
        "EnrollmentYear",
    ]

    tracked_applicant = [
        "ApplicantKey", "ApplicantId", "FirstName", "MiddleName", "LastName",
        "Gender", "City", "State", "ZipCode", "Telephone", "Email",
        "DateOfBirth", "ApplicationTypeCode", "EffectiveStartDate",
        "EffectiveEndDate", "IsCurrent", "RowHash",
    ]
    tracked_app_no_meta = [
        "ApplicantId", "FirstName", "MiddleName", "LastName", "Gender",
        "City", "State", "ZipCode", "Telephone", "Email", "DateOfBirth",
        "ApplicationTypeCode",
    ]

    process_dim("DimStudent", "students", "StudentId", "StudentKey",
                tracked_student, tracked_no_meta, enrich_student)
    process_dim("DimApplicant", "applicants", "ApplicantId", "ApplicantKey",
                tracked_applicant, tracked_app_no_meta, enrich_applicant)
    print("\n=== SCD2 done ===")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"FATAL: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
