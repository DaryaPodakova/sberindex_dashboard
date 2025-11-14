#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Steam L0 profiler -> L1 DDL & mapping skeleton
- Connects via db_tools.migration_utils.migrate_psycopg2_connection()
- Scans l0_steam_raw: payload (JSON) first, else content (CSV)
- Infers fields for sales / revenue_share
- Emits:
  1) CREATE TABLE DDL for l1_gaming_sales / l1_gaming_revenue_share
  2) JSON mapping skeleton with detected keys
Usage:
  L0_TABLE=l0_steam_raw L1_SALES=l1_gaming_sales L1_REVENUE=l1_gaming_revenue_share \
  python steam_l0_profile_and_ddl.py --limit 1000
"""
import os, io, re, json, argparse
from collections import Counter, defaultdict
import pandas as pd
import psycopg2.extras as pgx
from db_tools.migration_utils import migrate_psycopg2_connection

L0_TABLE   = os.getenv("L0_TABLE", "l0_steam_raw")
L1_SALES   = os.getenv("L1_SALES", "l1_gaming_sales")
L1_REVENUE = os.getenv("L1_REVENUE", "l1_gaming_revenue_share")

SALES_HINTS = {
    "country":  ["country","region","territory","market"],
    "units":    ["units","unit","qty","quantity","net_units"],
    "revenue":  ["revenue","gross","net","sales","usd","amount"]
}
REVSHARE_HINTS = {
    "country":  ["country","region","territory","market"],
    "share":    ["revenue_share","rev_share","additional","bonus","usd","amount","total"]
}

def get_conn():
    return migrate_psycopg2_connection()

def fetch_l0(conn, limit=0, date_from=None, date_to=None):
    where, params = [], []
    if date_from: where += ["loaded_at >= %s"]; params += [date_from]
    if date_to:   where += ["loaded_at < %s"];  params += [date_to]
    limit_sql = f" LIMIT {int(limit)} " if limit and int(limit) > 0 else ""
    sql = f"""SELECT id, source_name, file_name, file_path, report_type, report_period,
                     payload, content, file_metadata
              FROM {L0_TABLE}
              {"WHERE " + " AND ".join(where) if where else ""}
              ORDER BY loaded_at, id {limit_sql}"""
    with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
        cur.execute(sql, params)
        return cur.fetchall()

def try_json(x):
    if x is None: return None
    if isinstance(x, (dict, list)): return x
    if isinstance(x, (bytes, memoryview)):
        x = bytes(x).decode("utf-8", "ignore")
    if isinstance(x, str):
        x = x.strip()
        try:
            return json.loads(x)
        except Exception:
            return None
    return None

def csv_to_df(text):
    if text is None: return None
    if isinstance(text, (bytes, memoryview)):
        text = bytes(text).decode("utf-8", "ignore")
    s = text.lstrip()
    # Skip Steam preface lines like 'sep=,' and title rows
    skip_rows = 0
    pref_lines = 0
    for i, line in enumerate(s.splitlines()[:5]):
        if i < 3 and (line.lower().startswith("sep=") or "," not in line):
            pref_lines += 1
    skip_rows = min(pref_lines, 3)
    try:
        df = pd.read_csv(io.StringIO(s), sep=",", skiprows=skip_rows, dtype=str, engine="python")
        df.columns = [str(c).strip() for c in df.columns]
        return df
    except Exception:
        return None

def normalize_frame(obj):
    if obj is None: return None
    if isinstance(obj, list):
        return pd.json_normalize(obj)
    if isinstance(obj, dict):
        return pd.json_normalize([obj])
    if isinstance(obj, pd.DataFrame):
        return obj
    return None

def pick_column(cols, hints):
    lc = {c.lower(): c for c in cols}
    for h in hints:
        for k, orig in lc.items():
            if h in k:
                return orig
    return None

def infer_roles(df):
    """Return role -> column name"""
    cols = list(df.columns)
    roles = {}
    roles["country"] = pick_column(cols, SALES_HINTS["country"])
    # decide whether this looks like sales vs revenue_share by column patterns
    has_units   = pick_column(cols, SALES_HINTS["units"]) is not None
    has_rev     = pick_column(cols, SALES_HINTS["revenue"]) is not None
    has_share   = pick_column(cols, REVSHARE_HINTS["share"]) is not None and not has_units

    if has_share and not has_units:
        roles["share"] = pick_column(cols, REVSHARE_HINTS["share"])
        kind = "revenue_share"
    else:
        roles["units"]   = pick_column(cols, SALES_HINTS["units"])
        roles["revenue"] = pick_column(cols, SALES_HINTS["revenue"])
        kind = "sales"
    return kind, roles

def numify(s):
    if s is None: return None
    s = str(s).strip().replace("$","").replace(",","").replace("\u00a0","")
    s = re.sub(r"[^\d\.\-]", "", s)
    try:
        return float(s) if s else None
    except Exception:
        return None

def profile_rows(rows):
    sample_cols = {"sales": Counter(), "revenue_share": Counter()}
    examples = {"sales": defaultdict(list), "revenue_share": defaultdict(list)}
    for r in rows:
        df = None
        pj = try_json(r.get("payload"))
        if pj is not None:
            df = normalize_frame(pj)
        if df is None:
            df = csv_to_df(r.get("content"))
        if df is None or df.empty:
            continue
        kind, roles = infer_roles(df)
        for role, col in roles.items():
            if col:
                sample_cols[kind][role] += 1
                # keep up to 3 example values per role
                vals = df[col].dropna().astype(str).head(3).tolist()
                if vals:
                    examples[kind][role].extend(vals[:3-len(examples[kind][role])])
    return sample_cols, examples

def suggest_sales_ddl(table):
    return f"""CREATE TABLE IF NOT EXISTS {table} (
  l0_id BIGINT,
  source_name TEXT,
  report_period TEXT,
  country TEXT,
  units DOUBLE PRECISION,
  net_revenue_usd DOUBLE PRECISION,
  source_file TEXT,
  loaded_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(l0_id, source_file, country, report_period)
);"""

def suggest_revshare_ddl(table):
    return f"""CREATE TABLE IF NOT EXISTS {table} (
  l0_id BIGINT,
  source_name TEXT,
  report_period TEXT,
  country TEXT,
  revenue_share_usd DOUBLE PRECISION,
  source_file TEXT,
  loaded_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(l0_id, source_file, country, report_period)
);"""

def to_mapping_skeleton(kind, roles):
    if kind == "sales":
        return {
            "target_table": L1_SALES,
            "fields": {
                "country_name": roles.get("country"),
                "units_sold": roles.get("units"),
                "net_revenue_usd": roles.get("revenue")
            }
        }
    else:
        return {
            "target_table": L1_REVENUE,
            "fields": {
                "country_name": roles.get("country"),
                "revenue_share_usd": roles.get("share")
            }
        }

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=int(os.getenv("L0_LIMIT","500")))
    ap.add_argument("--date-from")
    ap.add_argument("--date-to")
    args = ap.parse_args()

    conn = get_conn()
    rows = fetch_l0(conn, limit=args.limit, date_from=args.date_from, date_to=args.date_to)
    if not rows:
        print(json.dumps({"ok": False, "msg": "No L0 rows"})); return

    # light roles voting
    all_roles_examples = {"sales":{}, "revenue_share":{}}
    sales_votes = Counter(); rev_votes = Counter()
    role_choices = {"sales": defaultdict(Counter), "revenue_share": defaultdict(Counter)}

    for r in rows:
        df = None
        pj = try_json(r.get("payload"))
        if pj is not None:
            df = normalize_frame(pj)
        if df is None:
            df = csv_to_df(r.get("content"))
        if df is None or df.empty:
            continue
        kind, roles = infer_roles(df)
        for role, col in roles.items():
            if not col: continue
            role_choices[kind][role][col] += 1
        if kind == "sales": sales_votes["rows"] += 1
        else:               rev_votes["rows"] += 1

    # pick winners per role
    sales_roles = {k: (v.most_common(1)[0][0] if v else None) for k,v in role_choices["sales"].items()}
    rev_roles   = {k: (v.most_common(1)[0][0] if v else None) for k,v in role_choices["revenue_share"].items()}

    # Output
    out = {
        "ok": True,
        "summary": {
            "rows_scanned": len(rows),
            "detected_sales_rows": sales_votes.get("rows",0),
            "detected_revshare_rows": rev_votes.get("rows",0),
        },
        "ddl": {
            "sales": suggest_sales_ddl(L1_SALES),
            "revenue_share": suggest_revshare_ddl(L1_REVENUE)
        },
        "mapping_skeleton": {
            "sales": to_mapping_skeleton("sales", sales_roles),
            "revenue_share": to_mapping_skeleton("revenue_share", rev_roles)
        },
        "roles": {
            "sales": sales_roles,
            "revenue_share": rev_roles
        }
    }
    print(json.dumps(out, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
