#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
One-shot: l0_steam_raw -> (l1_gaming_sales, l1_gaming_revenue_share)
- Подключение к БД: через db_tools.migration_utils.migrate_psycopg2_connection()
- Правила: steam.json (file_pattern + csv_rules)
- Парсер: pandas
- Диагностика: counters (no_rule/empty_df/sales_rows/rev_rows)
"""

import os, re, io, json, sys, warnings
from datetime import datetime
import pandas as pd
import psycopg2.extras as pgx

# --- CONFIG (env) -------------------------------------------------------------
L0_TABLE   = os.getenv("L0_TABLE", "l0_steam_raw")
L1_SALES   = os.getenv("L1_SALES", "l1_gaming_sales")
L1_REVENUE = os.getenv("L1_REVENUE", "l1_gaming_revenue_share")
RULES_PATH = os.getenv("STEAM_RULES", "etl/config/rules/steam.json")
LIMIT      = int(os.getenv("L0_LIMIT", "0"))        # 0 = все
DATE_FROM  = os.getenv("DATE_FROM")                 # YYYY-MM-DD
DATE_TO    = os.getenv("DATE_TO")                   # YYYY-MM-DD

# --- DB CONNECT ---------------------------------------------------------------
from db_tools.migration_utils import migrate_psycopg2_connection
def get_conn():
    return migrate_psycopg2_connection()

# --- RULES -------------------------------------------------------------------
def fnmatch_to_rx(pat: str) -> str:
    rx = re.escape(pat).replace(r"\*", ".*").replace(r"\?", ".")
    return f"^{rx}$"

def infer_target(rule_id: str) -> str:
    s = (rule_id or "").lower()
    if "revenue_share" in s or "revshare" in s: return "revenue"
    if "sales" in s: return "sales"
    return "sales"

def load_rules(path: str):
    data = json.load(open(path, encoding="utf-8"))
    rules = {}
    for r in data.get("rules", []):
        rid = r.get("rule_id") or r.get("id") or f"rule_{len(rules)+1}"
        csvr = (r.get("processing", {}) or {}).get("csv_rules", {})
        rules[rid] = {
            "pattern": re.compile(fnmatch_to_rx(r.get("file_pattern") or "*.csv"), re.I),
            "sep": csvr.get("sep") or csvr.get("delimiter") or ",",
            "skip_rows": int(csvr.get("skip_rows", 3)),
            "encoding": csvr.get("encoding", "utf-8"),
            "has_header": bool(csvr.get("has_header", True)),
            "quotechar": (csvr.get("quotechar") or '"'),
            "target": infer_target(r.get("rule_id")),
        }
    return rules

def match_rule(rules: dict, file_name: str):
    for rid, r in rules.items():
        if r["pattern"].search(file_name or ""):
            return rid, r
    return None, None

# --- L0 FETCH ----------------------------------------------------------------
def build_where():
    where, params = [], []
    if DATE_FROM: where += ["loaded_at >= %s"]; params += [DATE_FROM]
    if DATE_TO:   where += ["loaded_at < %s"];  params += [DATE_TO]
    clause = ("WHERE " + " AND ".join(where)) if where else ""
    limit  = f" LIMIT {LIMIT} " if LIMIT > 0 else ""
    return clause, params, limit

def fetch_l0(conn):
    where, params, limit = build_where()
    sql = f"""
      SELECT id, source_name, file_name, file_path, report_type, report_period,
             payload, file_metadata, content, loaded_at
      FROM {L0_TABLE}
      {where}
      ORDER BY loaded_at, id
      {limit}
    """
    with conn.cursor(cursor_factory=pgx.RealDictCursor) as cur:
        cur.execute(sql, params); return cur.fetchall()

# --- CSV/JSON PARSE ----------------------------------------------------------
def read_csv_text(text: str, sep: str, skip_rows: int, has_header: bool, quotechar: str):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = pd.read_csv(
            io.StringIO(text),
            sep=sep,
            header=(0 if has_header else None),
            skiprows=skip_rows,
            dtype=str,
            engine="python",
            quotechar=quotechar
        )
    df.columns = [str(c).strip() for c in df.columns]
    return df

def payload_to_df(payload):
    if payload is None: return None
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            return None
    if isinstance(payload, list):   return pd.json_normalize(payload)
    if isinstance(payload, dict):   return pd.json_normalize([payload])
    return None

def norm_period_from_name(name: str, fallback=None) -> str:
    m = re.search(r"(January|February|March|April|May|June|July|August|September|October|November|December)[ _-]*(\d{4})", name or "", re.I)
    if m:
        dt = datetime.strptime(f"{m.group(1)} {m.group(2)}", "%B %Y")
        return dt.strftime("%Y-%m")
    return (fallback or "unknown").strip()

def to_num(x):
    if x is None or (isinstance(x, float) and pd.isna(x)): return None
    s = str(x).strip().replace("$", "").replace(",", "")
    try: return float(s) if s else None
    except: return None

# --- FLEX PICKERS ------------------------------------------------------------
def _pick(df: pd.DataFrame, *cands):
    for c in cands:
        if c in df.columns: 
            return c
    lc = {c.lower(): c for c in df.columns}
    for c in lc:
        if any(tok in c for tok in ("country","region","territory")):
            return lc[c]
    return None

def _pick_num(df: pd.DataFrame, prefer: list[str], must_include_any: list[str] = None):
    c = _pick(df, *prefer)
    if c: return c
    lc = {c.lower(): c for c in df.columns}
    for name, orig in lc.items():
        if must_include_any and not any(tok in name for tok in must_include_any):
            continue
        if any(tok in name for tok in ("gross","revenue","amount","usd","net","sales","units","qty","quantity","share")):
            return orig
    return None

# --- MAPPINGS ----------------------------------------------------------------
def map_sales(df: pd.DataFrame, period: str, src_file: str, source_name: str, l0_id: int):
    c_country = _pick(df, "Country","country")
    c_units   = _pick_num(df, ["Units","units","Quantity","units_sold","qty","quantity"])
    c_gross   = _pick_num(df, ["Gross Revenue (USD)","Gross_Revenue_USD","GrossRevenueUSD","Gross","gross_usd","revenue_usd"],
                          must_include_any=["gross","revenue","usd"])
    out = []
    for _, r in df.iterrows():
        country = (str(r.get(c_country)) if c_country else "").strip()
        units   = to_num(r.get(c_units)) if c_units else None
        gross   = to_num(r.get(c_gross)) if c_gross else None
        if not country and units is None and gross is None: 
            continue
        out.append(dict(
            l0_id=l0_id,
            source_name=source_name,
            report_period=period,
            country=(country or None),
            units=units,
            gross_revenue_usd=gross,
            source_file=src_file,
        ))
    return out

def map_revenue_share(df: pd.DataFrame, period: str, src_file: str, source_name: str, l0_id: int):
    c_country = _pick(df, "Country","country")
    c_share   = _pick_num(df, ["Additional Revenue Share (USD)","Additional_Revenue_Share_USD","Revenue Share (USD)","rev_share_usd","revenue_share_usd"],
                          must_include_any=["share","usd","revenue"])
    out = []
    for _, r in df.iterrows():
        country = (str(r.get(c_country)) if c_country else "").strip()
        share   = to_num(r.get(c_share)) if c_share else None
        if not country and share is None: 
            continue
        out.append(dict(
            l0_id=l0_id,
            source_name=source_name,
            report_period=period,
            country=(country or None),
            additional_revenue_share_usd=share,
            source_file=src_file,
        ))
    return out

# --- WRITE -------------------------------------------------------------------
def ensure_tables(conn):
    with conn.cursor() as cur:
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {L1_SALES} (
          l0_id BIGINT,
          source_name TEXT,
          report_period TEXT,
          country TEXT,
          units DOUBLE PRECISION,
          gross_revenue_usd DOUBLE PRECISION,
          source_file TEXT,
          loaded_at TIMESTAMPTZ DEFAULT NOW(),
          UNIQUE(l0_id, source_file, country, report_period)
        );""")
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {L1_REVENUE} (
          l0_id BIGINT,
          source_name TEXT,
          report_period TEXT,
          country TEXT,
          additional_revenue_share_usd DOUBLE PRECISION,
          source_file TEXT,
          loaded_at TIMESTAMPTZ DEFAULT NOW(),
          UNIQUE(l0_id, source_file, country, report_period)
        );""")
    conn.commit()

def upsert_rows(conn, table: str, rows: list[dict]) -> int:
    if not rows: return 0
    cols = list(rows[0].keys())
    on_conflict_cols = ["l0_id","source_file","country","report_period"]
    update_cols = [c for c in cols if c not in on_conflict_cols]
    set_clause = ", ".join(f"{c}=EXCLUDED.{c}" for c in update_cols) or ""
    conflict = f" ON CONFLICT ({','.join(on_conflict_cols)}) DO " + ("UPDATE SET " + set_clause if set_clause else "NOTHING")
    sql = f"INSERT INTO {table} ({','.join(cols)}) VALUES %s {conflict}"
    values = [tuple(r.get(c) for c in cols) for r in rows]
    with conn.cursor() as cur:
        pgx.execute_values(cur, sql, values, page_size=10000)
    conn.commit()
    return len(rows)

# --- MAIN --------------------------------------------------------------------
def main():
    rules = load_rules(RULES_PATH)
    conn = get_conn()
    ensure_tables(conn)

    l0_rows = fetch_l0(conn)
    stats = {"no_rule":0, "empty_df":0, "sales_rows":0, "rev_rows":0}
    sales_batch, rev_batch = [], []

    for r in l0_rows:
        name = r.get("file_name") or r.get("file_path") or ""
        _, rule = match_rule(rules, name)
        if not rule: 
            stats["no_rule"] += 1
            continue
        period = norm_period_from_name(name, r.get("report_period"))

        # payload (JSON/строка JSON)
        df = payload_to_df(r.get("payload"))

        # content (CSV) — если payload пуст
        if df is None and r.get("content"):
            content = r.get("content")
            if isinstance(content, (bytes, memoryview)):
                try:
                    content = bytes(content).decode("utf-8", "ignore")
                except Exception:
                    content = str(bytes(content))
            df = read_csv_text(
                content,
                sep=rule["sep"],
                skip_rows=rule["skip_rows"],
                has_header=rule["has_header"],
                quotechar=rule["quotechar"],
            )

        if df is None or df.empty:
            stats["empty_df"] += 1
            continue

        if rule["target"] == "revenue":
            added = map_revenue_share(df, period, name, r.get("source_name"), r["id"])
            rev_batch += added
            stats["rev_rows"] += len(added)
        else:
            added = map_sales(df, period, name, r.get("source_name"), r["id"])
            sales_batch += added
            stats["sales_rows"] += len(added)

    ins1 = upsert_rows(conn, L1_SALES, sales_batch)
    ins2 = upsert_rows(conn, L1_REVENUE, rev_batch)
    print(json.dumps({
        "ok": True,
        "l0_read": len(l0_rows),
        **stats,
        "l1_sales_upserted": ins1,
        "l1_revenue_upserted": ins2
    }, ensure_ascii=False))
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) == 2:
        RULES_PATH = sys.argv[1]
    main()
