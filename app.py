# -*- coding: utf-8 -*-
import io, os, json, sqlite3, logging, threading, asyncio, html as ihtml
from datetime import datetime, timedelta
from dataclasses import dataclass
from collections import deque
from typing import Deque, List, Optional, Tuple, Dict

import pandas as pd
from zoneinfo import ZoneInfo

def _maybe_trustee_plus(df: pd.DataFrame) -> pd.DataFrame:
    try:
        cols = set(map(str, df.columns))
    except Exception:
        return df
    needed = {"Date","Type","Status","Amount","Currency code","Currency code after swap","Rate"}
    if not needed.issubset(cols):
        return df
    out = df.copy()
    if "Created Time" not in out.columns and "Date" in out.columns:
        out["Created Time"] = out["Date"]
    if "Order Type" not in out.columns and "Type" in out.columns:
        s = out["Type"].astype(str).str.upper().str.strip()
        out["Order Type"] = s.replace({"DEPOSIT":"Buy","WITHDRAW":"Sell"})
    if "Asset Type" not in out.columns and "Currency code" in out.columns:
        out["Asset Type"] = out["Currency code"]
    if "Fiat Type" not in out.columns and "Currency code after swap" in out.columns:
        out["Fiat Type"] = out["Currency code after swap"]
    if "Quantity" not in out.columns and "Amount" in out.columns:
        out["Quantity"] = pd.to_numeric(out["Amount"], errors="coerce").abs()
    if "Price" not in out.columns and "Rate" in out.columns:
        out["Price"] = pd.to_numeric(out["Rate"], errors="coerce")
    if "Total Price" not in out.columns and {"Quantity","Price"}.issubset(out.columns):
        out["Total Price"] = pd.to_numeric(out["Quantity"], errors="coerce").abs() * pd.to_numeric(out["Price"], errors="coerce")
    if "Status" in out.columns:
        st = out["Status"].astype(str).str.upper().str.strip()
        out["Status"] = st.replace({"DONE":"completed"}).str.lower()
    return out

def _maybe_p2p_ru_csv(df: pd.DataFrame) -> pd.DataFrame:
    try:
        cols = set(map(str, df.columns))
    except Exception:
        return df
    needed = {"Тип ордера","Крипта","Валюта","Цена","Объем","Сумма","Статус","Дата создания"}
    if not needed.issubset(cols):
        return df
    out = df.copy()
    if "Created Time" not in out.columns and "Дата создания" in out.columns:
        out["Created Time"] = out["Дата создания"]
    if "Order Type" not in out.columns and "Тип ордера" in out.columns:
        s = out["Тип ордера"].astype(str).str.strip().str.lower()
        out["Order Type"] = s.replace({"купить":"Buy","продать":"Sell"})
    if "Asset Type" not in out.columns and "Крипта" in out.columns:
        out["Asset Type"] = out["Крипта"].astype(str).str.upper()
    if "Fiat Type" not in out.columns and "Валюта" in out.columns:
        out["Fiat Type"] = out["Валюта"].astype(str).str.upper()
    if "Quantity" not in out.columns and "Объем" in out.columns:
        out["Quantity"] = pd.to_numeric(out["Объем"], errors="coerce")
    if "Price" not in out.columns and "Цена" in out.columns:
        out["Price"] = pd.to_numeric(out["Цена"], errors="coerce")
    if "Total Price" not in out.columns and "Сумма" in out.columns:
        out["Total Price"] = pd.to_numeric(out["Сумма"], errors="coerce")
    if "Status" not in out.columns and "Статус" in out.columns:
        out["Status"] = out["Статус"]
    if "Status" in out.columns:
        st = out["Status"].astype(str).str.strip().str.lower()
        out["Status"] = st.replace({
            "выполнено":"completed","готово":"completed","успешно":"completed",
            "отменено":"canceled","отмена":"canceled"
        })
    return out


from flask import Flask, request, redirect, url_for, session, render_template_string, abort

from telegram import Update, constants, ForceReply, ReplyKeyboardMarkup, KeyboardButton
from telegram.ext import Application, CommandHandler, MessageHandler, ContextTypes, filters, Defaults

try:
    from dotenv import load_dotenv, find_dotenv
    load_dotenv(find_dotenv(usecwd=True))
except Exception:
    pass

DB_PATH        = os.environ.get("PNL_DB_PATH", "state.sqlite")
FIAT           = "UAH"
ASSETS         = ("USDT", "USDC")
ADMIN_PASSWORD = os.environ.get("ADMIN_PASSWORD")
ADMIN_SECRET   = os.environ.get("ADMIN_SECRET", "change-me")
ADMIN_TZ       = os.environ.get("ADMIN_TZ", "Europe/Moscow")

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("pnl-bot")

def db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def db_init():
    con = db(); cur = con.cursor()
    cur.execute("""CREATE TABLE IF NOT EXISTS inventory(
        user_id TEXT NOT NULL,
        asset TEXT NOT NULL,
        layer_index INTEGER NOT NULL,
        qty REAL NOT NULL,
        cost REAL NOT NULL,
        PRIMARY KEY(user_id, asset, layer_index)
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS last_trades_by_asset(
        user_id TEXT NOT NULL,
        asset TEXT NOT NULL,
        trades_json TEXT NOT NULL,
        PRIMARY KEY(user_id, asset)
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS fiat_balance(
        user_id TEXT NOT NULL,
        currency TEXT NOT NULL,
        balance REAL NOT NULL,
        PRIMARY KEY(user_id, currency)
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS manual_asset_ops(
        user_id TEXT NOT NULL,
        asset TEXT NOT NULL,
        created_time TEXT NOT NULL,
        side TEXT NOT NULL,
        qty REAL NOT NULL,
        price REAL NOT NULL
    )""")
    cur.execute("""CREATE TABLE IF NOT EXISTS users(
        user_id TEXT PRIMARY KEY,
        first_name TEXT, last_name TEXT, username TEXT,
        is_blocked INTEGER DEFAULT 0
    )""")
    cur.execute("PRAGMA table_info(users)")
    cols = {r[1] for r in cur.fetchall()}
    if "created_at" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN created_at TEXT")
    if "last_seen" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN last_seen TEXT")
    if "is_blocked" not in cols:
        cur.execute("ALTER TABLE users ADD COLUMN is_blocked INTEGER DEFAULT 0")

    cur.execute("""CREATE TABLE IF NOT EXISTS chat_log(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id TEXT NOT NULL,
        ts TEXT NOT NULL,
        direction TEXT NOT NULL, -- 'in' | 'out'
        text TEXT
    )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS broadcast_queue(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        created_at   TEXT NOT NULL,
        scheduled_at TEXT,            -- NULL => сразу
        text         TEXT NOT NULL,
        status       TEXT NOT NULL DEFAULT 'ожидание', -- ожидание|sending|готово
        author       TEXT,
        target_user_id TEXT           -- NULL => всем
    )""")
    cur.execute("PRAGMA table_info(broadcast_queue)")
    bcols = {r[1] for r in cur.fetchall()}
    if "scheduled_at" not in bcols:
        cur.execute("ALTER TABLE broadcast_queue ADD COLUMN scheduled_at TEXT")

    con.commit(); con.close()

def upsert_user_from_telegram(u) -> None:
    if not u: return
    user_id = str(u.id)
    con = db(); cur = con.cursor()
    now = datetime.utcnow().isoformat()
    cur.execute("""
        INSERT INTO users (user_id, first_name, last_name, username, created_at, last_seen, is_blocked)
        VALUES (?, ?, ?, ?, ?, ?, 1)
        ON CONFLICT(user_id) DO UPDATE SET
            first_name=excluded.first_name,
            last_name =excluded.last_name,
            username  =excluded.username,
            last_seen =excluded.last_seen
    """, (user_id, u.first_name or "", u.last_name or "", u.username or "", now, now))
    con.commit(); con.close()

def user_is_blocked(user_id: str) -> bool:
    con = db(); row = con.execute("SELECT is_blocked FROM users WHERE user_id=?", (user_id,)).fetchone(); con.close()
    return bool(row and row[0])

def set_block(user_id: str, flag: bool):
    con = db(); con.execute("UPDATE users SET is_blocked=? WHERE user_id=?", (1 if flag else 0, user_id)); con.commit(); con.close()

def log_chat(user_id: str, direction: str, text: str):
    con = db(); con.execute("INSERT INTO chat_log (user_id, ts, direction, text) VALUES (?,?,?,?)",
                            (user_id, datetime.utcnow().isoformat(), direction, text)); con.commit(); con.close()

def load_layers(user_id: str, asset: str) -> List[Tuple[float, float]]:
    con = db(); rows = con.execute(
        "SELECT qty, cost FROM inventory WHERE user_id=? AND asset=? ORDER BY layer_index",
        (user_id, asset)
    ).fetchall(); con.close()
    return [(float(r[0]), float(r[1])) for r in rows]

def save_layers(user_id: str, layers: List[Tuple[float, float]], asset: str):
    con = db(); cur = con.cursor()
    cur.execute("DELETE FROM inventory WHERE user_id=? AND asset=?", (user_id, asset))
    for i, (q, c) in enumerate(layers):
        cur.execute("INSERT INTO inventory (user_id, asset, layer_index, qty, cost) VALUES (?,?,?,?,?)",
                    (user_id, asset, i, float(q), float(c)))
    con.commit(); con.close()

def reset_layers(user_id: str, asset: str):
    con = db(); con.execute("DELETE FROM inventory WHERE user_id=? AND asset=?", (user_id, asset)); con.commit(); con.close()

def set_last_trades(user_id: str, asset: str, trades: pd.DataFrame):
    t = trades.copy()
    if not t.empty and "Created Time" in t.columns:
        t["Created Time"] = t["Created Time"].astype(str)
    payload = json.dumps(t.to_dict(orient="records"), ensure_ascii=False)
    con = db(); con.execute("REPLACE INTO last_trades_by_asset (user_id, asset, trades_json) VALUES (?,?,?)",
                            (user_id, asset, payload)); con.commit(); con.close()

def get_last_trades(user_id: str, asset: str) -> Optional[pd.DataFrame]:
    con = db(); row = con.execute(
        "SELECT trades_json FROM last_trades_by_asset WHERE user_id=? AND asset=?",
        (user_id, asset)
    ).fetchone(); con.close()
    if not row: return None
    df = pd.DataFrame.from_records(json.loads(row[0]))
    if not df.empty and "Created Time" in df.columns:
        df["Created Time"] = pd.to_datetime(df["Created Time"], errors="coerce")
    return df

def add_manual_op(user_id: str, asset: str, side: str, qty: float, price: float):
    con = db(); con.execute(
        "INSERT INTO manual_asset_ops (user_id, asset, created_time, side, qty, price) VALUES (?,?,?,?,?,?)",
        (user_id, asset, datetime.utcnow().isoformat(), side, float(qty), float(price))
    ); con.commit(); con.close()

def get_manual_ops(user_id: str, asset: str) -> pd.DataFrame:
    con = db(); rows = con.execute(
        "SELECT created_time, side, qty, price FROM manual_asset_ops WHERE user_id=? AND asset=?",
        (user_id, asset)
    ).fetchall(); con.close()
    if not rows:
        return pd.DataFrame(columns=["Created Time","Order Type","Asset Type","Fiat Type","Quantity","Price","Total Price"])
    df = pd.DataFrame(rows, columns=["Created Time","Order Type","Quantity","Price"])
    df["Created Time"] = pd.to_datetime(df["Created Time"], errors="coerce")
    df["Order Type"]   = df["Order Type"].astype(str).str.title()
    df["Asset Type"]   = asset
    df["Fiat Type"]    = FIAT
    df["Quantity"]     = df["Quantity"].astype(float)
    df["Price"]        = df["Price"].astype(float)
    df["Total Price"]  = df["Quantity"] * df["Price"]
    return df[["Created Time","Order Type","Asset Type","Fiat Type","Quantity","Price","Total Price"]]

def clear_manual_ops(user_id: str, asset: str):
    con = db(); con.execute("DELETE FROM manual_asset_ops WHERE user_id=? AND asset=?", (user_id, asset)); con.commit(); con.close()

def get_fiat_balance(user_id: str, currency: str = FIAT) -> float:
    con = db(); row = con.execute("SELECT balance FROM fiat_balance WHERE user_id=? AND currency=?", (user_id, currency)).fetchone(); con.close()
    return float(row[0]) if row else 0.0

def set_fiat_balance(user_id: str, balance: float, currency: str = FIAT):
    con = db(); con.execute("REPLACE INTO fiat_balance (user_id, currency, balance) VALUES (?,?,?)",
                            (user_id, currency, float(balance))); con.commit(); con.close()

def add_fiat_delta(user_id: str, delta: float, currency: str = FIAT) -> float:
    bal = get_fiat_balance(user_id, currency); bal2 = bal + float(delta)
    set_fiat_balance(user_id, bal2, currency); return bal2

def reset_fiat_balance(user_id: str, currency: str = FIAT):
    con = db(); con.execute("DELETE FROM fiat_balance WHERE user_id=? AND currency=?", (user_id, currency)); con.commit(); con.close()

ALT_MAP = {
    "Created Time": ["Created Time","Time","Date(UTC)","Order Time","Create Time"],
    "Order Type":   ["Order Type","Side","Type"],
    "Status":       ["Status","Order Status","Trade Status"],
    "Asset Type":   ["Asset Type","Asset","Crypto","Base","Cryptocurrency"],
    "Fiat Type":    ["Fiat Type","Fiat","Quote","Currency","Currency.1"],
    "Quantity":     ["Quantity","Amount","Crypto Amount","Base Amount","Coin Amount"],
    "Price":        ["Price","Unit Price","Fiat Price"],
    "Total Price":  ["Total Price","Total","Fiat Amount","Quote Amount","Amount(Quote)","Fiat Amount"],
}

def _coalesce_duplicate_columns(df: pd.DataFrame) -> pd.DataFrame:
    if not df.columns.has_duplicates: return df
    cols = pd.Index(df.columns); seen=set()
    for name in cols:
        if name in seen: continue
        dup_idx = [i for i,c in enumerate(cols) if c==name]
        if len(dup_idx)>1:
            base = df.iloc[:,dup_idx[0]].copy()
            for idx in dup_idx[1:]:
                cand = df.iloc[:,idx]
                try: base = base.fillna(cand)
                except Exception:
                    mask = base.astype(str).str.strip().eq("") | base.isna()
                    base.loc[mask] = cand.loc[mask]
            df.drop(df.columns[dup_idx[1:]], axis=1, inplace=True)
            df[name] = base
        seen.add(name)
    return df

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = {c.strip(): c for c in df.columns}
    rename={}
    for std, alts in ALT_MAP.items():
        for a in alts:
            if a in cols:
                rename[cols[a]] = std; break
    df = df.rename(columns=rename)
    df = _coalesce_duplicate_columns(df)
    if "Fiat Type" not in df.columns:
        for cand in ["Currency","Currency.1","Fiat"]:
            if cand in df.columns:
                df = df.rename(columns={cand:"Fiat Type"}); break
    if "Asset Type" not in df.columns and "Cryptocurrency" in df.columns:
        df = df.rename(columns={"Cryptocurrency":"Asset Type"})
    if "Total Price" not in df.columns and "Fiat Amount" in df.columns:
        df = df.rename(columns={"Fiat Amount":"Total Price"})
    if "Created Time" in df.columns:
        df["Created Time"] = pd.to_datetime(df["Created Time"], errors="coerce")
    for c in ["Quantity","Price","Total Price"]:
        if c in df.columns: df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def _file_signature(b: bytes) -> str:
    if b.startswith(b"PK\x03\x04"): return "zip"
    if b.startswith(b"\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"): return "ole"
    return "unknown"

def _read_xls_with_xlrd_raw(file_bytes: bytes) -> pd.DataFrame:
    import xlrd
    book = xlrd.open_workbook(file_contents=file_bytes)
    sh = book.sheet_by_index(0)
    headers = [str(sh.cell_value(0,c)).strip() for c in range(sh.ncols)]
    rows = [[sh.cell_value(r,c) for c in range(sh.ncols)] for r in range(1, sh.nrows)]
    df = pd.DataFrame(rows, columns=headers)
    df = _maybe_p2p_ru_csv(df)
    df = _maybe_trustee_plus(df)
    return normalize_columns(df)

def read_table(file_bytes: bytes, filename: str|None) -> pd.DataFrame:
    name = (filename or "").lower()
    sig = _file_signature(file_bytes[:8])
    if sig=="zip":
        try:    df = pd.read_excel(io.BytesIO(file_bytes), engine="openpyxl")
        except: df = pd.read_excel(io.BytesIO(file_bytes))
        df = _maybe_p2p_ru_csv(df)
        df = _maybe_trustee_plus(df)
        return normalize_columns(df)
    if sig=="ole":
        try:    df = pd.read_excel(io.BytesIO(file_bytes), engine="xlrd")
        except: df = _read_xls_with_xlrd_raw(file_bytes)
        df = _maybe_p2p_ru_csv(df)
        df = _maybe_trustee_plus(df)
        return normalize_columns(df)
    try:
        if name.endswith(".xlsx"):
            df = pd.read_excel(io.BytesIO(file_bytes), engine="openpyxl"); df = _maybe_p2p_ru_csv(df)
        df = _maybe_trustee_plus(df)
        return normalize_columns(df)
        if name.endswith(".xls"):
            try:    df = pd.read_excel(io.BytesIO(file_bytes), engine="xlrd")
            except: df = _read_xls_with_xlrd_raw(file_bytes)
            df = _maybe_p2p_ru_csv(df)
        df = _maybe_trustee_plus(df)
        return normalize_columns(df)
        if name.endswith(".csv"):
            df = pd.read_csv(io.BytesIO(file_bytes)); df = _maybe_p2p_ru_csv(df)
        df = _maybe_trustee_plus(df)
        return normalize_columns(df)
    except Exception:
        pass
    try:
        df = pd.read_excel(io.BytesIO(file_bytes)); df = _maybe_p2p_ru_csv(df)
        df = _maybe_trustee_plus(df)
        return normalize_columns(df)
    except Exception:
        df = pd.read_csv(io.BytesIO(file_bytes)); df = _maybe_p2p_ru_csv(df)
        df = _maybe_trustee_plus(df)
        return normalize_columns(df)

def _as_series(col):
    if isinstance(col, pd.DataFrame): return col.iloc[:,0]
    return col


def extract_asset_trades(df: pd.DataFrame, asset: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=["Created Time","Order Type","Asset Type","Fiat Type","Quantity","Price","Total Price"])
    status_col = _as_series(df.get("Status"))
    ok_mask = True
    if isinstance(status_col, (pd.Series, pd.DataFrame)):
        ok_values = {"completed","готово","filled","done","success","выполнено","успешно","завершено"}
        ok_mask = _as_series(status_col).astype(str).str.strip().str.lower().isin(ok_values)

    asset_series = None
    for cand in ["Asset Type","Крипта","Currency code","Cryptocurrency","Монета","Token","Токен"]:
        if cand in df.columns:
            asset_series = _as_series(df[cand]); break
    if asset_series is None:
        for cand in ["Pair","Symbol","Market","Instrument","Пара","Инструмент"]:
            if cand in df.columns:
                s = _as_series(df[cand]).astype(str).str.upper()
                s = s.str.replace(r"[\s—–\-→\\]+", "/", regex=True)
                asset_series = s.str.split("/").str[0]
                break
    if asset_series is None:
        return pd.DataFrame(columns=["Created Time","Order Type","Asset Type","Fiat Type","Quantity","Price","Total Price"])

    asset_mask = asset_series.astype(str).str.upper().str.strip()==asset
    mask = asset_mask & ok_mask

    cols = ["Created Time","Order Type","Asset Type","Fiat Type","Quantity","Price","Total Price"]
    present = [c for c in cols if c in df.columns]
    out = df.loc[mask, present].copy()
    for c in cols:
        if c not in out.columns:
            out[c] = pd.Series([None]*len(out))

    out["Created Time"] = pd.to_datetime(_as_series(out["Created Time"]), errors="coerce")
    out = out.dropna(subset=["Created Time"])

    for c in ["Quantity","Price","Total Price"]:
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")
    out = out.dropna(subset=["Quantity","Price","Total Price"], how="any")

    out["Order Type"] = _as_series(out["Order Type"]).astype(str).str.strip().str.title()
    out["Order Type"] = out["Order Type"].replace({"Покупка":"Buy","Купівля":"Buy","Продажа":"Sell","Продаж":"Sell"})

    return out.sort_values("Created Time").reset_index(drop=True)

def is_uah_uah_row(row) -> bool:
    asset = str(row.get("Asset Type","")).strip().upper()
    fiat  = str(row.get("Fiat Type","")).strip().upper()
    if asset=="UAH" and fiat=="UAH": return True
    for k in ("Symbol","Pair","Market"):
        v = row.get(k)
        if isinstance(v,str) and v.strip().upper() in ("UAH-UAH","UAH/UAH"):
            return True
    return False

def extract_uah_ops(df: pd.DataFrame) -> pd.DataFrame:
    allowed = {"","completed","готово","filled"}
    rows=[]
    for _,r in df.iterrows():
        status = str(r.get("Status","")).strip().lower()
        if status not in allowed: continue
        if not is_uah_uah_row(r): continue
        rows.append(r)
    if not rows:
        return pd.DataFrame(columns=list(df.columns))
    out = pd.DataFrame(rows).sort_values("Created Time").reset_index(drop=True)
    def infer_side(row):
        side = str(row.get("Order Type","")).strip().title()
        if side in ("Buy","Sell"): return side
        total = float(row.get("Total Price",0.0)); qty = float(row.get("Quantity",0.0))
        if total < 0 or qty < 0: return "Sell"
        if total > 0 or qty > 0: return "Buy"
        return "Buy"
    out["Side"] = out.apply(infer_side, axis=1)
    out["AmountUAH"] = out["Total Price"].astype(float).abs()
    return out

@dataclass
class SummaryAsset:
    buy_qty: float; sell_qty: float
    avg_buy_price: Optional[float]; avg_sell_price: Optional[float]
    sum_buys_uah: float; sum_sells_uah: float; cash_flow_uah: float
    start_cost_uah: float
    cogs_fifo_uah: Optional[float]; realized_pnl_fifo_uah: Optional[float]
    end_qty: float; end_cost_uah: float; end_avg_cost: Optional[float]
    shortage_qty: float; note: str

def fifo_cost_for_sold(buy_layers: List[Tuple[float,float]], sell_qty: float):
    layers: Deque[Tuple[float,float]] = deque([[float(q),float(p)] for q,p in buy_layers if q>1e-12])
    need = float(sell_qty); cogs=0.0
    while need>1e-12 and layers:
        q,p = layers[0]; take = min(q,need)
        cogs += take*p; q -= take; need -= take
        if q<=1e-12: layers.popleft()
        else: layers[0][0] = q
    ending = [(float(q),float(p)) for q,p in layers]
    shortage = max(0.0, need)
    return cogs, ending, shortage

def layers_totals(layers: List[Tuple[float,float]]):
    qty = sum(q for q,_ in layers); cost = sum(q*c for q,c in layers)
    avg = (cost/qty) if qty>1e-12 else None
    return qty, cost, avg


def _layers_avg(layers):

    try:
        total_qty = sum(q for q,_ in layers) if layers else 0.0
        total_cost = sum(q*p for q,p in layers) if layers else 0.0
        return total_qty, (total_cost/total_qty if total_qty else None)
    except Exception:
        return 0.0, None
def build_summary_asset(asset: str, trades: pd.DataFrame, opening_layers: List[Tuple[float,float]]):
    buy  = trades[trades["Order Type"]=="Buy"]  if "Order Type" in trades else pd.DataFrame(columns=trades.columns)
    sell = trades[trades["Order Type"]=="Sell"] if "Order Type" in trades else pd.DataFrame(columns=trades.columns)
    buy_qty  = float(buy["Quantity"].sum())  if "Quantity" in buy  else 0.0
    sell_qty = float(sell["Quantity"].sum()) if "Quantity" in sell else 0.0

    def wavg(df: pd.DataFrame) -> Optional[float]:
        if df is None or df.empty or "Quantity" not in df or "Price" not in df: return None
        q = float(df["Quantity"].sum());  return None if q<=0 else float((df["Price"]*df["Quantity"]).sum()/q)

    avg_buy, avg_sell = wavg(buy), wavg(sell)
    sum_buys_uah  = float(buy["Total Price"].sum())  if "Total Price" in buy  else 0.0
    sum_sells_uah = float(sell["Total Price"].sum()) if "Total Price" in sell else 0.0
    cash_flow = sum_sells_uah - sum_buys_uah

    start_cost = sum(q*c for q,c in opening_layers)
    buy_layers = list(opening_layers) + [(float(q), float(p)) for q,p in zip(buy.get("Quantity",[]), buy.get("Price",[]))]
    cogs, ending_layers, shortage = fifo_cost_for_sold(buy_layers, sell_qty)
    end_qty, end_cost, end_avg = layers_totals(ending_layers)

    note=""; realized=None; cogs_out=None
    if shortage > 1e-12:
        note = (f"⚠️ Продано на {shortage:.2f} {asset} больше, чем старт + покупки. "
                f"Жми «🔁 Перерасчёт» → выбери {asset} и введи среднюю цену (UAH).")
    else:
        cogs_out = cogs; realized = sum_sells_uah - cogs

    return SummaryAsset(
        buy_qty, sell_qty, avg_buy, avg_sell, sum_buys_uah, sum_sells_uah, cash_flow,
        start_cost, cogs_out, realized, end_qty, end_cost, end_avg, float(shortage), note
    ), ending_layers

def fee_sell(a: float) -> float:
    a = abs(float(a))
    if 900 <= a <= 2000: return 100.0
    if 2000 < a <= 20000: return a*0.065
    if 20000 < a <= 100000: return a*0.05
    return 0.0

def fee_buy(a: float) -> float:
    a = abs(float(a))
    if 900 <= a <= 4000: return 90.0
    if 4000 < a <= 20000: return a*0.02
    if 20000 < a <= 100000: return a*0.015
    return 0.0

@dataclass
class UahSummary:
    sum_buy: float; sum_sell: float; fee_total: float
    delta_net: float; start_balance: float; end_balance: float

def process_uah_ops(user_id: str, ops: pd.DataFrame) -> UahSummary:
    if ops is None or ops.empty:
        b = get_fiat_balance(user_id)
        return UahSummary(0.0,0.0,0.0,0.0,b,b)
    fees, deltas, buys, sells = [],[],[],[]
    for _,r in ops.iterrows():
        side = r["Side"]; amt = float(r["AmountUAH"])
        fee = fee_buy(amt) if side=="Buy" else fee_sell(amt)
        if side=="Buy":  buys.append(amt);  deltas.append(+amt - fee)
        else:            sells.append(amt); deltas.append(-(amt + fee))
        fees.append(fee)
    sum_buy, sum_sell = float(sum(buys)), float(sum(sells))
    fee_total, delta_net = float(sum(fees)), float(sum(deltas))
    start_bal = get_fiat_balance(user_id); end_bal = add_fiat_delta(user_id, delta_net)
    return UahSummary(sum_buy,sum_sell,fee_total,delta_net,start_bal,end_bal)

MAIN_LABELS = [
    "📥 Добавить баланс", "🧾 Ручные операции",
    "💼 Баланс", "📊 Ручная торговля", "🔁 Перерасчёт",
    "🧹 Сброс", "❓ Справка"
]
BTN_BACK = "◀️ Назад"
BTN_BUY  = "Покупка (BUY)"
BTN_SELL = "Продажа (SELL)"

def rkb(rows) -> ReplyKeyboardMarkup: return ReplyKeyboardMarkup(rows, resize_keyboard=True)
def kb_main() -> ReplyKeyboardMarkup:
    rows = [
        [KeyboardButton("📥 Добавить баланс"), KeyboardButton("🧾 Ручные операции"), KeyboardButton("💼 Баланс")],
        [KeyboardButton("📊 Ручная торговля"), KeyboardButton("🔁 Перерасчёт"), KeyboardButton("❓ Справка")],
        [KeyboardButton("🧹 Сброс")]
    ]; return rkb(rows)
def kb_add() -> ReplyKeyboardMarkup:    return rkb([[KeyboardButton("USDT"), KeyboardButton("USDC"), KeyboardButton("UAH")],[KeyboardButton(BTN_BACK)]])
def kb_manual_assets() -> ReplyKeyboardMarkup: return kb_add()
def kb_manual_side(asset: str) -> ReplyKeyboardMarkup: return rkb([[KeyboardButton(BTN_BUY), KeyboardButton(BTN_SELL)],[KeyboardButton(BTN_BACK)]])
def kb_asset_2_usd() -> ReplyKeyboardMarkup: return rkb([[KeyboardButton("USDT"), KeyboardButton("USDC")],[KeyboardButton(BTN_BACK)]])
def kb_reset() -> ReplyKeyboardMarkup:  return kb_add()

HELP_TEXT = (
    "<b>Справка</b>\n"
    "• Пришлите CSV/XLSX/XLS — посчитаю USDT/USDC (FIFO) и баланс UAH с комиссиями.\n"
    "• «Добавить баланс» — установить стартовый остаток по USDT/USDC или баланс UAH.\n"
    "• «Ручные операции» — покупка/продажа USDT/USDC и UAH→UAH.\n"
    "• «Ручная торговля» — итог по ручным операциям.\n"
    "• «Перерасчёт» — если продано больше, чем доступно.\n"
)

def fmt_opt(x: Optional[float]) -> str: return "—" if x is None else f"{x:.2f}"

ожидание_RECALC: Dict[str, Dict] = {}
ожидание_FORM:   Dict[str, Dict] = {}
MENU_STATE:     Dict[str, str] = {}

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    upsert_user_from_telegram(u)
    if user_is_blocked(str(u.id)): return
    log_chat(str(u.id), "in", "/start")
    await update.message.reply_text("Главное меню:", reply_markup=kb_main())
    await update.message.reply_text(HELP_TEXT, reply_markup=kb_main())

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    upsert_user_from_telegram(u)
    if user_is_blocked(str(u.id)): return
    log_chat(str(u.id), "in", "/help")
    await update.message.reply_text(HELP_TEXT, reply_markup=kb_main())

async def send_all_holdings(uid: str, msg_target, title: str = "Баланс"):
    lt = load_layers(uid, "USDT"); q_t = sum(q for q,_ in lt)
    avg_t = (sum(q*c for q,c in lt)/q_t) if q_t > 1e-12 else None
    lc = load_layers(uid, "USDC"); q_c = sum(q for q,_ in lc)
    avg_c = (sum(q*c for q,c in lc)/q_c) if q_c > 1e-12 else None
    uah = get_fiat_balance(uid)

    def line(asset, q, avg):
        return f"• {asset}: {q:.2f} {asset}" if avg is None else f"• {asset}: {q:.2f} {asset} (ср. {avg:.2f} {FIAT}/{asset})"

    txt = f"<b>{title}</b>\n{line('USDT', q_t, avg_t)}\n{line('USDC', q_c, avg_c)}\n• UAH: {uah:.2f} UAH"
    await msg_target.reply_text(txt, reply_markup=kb_main())

def _to_float(txt: str) -> Optional[float]:
    try: return float(txt.replace(",", ".").strip())
    except Exception: return None

async def switch_menu(m, uid: str, state: str):
    MENU_STATE[uid] = state
    mapping = {
        "main": ("Главное меню:", kb_main()),
        "add": ("Что добавить?", kb_add()),
        "manual": ("Выберите актив для ручной операции:", kb_manual_assets()),
        "recalc": ("Для какого ассета сделать перерасчёт?", kb_asset_2_usd()),
        "manualpnl": ("Посчитать ручную торговлю по активу:", kb_asset_2_usd()),
        "reset": ("Что сбросить?", kb_reset()),
    }
    if state.startswith("manual_side:"):
        asset = state.split(":")[1]
        await m.reply_text(f"Тип операции для {asset}:", reply_markup=kb_manual_side(asset)); return
    title, kb = mapping[state]
    await m.reply_text(title, reply_markup=kb)

async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    m = update.message; u = update.effective_user
    uid = str(u.id); text = (m.text or "").strip()
    upsert_user_from_telegram(u)
    if user_is_blocked(uid): return
    log_chat(uid, "in", text)

    if text in MAIN_LABELS:
        ожидание_RECALC.pop(uid, None); ожидание_FORM.pop(uid, None)

        if text == "💼 Баланс":
            await send_all_holdings(uid, m, title="Баланс")
            MENU_STATE[uid] = "main"; return

        mapping = {
            "📥 Добавить баланс":"add", "🧾 Ручные операции":"manual",
            "📊 Ручная торговля":"manualpnl", "🔁 Перерасчёт":"recalc",
            "🧹 Сброс":"reset", "❓ Справка":"main"
        }
        await switch_menu(m, uid, mapping[text])
        if text=="❓ Справка": await m.reply_text(HELP_TEXT, reply_markup=kb_main())
        return

    if text == BTN_BACK:
        state = MENU_STATE.get(uid,"main")
        if state.startswith("manual_side:"): await switch_menu(m, uid, "manual")
        else: await switch_menu(m, uid, "main")
        return

    if uid in ожидание_RECALC and uid not in ожидание_FORM:
        price = _to_float(text)
        if not price or price <= 0:
            await m.reply_text("Нужно положительное число (цена). Пример: 39.00"); return
        data = ожидание_RECALC.pop(uid)
        asset, qty = data["asset"], float(data["qty"])
        trades_all__tmp = get_last_trades(uid, asset)
        if trades_all__tmp is None or (isinstance(trades_all__tmp, pd.DataFrame) and trades_all__tmp.empty):
            await m.reply_text(f"Нет последней выписки по {asset}. Сначала отправьте файл.", reply_markup=kb_main()); return
        trades_all = trades_all__tmp
        manual = get_manual_ops(uid, asset)
        if manual is not None and not manual.empty:
            trades_all = pd.concat([trades_all, manual], ignore_index=True).sort_values("Created Time")
        opening = load_layers(uid, asset)
        summary, ending_layers = build_summary_asset(asset, trades_all, opening + [(qty, price)])
        if summary.cogs_fifo_uah is not None:
            save_layers(uid, ending_layers, asset)
        out = (f"<b>Перерасчёт остатка ({asset})</b>"
               f"• Себестоимость проданного (FIFO): {fmt_opt(summary.cogs_fifo_uah)} {FIAT}"
               f"• Прибыль (FIFO): {fmt_opt(summary.realized_pnl_fifo_uah)} {FIAT}"
               f"• Конечный остаток: {summary.end_qty:.2f} {asset}")
        await m.reply_text(out, reply_markup=kb_main()); return

    if uid in ожидание_FORM:
        flow = ожидание_FORM[uid]; t=flow["type"]; step=flow["step"]; asset=flow.get("asset"); data=flow["data"]
        if t=="set_inv":
            if step=="qty":
                qty = _to_float(text)
                if not qty or qty<=0: await m.reply_text("Введите число > 0. Пример: 500"); return
                data["qty"]=qty; flow["step"]="price"
                await m.reply_text(f"Теперь введите <b>среднюю цену</b> (UAH/{asset}):", reply_markup=ForceReply(selective=True)); return
            if step=="price":
                price = _to_float(text)
                if not price or price<=0: await m.reply_text("Введите число > 0. Пример: 40.5"); return
                save_layers(uid, [(data["qty"],price)], asset); del ожидание_FORM[uid]
                await m.reply_text(f"✅ Установлен старт: {data['qty']:.2f} {asset} по {price:.2f} {FIAT}/{asset}", reply_markup=kb_main()); return
        if t=="uah_set" and step=="amount":
            amt = _to_float(text)
            if amt is None: await m.reply_text("Введите число. Пример: 15000"); return
            set_fiat_balance(uid, amt); del ожидание_FORM[uid]
            await m.reply_text(f"✅ Баланс UAH установлен: {amt:.2f} UAH", reply_markup=kb_main()); return
        if t=="buy_asset":
            if step=="qty":
                qty=_to_float(text)
                if not qty or qty<=0: await m.reply_text("Введите число > 0. Пример: 200"); return
                data["qty"]=qty; flow["step"]="price"
                await m.reply_text(f"Введите <b>цену покупки</b> (UAH/{asset}):", reply_markup=ForceReply(selective=True)); return
            if step=="price":
                price=_to_float(text)
                if not price or price<=0: await m.reply_text("Введите число > 0. Пример: 41.2"); return
                add_manual_op(uid, asset, "Buy", data["qty"], price)
                layers = load_layers(uid, asset); layers.append((data["qty"], price)); save_layers(uid, layers, asset)
                del ожидание_FORM[uid]
                await m.reply_text(f"📝 Покупка добавлена: {data['qty']:.2f} {asset} по {price:.2f} {FIAT}/{asset}", reply_markup=kb_main()); return
        if t=="sell_asset":
            if step=="qty":
                qty=_to_float(text)
                if not qty or qty<=0: await m.reply_text("Введите число > 0. Пример: 200"); return
                data["qty"]=qty; flow["step"]="price"
                await m.reply_text(f"Введите <b>цену продажи</b> (UAH/{asset}):", reply_markup=ForceReply(selective=True)); return
            if step=="price":
                price=_to_float(text)
                if not price or price<=0: await m.reply_text("Введите число > 0. Пример: 52.3"); return
                add_manual_op(uid, asset, "Sell", data["qty"], price)
                layers = load_layers(uid, asset)
                _, ending, shortage = fifo_cost_for_sold(layers, data["qty"])
                save_layers(uid, ending, asset)
                del ожидание_FORM[uid]
                msg = f"📝 Продажа добавлена: {data['qty']:.2f} {asset} по {price:.2f} {FIAT}/{asset}"
                if shortage>1e-12: msg += f"\n⚠️ Недостача {shortage:.2f} {asset}."
                await m.reply_text(msg, reply_markup=kb_main()); return
        if t=="sell_uah" and step=="amount":
            amt=_to_float(text)
            if not amt or amt<=0: await m.reply_text("Введите число > 0. Пример: 1200"); return
            side = flow.get("side","Buy")
            fee = fee_buy(amt) if side=="Buy" else fee_sell(amt)
            delta = (+amt - fee) if side=="Buy" else (-(amt + fee))
            start = get_fiat_balance(uid); end = add_fiat_delta(uid, delta)
            del ожидание_FORM[uid]
            await m.reply_text(
                f"🧾 Ручная UAH {side} {amt:.2f} UAH\n"
                f"• Комиссия: {fee:.2f} UAH\n"
                f"• Применено к балансу: {delta:+.2f} UAH\n"
                f"• Баланс: {start:.2f} → {end:.2f} UAH",
                reply_markup=kb_main()
            ); return
    state = MENU_STATE.get(uid, "main")
    if state=="add" and text in ("USDT","USDC","UAH"):
        if text=="UAH":
            ожидание_FORM[uid] = {"type":"uah_set","asset":"UAH","step":"amount","data":{}}
            await m.reply_text("Введите сумму баланса UAH:", reply_markup=ForceReply(selective=True)); return
        asset=text
        ожидание_FORM[uid] = {"type":"set_inv","asset":asset,"step":"qty","data":{}}
        await m.reply_text(f"Введите <b>количество {asset}</b>:", reply_markup=ForceReply(selective=True)); return

    if state=="manual" and text in ("USDT","USDC","UAH"):
        if text=="UAH":
            ожидание_FORM[uid] = {"type":"sell_uah","asset":"UAH","side":"Buy","step":"amount","data":{}}
            MENU_STATE[uid] = "manual_uah_side"
            await m.reply_text("Выберите тип операции UAH→UAH:", reply_markup=ReplyKeyboardMarkup(
                [[KeyboardButton(BTN_BUY), KeyboardButton(BTN_SELL)],[KeyboardButton(BTN_BACK)]], resize_keyboard=True)); return
        MENU_STATE[uid] = f"manual_side:{text}"
        await m.reply_text(f"Тип операции для {text}:", reply_markup=kb_manual_side(text)); return

    if state.startswith("manual_side:") and text in (BTN_BUY, BTN_SELL):
        asset = state.split(":")[1]
        ожидание_FORM[uid] = {"type": ("buy_asset" if text==BTN_BUY else "sell_asset"),
                             "asset": asset, "step":"qty","data":{}}
        await m.reply_text(f"Введите <b>количество {asset}</b>:", reply_markup=ForceReply(selective=True)); return

    if state=="manual_uah_side" and text in (BTN_BUY, BTN_SELL):
        side = "Buy" if text==BTN_BUY else "Sell"
        ожидание_FORM[uid] = {"type":"sell_uah","asset":"UAH","side":side,"step":"amount","data":{}}
        await m.reply_text(f"Введите сумму {side} (UAH→UAH):", reply_markup=ForceReply(selective=True)); return

    if state=="recalc" and text in ("USDT","USDC"):
        asset=text
        trades_all__tmp = get_last_trades(uid, asset)
        if trades_all__tmp is None or (isinstance(trades_all__tmp, pd.DataFrame) and trades_all__tmp.empty):
            trades_all = pd.DataFrame(columns=["Created Time","Order Type","Asset Type","Fiat Type","Quantity","Price","Total Price"])
        else:
            trades_all = trades_all__tmp
        manual     = get_manual_ops(uid, asset)
        if not manual.empty: trades_all = pd.concat([trades_all, manual], ignore_index=True).sort_values("Created Time")
        opening    = load_layers(uid, asset)
        summary,_  = build_summary_asset(asset, trades_all, opening)
        if summary.shortage_qty > 1e-12:
            ожидание_RECALC[uid] = {"asset": asset, "qty": summary.shortage_qty}
            await m.reply_text(f"Не хватает {summary.shortage_qty:.2f} {asset}. Введите среднюю цену (UAH):",
                               reply_markup=ForceReply(selective=True))
        else:
            await m.reply_text(f"Дефицита по {asset} нет — всё проданное покрыто стартом и покупками.", reply_markup=kb_asset_2_usd())
        return

    if state=="manualpnl" and text in ("USDT","USDC"):
        asset=text; ops = get_manual_ops(uid, asset)
        if ops.empty: await m.reply_text(f"По {asset} ручных операций нет.", reply_markup=kb_asset_2_usd()); return
        summary,_ = build_summary_asset(asset, ops, opening_layers=[])
        lines = [
            f"📊 <b>Ручная торговля — {asset}</b>",
            f"• Покупка, кол-во: {summary.buy_qty:.2f} {asset}",
            f"• Продажа, кол-во: {summary.sell_qty:.2f} {asset}",
            f"• Ср. цена покупки: {fmt_opt(summary.avg_buy_price)} {FIAT}/{asset}",
            f"• Ср. цена продажи: {fmt_opt(summary.avg_sell_price)} {FIAT}/{asset}",
            f"• Сумма покупок: {summary.sum_buys_uah:.2f} {FIAT}",
            f"• Сумма продаж: {summary.sum_sells_uah:.2f} {FIAT}",
        ]
        if summary.cogs_fifo_uah is not None:
            lines += [
                f"• Себестоимость проданного (FIFO): {fmt_opt(summary.cogs_fifo_uah)} {FIAT}",
                f"• Прибыль (FIFO): {fmt_opt(summary.realized_pnl_fifo_uah)} {FIAT}",
            ]
        lines.append(f"• Конечный остаток (только ручн.): {summary.end_qty:.2f} {asset}")
        if summary.note: lines.append("\n"+ihtml.escape(summary.note))
        await m.reply_text("\n".join(lines), reply_markup=kb_asset_2_usd()); return

    if state=="reset" and text in ("USDT","USDC","UAH"):
        if text=="UAH":
            reset_fiat_balance(uid); await m.reply_text("✅ Баланс UAH сброшен.", reply_markup=kb_reset()); return
        reset_layers(uid, text); clear_manual_ops(uid, text)
        await m.reply_text(f"✅ Полный сброс {text} (стартовый остаток + ручные операции).", reply_markup=kb_reset()); return

async def on_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = str(update.effective_user.id)
    upsert_user_from_telegram(update.effective_user)
    if user_is_blocked(uid): return
    log_chat(uid, "in", f"[document] {update.message.document.file_name}")

    tg_file = await context.bot.get_file(update.message.document.file_id)
    buf = io.BytesIO(); await tg_file.download_to_memory(out=buf)
    file_bytes = buf.getvalue(); filename = update.message.document.file_name or "file"

    try:
        raw_df = read_table(file_bytes, filename)
    except Exception as e:
        await update.message.reply_text(f"Ошибка чтения файла: {ihtml.escape(str(e))}", reply_markup=kb_main()); return

    any_asset=False
    for asset in ASSETS:
        trades = extract_asset_trades(raw_df, asset)
        if trades is None or trades.empty: continue
        any_asset=True
        set_last_trades(uid, asset, trades)
        opening = load_layers(uid, asset)
        summary, ending_layers = build_summary_asset(asset, trades, opening)
        open_qty, open_avg = _layers_avg(opening)
        buy_qty = summary.buy_qty or 0.0
        sell_qty = summary.sell_qty or 0.0
        file_avg = summary.avg_buy_price or 0.0
        deficit = max(0.0, sell_qty - buy_qty)
        cogs_file = min(sell_qty, buy_qty) * file_avg
        cogs_balance = deficit * (open_avg or 0.0)
        summary.cogs_fifo_uah = cogs_file + cogs_balance
        summary.realized_pnl_fifo_uah = (summary.sum_sells_uah or 0.0) - (summary.cogs_fifo_uah or 0.0)
        remain_open_qty = max(0.0, open_qty - deficit)
        remain_open_cost = remain_open_qty * (open_avg or 0.0)
        leftover_file_qty = max(0.0, buy_qty - sell_qty)
        leftover_file_cost = leftover_file_qty * file_avg
        new_qty = remain_open_qty + leftover_file_qty
        new_avg = ((remain_open_cost + leftover_file_cost)/new_qty) if new_qty else None
        ending_layers = [(new_qty, new_avg or 0.0)] if new_qty else []
        if summary.shortage_qty <= 1e-12:
            save_layers(uid, ending_layers, asset)
        lines = [
            f"<b>{asset} — баланс/отчёт</b>",
            f"• Покупка, кол-во: {summary.buy_qty:.2f} {asset}",
            f"• Продажа, кол-во: {summary.sell_qty:.2f} {asset}",
            f"• Ср. цена покупки: {fmt_opt(summary.avg_buy_price)} {FIAT}/{asset}",
            f"• Ср. цена продажи: {fmt_opt(summary.avg_sell_price)} {FIAT}/{asset}",
            f"• Сумма покупок (по файлу): {summary.sum_buys_uah:.2f} {FIAT}",
            f"• Сумма продаж (файл+ручн.): {summary.sum_sells_uah:.2f} {FIAT}",
            f"• Денежный поток (файл+ручн.): {summary.cash_flow_uah:.2f} {FIAT}",
            *(["• Себестоимость проданного (FIFO): " + fmt_opt(summary.cogs_fifo_uah) + f" {FIAT}"] if summary.cogs_fifo_uah is not None else []),
            *(["• Прибыль (FIFO): " + fmt_opt(summary.realized_pnl_fifo_uah) + f" {FIAT}"] if summary.realized_pnl_fifo_uah is not None else []),
            f"• Конечный остаток: {summary.end_qty:.2f} {asset}"
        ]
        if summary.note: lines.append("\n"+ihtml.escape(summary.note))
        await update.message.reply_text("\n".join(lines), reply_markup=kb_main())

    uah_ops = extract_uah_ops(raw_df)
    if not uah_ops.empty:
        uah_sum = process_uah_ops(uid, uah_ops)
        lines = [
            "💴 <b>UAH — расчёт комиссий и баланса</b>",
            f"• Сумма BUY (UAH→UAH): {uah_sum.sum_buy:.2f} UAH",
            f"• Сумма SELL (UAH→UAH): {uah_sum.sum_sell:.2f} UAH",
            f"• Комиссия всего: {uah_sum.fee_total:.2f} UAH",
            f"• Чистое изменение (применено к балансу): {uah_sum.delta_net:.2f} UAH",
            f"• Баланс UAH: {uah_sum.start_balance:.2f} → {uah_sum.end_balance:.2f} UAH",
        ]
        await update.message.reply_text("\n".join(lines), reply_markup=kb_main())
    elif not any_asset:
        await update.message.reply_text("Готово. Выберите действие:", reply_markup=kb_main())
    else:
        await update.message.reply_text("Выберите действие:", reply_markup=kb_main())

def _parse_utc_storage(s: str) -> datetime:
    try: dt = datetime.fromisoformat(s)
    except Exception: dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    if dt.tzinfo is None: dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt

async def broadcast_worker(context):
    con = db(); row = con.execute(
        "SELECT id, text, target_user_id, scheduled_at FROM broadcast_queue WHERE status='ожидание' ORDER BY id LIMIT 1"
    ).fetchone()
    if not row: con.close(); return
    qid, text, target, sched = row

    now_utc = datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
    if sched:
        try:
            when_utc = _parse_utc_storage(sched)
            if when_utc > now_utc:
                con.close(); return
        except Exception:
            pass

    con.execute("UPDATE broadcast_queue SET status='sending' WHERE id=?", (qid,))
    con.commit(); con.close()

    if target:
        targets = [target]
    else:
        con = db(); targets = [r[0] for r in con.execute("SELECT user_id FROM users WHERE IFNULL(is_blocked,0)=0").fetchall()]; con.close()

    for uid in targets:
        try:
            await context.bot.send_message(chat_id=int(uid), text=text)
            log_chat(str(uid), "out", text)
            await asyncio.sleep(0.03)
        except Exception as e:
            if "Forbidden" in str(e) or "bot was blocked" in str(e):
                set_block(str(uid), True)

    con = db(); con.execute("UPDATE broadcast_queue SET status='готово' WHERE id=?", (qid,)); con.commit(); con.close()

if not ADMIN_PASSWORD:
    raise SystemExit("ADMIN_PASSWORD не задан (в .env).")

app = Flask(__name__)
app.secret_key = ADMIN_SECRET

def utc_storage_str(dt_utc: datetime) -> str:
    return dt_utc.replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

def fmt_ts(iso_like_utc: str, tz_name: str = ADMIN_TZ) -> str:
    if not iso_like_utc: return ""
    try:
        dt = datetime.fromisoformat(str(iso_like_utc))
        if dt.tzinfo is None: dt = dt.replace(tzinfo=ZoneInfo("UTC"))
        return dt.astimezone(ZoneInfo(tz_name)).strftime("%d.%m.%Y %H:%M")
    except Exception:
        return str(iso_like_utc)

app.jinja_env.filters["fmtmsk"] = fmt_ts

def parse_scheduled(form):
    d = (form.get("when_date") or "").strip()
    t = (form.get("when_time") or "").strip()
    if not (d and t): return None
    if "." in d:
        dd, mm, yyyy = d.split("."); d_iso = f"{yyyy}-{mm.zfill(2)}-{dd.zfill(2)}"
    else:
        d_iso = d
    dt_local = datetime.strptime(f"{d_iso} {t}", "%Y-%m-%d %H:%M").replace(tzinfo=ZoneInfo(ADMIN_TZ))
    return utc_storage_str(dt_local.astimezone(ZoneInfo("UTC")))

BASE = """
<!doctype html><html><head><meta charset="utf-8"><title>Admin</title>
<style>
:root{--bg:#0f172a;--card:#111827;--text:#e5e7eb;--muted:#9ca3af;--line:#334155;--primary:#2563eb;--badge:#1f2937}
*{box-sizing:border-box}body{font:14px/1.5 system-ui,Arial,sans-serif;margin:0;background:var(--bg);color:var(--text)}
.header{padding:16px 20px;border-bottom:1px solid var(--line);display:flex;gap:14px;flex-wrap:wrap}
.header a{color:#93c5fd;text-decoration:none}.header a:hover{text-decoration:underline}
.container{padding:20px;max-width:1200px;margin:0 auto}.card{background:var(--card);border:1px solid var(--line);border-radius:12px;padding:16px}
.grid{display:grid;grid-template-columns:repeat(12,1fr);gap:16px}
.stat{display:flex;flex-direction:column;gap:4px;padding:16px;background:var(--card);border:1px solid var(--line);border-radius:12px}
.stat .num{font-size:24px;font-weight:700}.stat .label{color:var(--muted)}table{border-collapse:collapse;width:100%}
th,td{border:1px solid var(--line);padding:8px}th{background:var(--card)}input,textarea{width:100%;padding:10px;background:var(--card);border:1px solid var(--line);color:var(--text);border-radius:8px}
button{padding:10px 14px;background:var(--primary);border:none;color:#fff;border-radius:8px;cursor:pointer}.badge{padding:2px 6px;border-radius:4px;background:var(--badge)}
.small{font-size:12px;color:var(--muted)}.mono{font-family:ui-monospace, Menlo, monospace}.row{display:flex;gap:8px;align-items:center}
</style></head><body>
<div class="header">
  <a href="{{ url_for('index') }}">🏠 Главная</a>
  <a href="{{ url_for('users') }}">👥 Пользователи</a>
  <a href="{{ url_for('broadcast') }}">📣 Рассылка</a>
  <a href="{{ url_for('blacklist') }}">⛔ Черный список</a>
  <a href="{{ url_for('logout') }}">🚪 Выход</a>
</div>
<div class="container">{{ body|safe }}</div></body></html>
"""

def render(page_tpl: str, **ctx):
    inner = render_template_string(page_tpl, **ctx)
    return render_template_string(BASE, body=inner, **ctx)

@app.route("/login", methods=["GET","POST"])
def login():
    if request.method == "POST":
        if (request.form.get("password") or "") == ADMIN_PASSWORD:
            session["auth"] = True
            return redirect(url_for("index"))
    return render_template_string("""
<!doctype html><html><head><meta charset="utf-8"><title>Login</title></head>
<body style="font:14px/1.4 system-ui,Arial,sans-serif;margin:40px;background:#0f172a;color:#e5e7eb">
  <h2>Вход в админ-панель</h2>
  <form method="post" style="max-width:360px">
    <label>Пароль</label><br>
    <input type="password" name="password">
    <div style="margin-top:12px"><button>Войти</button></div>
  </form>
</body></html>""")

@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))

def login_required(fn):
    def wrap(*a, **kw):
        if not session.get("auth"): return redirect(url_for("login"))
        return fn(*a, **kw)
    wrap.__name__ = fn.__name__
    return wrap

@app.route("/")
@login_required
def index():
    con = db()
    users_total = con.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    blocked     = con.execute("SELECT COUNT(*) FROM users WHERE IFNULL(is_blocked,0)=1").fetchone()[0]
    queue_open  = con.execute("SELECT COUNT(*) FROM broadcast_queue WHERE status!='готово'").fetchone()[0]
    last_rows   = con.execute("SELECT * FROM users ORDER BY last_seen DESC LIMIT 10").fetchall()
    qrows       = con.execute("SELECT * FROM broadcast_queue ORDER BY id DESC LIMIT 10").fetchall()
    con.close()

    now_utc = datetime.utcnow(); active_24h = 0
    for r in last_rows:
        try:
            dt = datetime.fromisoformat(r["last_seen"])
            if (now_utc - dt) <= timedelta(hours=24): active_24h += 1
        except Exception: pass

    return render("""
<h2>Админ-панель</h2>
<div class="grid" style="margin:12px 0">
  <div class="stat" style="grid-column:span 3"><div class="num">{{ users_total }}</div><div class="label">Пользователи всего</div><div class="small">Активны за 24ч: {{ active_24h }}</div></div>
  <div class="stat" style="grid-column:span 3"><div class="num">{{ blocked }}</div><div class="label">В чёрном списке</div></div>
  <div class="stat" style="grid-column:span 3"><div class="num">{{ queue_open }}</div><div class="label">Заданий в очереди</div></div>
  <div class="card" style="grid-column:span 3">
    <form method="post" action="{{ url_for('broadcast_quick') }}">
      <div class="small" style="margin-bottom:6px">Быстрая рассылка (всем):</div>
      <textarea name="text" rows="3" placeholder="Текст сообщения"></textarea>
      <div class="small" style="margin-top:8px">Время отправки (МСК):</div>
      <div class="row">
        <input type="text" name="when_date" placeholder="ДД.ММ.ГГГГ" maxlength="10" pattern="\\d{2}\\.\\d{2}\\.\\d{4}">
        <input type="text" name="when_time" placeholder="ЧЧ:ММ" maxlength="5" pattern="\\d{2}:\\d{2}">
      </div>
      <div style="margin-top:8px;text-align:right"><button>Поставить</button></div>
    </form>
  </div>
</div>

<div class="grid" style="margin-top:8px">
  <div class="card" style="grid-column:span 7">
    <div style="display:flex;justify-content:space-between;align-items:center">
      <h3 style="margin:0">Последние пользователи</h3>
      <a href="{{ url_for('users') }}" class="small">Все пользователи →</a>
    </div>
    <table>
      <tr><th>ID</th><th>Имя</th><th>Username</th><th>Последняя активность</th><th>Статус</th></tr>
      {% for r in last_rows %}
      <tr>
        <td class="mono">{{ r['user_id'] }}</td>
        <td>{{ r['first_name'] }} {{ r['last_name'] }}</td>
        <td>@{{ r['username'] or '' }}</td>
        <td>{{ r['last_seen']|fmtmsk }}</td>
        <td>{% if r['is_blocked'] %}<span class="badge">blocked</span>{% else %}<span class="badge">ok</span>{% endif %}</td>
      </tr>
      {% endfor %}
    </table>
  </div>

  <div class="card" style="grid-column:span 5">
    <h3 style="margin-top:0">Очередь рассылок</h3>
    <table>
      <tr><th>ID</th><th>Создано</th><th>Запланировано</th><th>Статус</th><th>Кому</th></tr>
      {% for r in qrows %}
      <tr>
        <td>{{ r['id'] }}</td>
        <td>{{ r['created_at']|fmtmsk }}</td>
        <td>{{ r['scheduled_at']|fmtmsk }}</td>
        <td>{{ r['status'] }}</td>
        <td>{{ r['target_user_id'] or 'всем' }}</td>
      </tr>
      {% endfor %}
    </table>
    <p class="small" style="margin-top:6px">Часовой пояс: <span class="mono">{{ tz }}</span></p>
  </div>
</div>
""", users_total=users_total, blocked=blocked, queue_open=queue_open,
       last_rows=last_rows, qrows=qrows, active_24h=active_24h, tz=ADMIN_TZ)


@app.route("/broadcast/quick", methods=["POST"])
@login_required
def broadcast_quick():
    text = (request.form.get("text") or "").strip()
    when = parse_scheduled(request.form)
    if text:
        con = db(); con.execute(
            "INSERT INTO broadcast_queue (created_at,scheduled_at,text,status,author) VALUES (?,?,?,?,?)",
            (utc_storage_str(datetime.utcnow()), when, text, "ожидание", "admin")
        ); con.commit(); con.close()
    return redirect(url_for("index"))

@app.route("/broadcast", methods=["GET","POST"])
@login_required
def broadcast():
    if request.method == "POST":
        text = (request.form.get("text") or "").strip()
        when = parse_scheduled(request.form)
        if text:
            con = db(); con.execute(
                "INSERT INTO broadcast_queue (created_at,scheduled_at,text,status,author) VALUES (?,?,?,?,?)",
                (utc_storage_str(datetime.utcnow()), when, text, "ожидание", "admin")
            ); con.commit(); con.close()
            return redirect(url_for('broadcast'))
    con = db(); queue = con.execute("SELECT * FROM broadcast_queue ORDER BY id DESC LIMIT 30").fetchall(); con.close()
    return render("""
<h2>Рассылка</h2>
<form method="post">
  <textarea name="text" rows="6" placeholder="Текст для всех пользователей"></textarea>
  <div class="small" style="margin-top:8px">Время отправки (МСК):</div>
  <div class="row">
    <input type="text" name="when_date" placeholder="ДД.ММ.ГГГГ" maxlength="10" pattern="\\d{2}\\.\\d{2}\\.\\d{4}">
    <input type="text" name="when_time" placeholder="ЧЧ:ММ" maxlength="5" pattern="\\d{2}:\\d{2}">
  </div>
  <div style="margin-top:8px;text-align:right"><button>Поставить в очередь</button></div>
</form>
<h3 style="margin-top:18px">Последние задания</h3>
<table>
<tr><th>ID</th><th>Создано</th><th>Запланировано</th><th>Статус</th><th>Кому</th><th>Текст</th></tr>
{% for r in queue %}
<tr>
  <td>{{ r['id'] }}</td>
  <td>{{ r['created_at']|fmtmsk }}</td>
  <td>{{ r['scheduled_at']|fmtmsk }}</td>
  <td>{{ r['status'] }}</td>
  <td>{{ r['target_user_id'] or 'всем' }}</td>
  <td style="white-space:pre-wrap">{{ r['text'][:200] }}{% if r['text']|length>200 %}…{% endif %}</td>
</tr>
{% endfor %}
</table>
""", queue=queue)

@app.route("/users")
@login_required
def users():
    q = request.args.get("q","").strip()
    con = db()
    if q:
        rows = con.execute("""SELECT * FROM users
                              WHERE user_id LIKE ? OR username LIKE ? OR first_name LIKE ? OR last_name LIKE ?
                              ORDER BY last_seen DESC""",(f"%{q}%",)*4).fetchall()
    else:
        rows = con.execute("SELECT * FROM users ORDER BY last_seen DESC LIMIT 500").fetchall()
    con.close()
    return render("""
<h2>Пользователи</h2>
<form method="get"><input name="q" placeholder="Поиск по id/username/имени" value="{{ request.args.get('q','') }}"></form><br>
<table>
<tr><th>ID</th><th>Имя</th><th>Username</th><th>Последняя активность</th><th>Статус</th><th>Действия</th></tr>
{% for r in rows %}
<tr>
  <td class="mono">{{ r['user_id'] }}</td>
  <td>{{ r['first_name'] }} {{ r['last_name'] }}</td>
  <td>@{{ r['username'] or '' }}</td>
  <td>{{ r['last_seen']|fmtmsk }}</td>
  <td>{% if r['is_blocked'] %}<span class="badge">blocked</span>{% else %}<span class="badge">ok</span>{% endif %}</td>
  <td>
    <a href="{{ url_for('user_view', uid=r['user_id']) }}">История</a> |
    <a href="{{ url_for('user_message', uid=r['user_id']) }}">Написать</a> |
    {% if r['is_blocked'] %}
      <a href="{{ url_for('unblock', uid=r['user_id']) }}">Разблокировать</a>
    {% else %}
      <a href="{{ url_for('block', uid=r['user_id']) }}">Заблокировать</a>
    {% endif %}
  </td>
</tr>
{% endfor %}
</table>
""", rows=rows)

@app.route("/users/<uid>")
@login_required
def user_view(uid):
    con = db()
    user = con.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()
    if not user: abort(404)
    log_rows = con.execute("SELECT * FROM chat_log WHERE user_id=? ORDER BY id DESC LIMIT 200", (uid,)).fetchall()
    con.close()
    return render("""
<h2>История — {{ user['first_name'] }} @{{ user['username'] or '' }} <span class="mono">({{ user['user_id'] }})</span></h2>
<p>Статус: {% if user['is_blocked'] %}<b>Заблокирован</b>{% else %}<b>OK</b>{% endif %}</p>
<p><a href="{{ url_for('user_message', uid=user['user_id']) }}">Написать личное сообщение</a></p>
<table>
<tr><th>Время</th><th>Кто</th><th>Текст</th></tr>
{% for r in log_rows %}
<tr>
  <td class="mono">{{ r['ts']|fmtmsk }}</td>
  <td>{{ '👤 user' if r['direction']=='in' else '🤖 bot' }}</td>
  <td style="white-space:pre-wrap">{{ r['text'] }}</td>
</tr>
{% endfor %}
</table>
""", user=user, log_rows=log_rows)

@app.route("/users/<uid>/message", methods=["GET","POST"])
@login_required
def user_message(uid):
    if request.method == "POST":
        text = (request.form.get("text") or "").strip()
        when = parse_scheduled(request.form)
        if text:
            con = db(); con.execute(
                "INSERT INTO broadcast_queue (created_at,scheduled_at,text,status,author,target_user_id) VALUES (?,?,?,?,?,?)",
                (utc_storage_str(datetime.utcnow()), when, text, "ожидание", "admin", uid)
            ); con.commit(); con.close()
        return redirect(url_for("user_view", uid=uid))
    return render("""
<h2>Отправить личное сообщение</h2>
<form method="post">
  <textarea name="text" rows="6" placeholder="Текст сообщения"></textarea>
  <div class="small" style="margin-top:8px">Время отправки (МСК):</div>
  <div class="row">
    <input type="text" name="when_date" placeholder="ДД.ММ.ГГГГ" maxlength="10" pattern="\\d{2}\\.\\d{2}\\.\\d{4}">
    <input type="text" name="when_time" placeholder="ЧЧ:ММ" maxlength="5" pattern="\\d{2}:\\d{2}">
  </div>
  <p style="margin-top:10px"><button>Поставить</button> <a href="{{ url_for('user_view', uid=uid) }}">Отмена</a></p>
</form>
""", uid=uid)

@app.route("/blacklist")
@login_required
def blacklist():
    con = db(); rows = con.execute("SELECT * FROM users WHERE IFNULL(is_blocked,0)=1 ORDER BY last_seen DESC").fetchall(); con.close()
    return render("""
<h2>Черный список</h2>
<table>
<tr><th>ID</th><th>Имя</th><th>Username</th><th>Последняя активность</th><th>Действия</th></tr>
{% for r in rows %}
<tr>
  <td class="mono">{{ r['user_id'] }}</td>
  <td>{{ r['first_name'] }} {{ r['last_name'] }}</td>
  <td>@{{ r['username'] or '' }}</td>
  <td>{{ r['last_seen']|fmtmsk }}</td>
  <td><a href="{{ url_for('unblock', uid=r['user_id']) }}">Разблокировать</a></td>
</tr>
{% endfor %}
</table>
""", rows=rows)

@app.route("/block/<uid>")
@login_required
def block(uid):
    con = db(); con.execute("UPDATE users SET is_blocked=1 WHERE user_id=?", (uid,)); con.commit(); con.close()
    return redirect(request.referrer or url_for("users"))

@app.route("/unblock/<uid>")
@login_required
def unblock(uid):
    con = db(); con.execute("UPDATE users SET is_blocked=0 WHERE user_id=?", (uid,)); con.commit(); con.close()
    return redirect(request.referrer or url_for("users"))

def run_admin():

    app.run(host="0.0.0.0", port=8080, debug=False, use_reloader=False)

def run_bot():
    token = os.environ.get("TELEGRAM_BOT_TOKEN", "")
    if not token:
        raise SystemExit("TELEGRAM_BOT_TOKEN не задан (в .env).")

    application = (Application.builder()
                   .token(token)
                   .defaults(Defaults(parse_mode=constants.ParseMode.HTML))
                   .build())

    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help",  cmd_help))
    application.add_handler(MessageHandler(filters.Document.ALL, on_document))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    application.job_queue.run_repeating(broadcast_worker, interval=5, first=5)

    log.info("Bot is running...")
    application.run_polling()

def main():
    db_init()
    threading.Thread(target=run_admin, daemon=True).start()
    run_bot()

if __name__ == "__main__":
    main()

