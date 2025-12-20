import os
import re
import time
import math
import asyncio
import logging
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple, Set
from html import escape
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

import aiohttp
from aiohttp import web
import asyncpg

from aiogram import Bot, Dispatcher, Router, F
from aiogram.filters import CommandStart
from aiogram.types import (
    Message, CallbackQuery,
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
)
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramBadRequest

import json
import random
import socket

# Load .env if present
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

# ---------------------------- logging ----------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s"
)
log = logging.getLogger("asset-accountant-bot")

# ---------------------------- config ----------------------------
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()

# DB (Postgres / Neon)
DB_BACKEND = os.getenv("DB_BACKEND", "postgres").strip().lower()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
PG_POOL_SIZE = int(os.getenv("PG_POOL_SIZE", "5"))

PRICE_POLL_SECONDS = int(os.getenv("PRICE_POLL_SECONDS", "90"))
SNAPSHOT_EVERY_SECONDS = int(os.getenv("SNAPSHOT_EVERY_SECONDS", "3600"))

if not BOT_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN. Put it into your .env (BOT_TOKEN=...)")

if DB_BACKEND == "postgres" and not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL (Neon). Set it in Render env.")

RISK_LEVELS = [5, 10, 25]
TP_LEVELS = [5, 10, 25]

async def run_health_server():
    app = web.Application()

    async def health(request):
        return web.Response(text="ok")

    app.router.add_get("/", health)
    app.router.add_get("/health", health)

    runner = web.AppRunner(app)
    await runner.setup()

    port = int(os.getenv("PORT", "10000"))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()

    # держим сервер живым
    while True:
        await asyncio.sleep(3600)

# ---------------------------- UI helpers ----------------------------
def main_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Добавить актив"), KeyboardButton(text="📊 Сводка")],
            [KeyboardButton(text="✏️ Редактировать список активов"), KeyboardButton(text="🗑 Удалить актив")],
            [KeyboardButton(text="📅 PNL за неделю"), KeyboardButton(text="🗓 PNL за месяц")],
        ],
        resize_keyboard=True
    )

def summary_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Обновить", callback_data="summary:refresh")],
        [InlineKeyboardButton(text="➕ Добавить", callback_data="nav:add")],
        [InlineKeyboardButton(text="✏️ Редактировать", callback_data="nav:edit"),
         InlineKeyboardButton(text="🗑 Удалить", callback_data="nav:delete")]
    ])

def fmt_usd(x: float) -> str:
    return f"{x:,.2f}"

def fmt_qty(x: float) -> str:
    # reasonable crypto qty formatting
    if x == 0:
        return "0"
    if abs(x) >= 1:
        return f"{x:,.6f}".rstrip("0").rstrip(".")
    return f"{x:.10f}".rstrip("0").rstrip(".")

def money_usd(x: float) -> str:
    return f"${fmt_usd(x)}"

def sign_money(x: float) -> str:
    s = "+" if x >= 0 else "-"
    return f"{s}${fmt_usd(abs(x))}"

def sign_pct(x: float) -> str:
    s = "+" if x >= 0 else "-"
    return f"{s}{abs(x):.2f}%"

def pnl_icon(pnl_usd: float) -> str:
    return "📈" if pnl_usd >= 0 else "📉"

def format_alert_line(risk_pcts: List[int], tp_pcts: List[int]) -> str:
    r = set(int(x) for x in (risk_pcts or []))
    t = set(int(x) for x in (tp_pcts or []))

    both = sorted(r & t)
    only_r = sorted(r - t)
    only_t = sorted(t - r)

    parts: List[str] = []
    parts += [f"-{p}%" for p in only_r]
    parts += [f"+-{p}%" for p in both]
    parts += [f"+{p}%" for p in only_t]

    body = " ".join(parts) if parts else "❌"
    return f"🔔 АЛЕРТ: {body}"

def fmt_price(x: Optional[float]) -> str:
    if x is None:
        return "—"
    ax = abs(x)
    if ax >= 1000:
        return f"{x:,.2f}"
    if ax >= 1:
        return f"{x:,.4f}".rstrip("0").rstrip(".")
    if ax >= 0.01:
        return f"{x:,.6f}".rstrip("0").rstrip(".")
    return (f"{x:.10f}".rstrip("0").rstrip(".")) or "0"

def safe_float(text: str) -> Optional[float]:
    t = (text or "").strip().replace(",", ".")
    t = re.sub(r"\s+", "", t)
    try:
        v = float(t)
        if math.isfinite(v):
            return v
        return None
    except Exception:
        return None

class CoinGeckoClient:
    BASE = os.getenv("COINGECKO_BASE", "https://api.coingecko.com/api/v3").strip()

    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None

        # NEW: cache per-id (price)
        self._price_cache_id: Dict[str, Tuple[float, float]] = {}

        # NEW: cache for search(query)
        self._search_cache: Dict[str, Tuple[float, List[dict]]] = {}

        # NEW: limiter (simple spacing between requests)
        self._rl_lock = asyncio.Lock()
        self._last_request_ts = 0.0
        self._min_interval_sec = float(os.getenv("COINGECKO_MIN_INTERVAL_SEC", "0.35"))
        # 0.35s ~= до ~170 req/min в "идеале"; для free-tier можно и 0.6-1.0

        # NEW: adaptive backoff (when CoinGecko returns 429)
        self._base_min_interval_sec = self._min_interval_sec
        self._penalty_until_ts = 0.0
        self._penalty_min_interval_sec = self._min_interval_sec
        self._penalty_ttl_sec = 0  # extra TTL during penalty window

        # network lock not needed: rate-limit + retries already protect us
        self._net_lock = None

        self._api_key = os.getenv("COINGECKO_API_KEY", "").strip()
        self._headers = {
            "User-Agent": "asset-accountant-bot/1.0 (+https://github.com/your/repo)",
            "Accept": "application/json",
        }
        if self._api_key:
            self._headers["x-cg-demo-api-key"] = self._api_key
            self._headers["x-cg-pro-api-key"] = self._api_key

    async def session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=20, connect=10, sock_read=15)
            force_ipv4 = os.getenv("FORCE_IPV4", "0").strip() == "1"
            connector = aiohttp.TCPConnector(family=socket.AF_INET) if force_ipv4 else aiohttp.TCPConnector()
            self._session = aiohttp.ClientSession(timeout=timeout, connector=connector, headers=self._headers)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    def _enable_penalty(self, *, retry_after: float):
        # Increase throttling for a while to reduce 429s.
        now = time.time()
        # window: at least 120s, plus server hint
        window = max(120.0, retry_after, 0.0)
        self._penalty_until_ts = max(self._penalty_until_ts, now + window)

        # min interval: grow up to 2.5s
        self._penalty_min_interval_sec = min(
            max(self._penalty_min_interval_sec * 1.5, self._min_interval_sec),
            2.5
        )

        # cache TTL penalty: grow up to +900s
        self._penalty_ttl_sec = min(
            max(int(self._penalty_ttl_sec * 1.5), 120),
            900
        )

    async def _rate_limit_wait(self):
        # simple global pacing between requests (+ adaptive penalty on 429)
        async with self._rl_lock:
            now = time.time()
            in_penalty = now < self._penalty_until_ts
            interval = self._penalty_min_interval_sec if in_penalty else self._min_interval_sec

            wait = (self._last_request_ts + interval) - now
            if wait > 0:
                await asyncio.sleep(wait)
            self._last_request_ts = time.time()

            # decay penalty when window ends
            if not in_penalty:
                self._penalty_min_interval_sec = max(
                    self._base_min_interval_sec,
                    self._penalty_min_interval_sec * 0.9
                )
                self._penalty_ttl_sec = int(self._penalty_ttl_sec * 0.9)

    async def _get_json(self, path: str, params: Dict[str, str], *, tries: int = 5) -> dict:
        url = f"{self.BASE}{path}"
        backoff = 1.0
        last_exc: Optional[BaseException] = None

        for attempt in range(1, tries + 1):
            try:
                s = await self.session()

                await self._rate_limit_wait()

                # network request (rate-limit already applied)
                async with s.get(url, params=params) as r:
                    status = r.status
                    text = await r.text()
                    headers = dict(r.headers)

                if status == 200:
                    try:
                        return json.loads(text) if text else {}
                    except Exception as e:
                        raise RuntimeError(f"CoinGecko bad JSON ({path}): {text[:200]}") from e

                if status == 429:
                    ra = headers.get("Retry-After", "")
                    try:
                        retry_after = float(ra)
                    except Exception:
                        retry_after = 0.0

                    self._enable_penalty(retry_after=retry_after)

                    sleep_s = max(retry_after, backoff) + random.random() * 0.25
                    log.warning(
                        "CoinGecko 429 on %s (attempt %d/%d). Sleep %.2fs. Body=%r",
                        path, attempt, tries, sleep_s, text[:200]
                    )
                    await asyncio.sleep(sleep_s)
                    backoff = min(backoff * 2.0, 30.0)
                    continue

                if 500 <= status < 600:
                    log.warning(
                        "CoinGecko %d on %s (attempt %d/%d). Backoff %.2fs. Body=%r",
                        status, path, attempt, tries, backoff, text[:200]
                    )
                    await asyncio.sleep(backoff + random.random() * 0.25)
                    backoff = min(backoff * 2.0, 30.0)
                    continue

                raise RuntimeError(f"CoinGecko HTTP {status} on {path}: {text[:250]}")

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_exc = e
                log.warning("CoinGecko network error on %s (attempt %d/%d): %r",
                            path, attempt, tries, e)
                await asyncio.sleep(backoff + random.random() * 0.25)
                backoff = min(backoff * 2.0, 30.0)

            except Exception as e:
                last_exc = e
                log.warning("CoinGecko error on %s (attempt %d/%d): %r",
                            path, attempt, tries, e)
                await asyncio.sleep(backoff + random.random() * 0.25)
                backoff = min(backoff * 2.0, 30.0)

        raise last_exc or RuntimeError("CoinGecko request failed")

    async def search(self, query: str, ttl_sec: int = 600) -> List[dict]:
        q = (query or "").strip().lower()
        if not q:
            return []

        now = time.time()
        rec = self._search_cache.get(q)
        if rec and now - rec[0] <= ttl_sec:
            return rec[1]

        data = await self._get_json("/search", {"query": query})
        coins = data.get("coins", []) or []
        out = []
        for c in coins:
            out.append({
                "id": c.get("id"),
                "name": c.get("name"),
                "symbol": (c.get("symbol") or "").upper(),
            })

        self._search_cache[q] = (now, out)
        return out

    async def simple_prices_usd(self, ids: List[str], ttl_sec: int = 180) -> Dict[str, float]:
        ids = [i for i in ids if i]
        if not ids:
            return {}

        now = time.time()
        in_penalty = now < self._penalty_until_ts
        effective_ttl = ttl_sec + (self._penalty_ttl_sec if in_penalty else 0)

        uniq = sorted(set(ids))

        # take fresh from per-id cache
        fresh: Dict[str, float] = {}
        stale: List[str] = []
        for cid in uniq:
            rec = self._price_cache_id.get(cid)
            if rec and now - rec[0] <= effective_ttl:
                fresh[cid] = rec[1]
            else:
                stale.append(cid)

        out: Dict[str, float] = dict(fresh)
        if not stale:
            return out

        CHUNK = 100
        for i in range(0, len(stale), CHUNK):
            chunk = stale[i:i + CHUNK]
            data = await self._get_json("/simple/price", {"ids": ",".join(chunk), "vs_currencies": "usd"})
            for cid, row in (data or {}).items():
                try:
                    price = float(row["usd"])
                except Exception:
                    continue
                out[cid] = price
                self._price_cache_id[cid] = (now, price)

        return out

cg = CoinGeckoClient()

# ---------------------------- DB (Postgres / Neon) ----------------------------
pg_pool: Optional[asyncpg.Pool] = None

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS users (
  user_id BIGINT PRIMARY KEY,
  currency TEXT NOT NULL DEFAULT 'USD',
  last_summary_chat_id BIGINT,
  last_summary_message_id BIGINT
);

CREATE TABLE IF NOT EXISTS assets (
  id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  symbol TEXT NOT NULL,
  coingecko_id TEXT NOT NULL,
  name TEXT,
  invested_usd DOUBLE PRECISION NOT NULL,
  entry_price DOUBLE PRECISION NOT NULL,
  qty_override DOUBLE PRECISION,
  created_at BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_assets_user ON assets(user_id);
CREATE INDEX IF NOT EXISTS idx_assets_cgid ON assets(coingecko_id);

CREATE TABLE IF NOT EXISTS alerts (
  id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  asset_id BIGINT NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
  type TEXT NOT NULL,               -- 'RISK' or 'TP'
  pct INTEGER NOT NULL,             -- 5/10/25
  target_price DOUBLE PRECISION NOT NULL,
  triggered INTEGER NOT NULL DEFAULT 0,
  triggered_at BIGINT
);

CREATE INDEX IF NOT EXISTS idx_alerts_asset ON alerts(asset_id);
CREATE INDEX IF NOT EXISTS idx_alerts_triggered ON alerts(triggered);

CREATE TABLE IF NOT EXISTS pnl_snapshots (
  id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
  user_id BIGINT NOT NULL REFERENCES users(user_id) ON DELETE CASCADE,
  ts BIGINT NOT NULL,
  total_value_usd DOUBLE PRECISION NOT NULL,
  total_invested_usd DOUBLE PRECISION NOT NULL,
  total_pnl_usd DOUBLE PRECISION NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_snap_user_ts ON pnl_snapshots(user_id, ts);
"""

async def init_db():
    global pg_pool
    if DB_BACKEND != "postgres":
        raise RuntimeError(f"Unsupported DB_BACKEND={DB_BACKEND}. Use postgres.")

    pg_pool = await asyncpg.create_pool(
        dsn=DATABASE_URL,
        min_size=1,
        max_size=PG_POOL_SIZE,
        command_timeout=30,
        statement_cache_size=0,  # FIX: не кешируем prepared statements
    )
    async with pg_pool.acquire() as conn:
        await conn.execute(SCHEMA_SQL)

        # MIGRATION: для старых БД, где assets уже есть без qty_override
        try:
            await conn.execute("ALTER TABLE assets ADD COLUMN IF NOT EXISTS qty_override DOUBLE PRECISION;")
        except Exception:
            log.exception("Migration failed: ALTER TABLE assets ADD COLUMN qty_override")
            raise

async def db_exec(sql: str, params: tuple = ()):
    assert pg_pool is not None
    async with pg_pool.acquire() as conn:
        try:
            await conn.execute(sql, *params)
            return
        except asyncpg.exceptions.InvalidCachedStatementError:
            # FIX: протухший cached plan после DDL/ALTER/настроек
            try:
                await conn.reload_schema_state()
            except Exception:
                pass
            await conn.execute(sql, *params)
            return

async def db_fetchone(sql: str, params: tuple = ()):
    assert pg_pool is not None
    async with pg_pool.acquire() as conn:
        try:
            row = await conn.fetchrow(sql, *params)
        except asyncpg.exceptions.InvalidCachedStatementError:
            try:
                await conn.reload_schema_state()
            except Exception:
                pass
            row = await conn.fetchrow(sql, *params)
        return dict(row) if row else None

async def db_fetchall(sql: str, params: tuple = ()):
    assert pg_pool is not None
    async with pg_pool.acquire() as conn:
        try:
            rows = await conn.fetch(sql, *params)
        except asyncpg.exceptions.InvalidCachedStatementError:
            try:
                await conn.reload_schema_state()
            except Exception:
                pass
            rows = await conn.fetch(sql, *params)
        return [dict(r) for r in rows]

async def upsert_user(user_id: int):
    await db_exec(
        "INSERT INTO users(user_id) VALUES ($1) ON CONFLICT(user_id) DO NOTHING",
        (user_id,)
    )

async def set_last_summary_message(user_id: int, chat_id: int, message_id: int):
    await db_exec(
        "UPDATE users SET last_summary_chat_id=$1, last_summary_message_id=$2 WHERE user_id=$3",
        (chat_id, message_id, user_id)
    )

async def list_assets(user_id: int):
    return await db_fetchall(
        "SELECT * FROM assets WHERE user_id=$1 ORDER BY id DESC",
        (user_id,)
    )

async def get_asset(user_id: int, asset_id: int):
    return await db_fetchone(
        "SELECT * FROM assets WHERE user_id=$1 AND id=$2",
        (user_id, asset_id)
    )

async def add_asset_row(user_id: int, symbol: str, coingecko_id: str, name: str,
                        invested_usd: float, entry_price: float,
                        qty_override: Optional[float] = None) -> int:
    ts = int(time.time())

    row = await db_fetchone(
        """
        INSERT INTO assets(
            user_id, symbol, coingecko_id, name,
            invested_usd, entry_price, qty_override, created_at
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING id
        """,
        (user_id, symbol.upper(), coingecko_id, name,
         invested_usd, entry_price, qty_override, ts)
    )

    if not row or "id" not in row:
        raise RuntimeError("add_asset_row: INSERT succeeded but no id returned")

    return int(row["id"])

async def update_asset_row(user_id: int, asset_id: int,
                           invested_usd: float, entry_price: float,
                           qty_override: Optional[float]):
    await db_exec(
        "UPDATE assets SET invested_usd=$1, entry_price=$2, qty_override=$3 "
        "WHERE user_id=$4 AND id=$5",
        (invested_usd, entry_price, qty_override, user_id, asset_id)
    )

async def delete_asset_row(user_id: int, asset_id: int):
    # alerts удалятся сами из-за ON DELETE CASCADE, но оставим “явно” удаление assets
    await db_exec("DELETE FROM assets WHERE user_id=$1 AND id=$2", (user_id, asset_id))

async def replace_alerts(asset_id: int, alerts: List[Tuple[str, int, float]]):
    await db_exec("DELETE FROM alerts WHERE asset_id=$1", (asset_id,))
    for t, pct, target in alerts:
        await db_exec(
            "INSERT INTO alerts(asset_id, type, pct, target_price) VALUES ($1, $2, $3, $4)",
            (asset_id, t, pct, target)
        )

async def list_alerts_for_asset(asset_id: int):
    return await db_fetchall("SELECT * FROM alerts WHERE asset_id=$1", (asset_id,))

async def recompute_alert_targets(asset_id: int, new_entry: float):
    if new_entry <= 0:
        await replace_alerts(asset_id, [])
        return

    rows = await list_alerts_for_asset(asset_id)
    updated: List[Tuple[str, int, float]] = []
    for r in rows:
        t = str(r["type"])
        pct = int(r["pct"])
        target = new_entry * (1 - pct / 100.0) if t == "RISK" else new_entry * (1 + pct / 100.0)
        updated.append((t, pct, float(target)))
    if updated:
        await replace_alerts(asset_id, updated)

async def pending_alerts_joined():
    return await db_fetchall(
        """
        SELECT
  	 al.id AS alert_id, al.type, al.pct, al.target_price,
  	 a.id AS asset_id, a.user_id, a.symbol, a.coingecko_id, a.name,
  	 a.invested_usd, a.entry_price, a.qty_override

        FROM alerts al
        JOIN assets a ON a.id = al.asset_id
        WHERE al.triggered = 0
        """
    )

async def mark_alert_triggered(alert_id: int):
    await db_exec(
        "UPDATE alerts SET triggered=1, triggered_at=$1 WHERE id=$2",
        (int(time.time()), alert_id)
    )

async def all_users() -> List[int]:
    rows = await db_fetchall("SELECT user_id FROM users")
    return [int(r["user_id"]) for r in rows]

async def insert_snapshot(user_id: int, total_value: float, total_invested: float):
    pnl = total_value - total_invested
    await db_exec(
        """
        INSERT INTO pnl_snapshots(user_id, ts, total_value_usd, total_invested_usd, total_pnl_usd)
        VALUES ($1, $2, $3, $4, $5)
        """,
        (user_id, int(time.time()), total_value, total_invested, pnl)
    )

async def get_snapshot_latest(user_id: int):
    return await db_fetchone(
        "SELECT * FROM pnl_snapshots WHERE user_id=$1 ORDER BY ts DESC LIMIT 1",
        (user_id,)
    )

async def get_snapshot_at_or_before(user_id: int, ts_cutoff: int):
    return await db_fetchone(
        "SELECT * FROM pnl_snapshots WHERE user_id=$1 AND ts <= $2 ORDER BY ts DESC LIMIT 1",
        (user_id, ts_cutoff)
    )

# ---------------------------- calculations/formatting ----------------------------
@dataclass
class AssetComputed:
    asset_id: int
    symbol: str
    name: str
    coingecko_id: str
    invested: float
    entry: float
    qty: float
    current: Optional[float]
    pnl_usd: Optional[float]
    pnl_pct: Optional[float]

def compute_asset(row, current_price: Optional[float]) -> AssetComputed:
    invested = float(row["invested_usd"])
    entry = float(row["entry_price"])
    qty_override = float(row.get("qty_override") or 0.0)
    qty = invested / entry if entry > 0 else qty_override

    if current_price is None:
        return AssetComputed(
            asset_id=int(row["id"]),
            symbol=str(row["symbol"]),
            name=str(row["name"] or ""),
            coingecko_id=str(row["coingecko_id"]),
            invested=invested,
            entry=entry,
            qty=qty,
            current=None,
            pnl_usd=None,
            pnl_pct=None,
        )

    current_value = qty * float(current_price)
    pnl_usd = current_value - invested
    pnl_pct = None if invested == 0 else (pnl_usd / invested * 100.0)

    return AssetComputed(
        asset_id=int(row["id"]),
        symbol=str(row["symbol"]),
        name=str(row["name"] or ""),
        coingecko_id=str(row["coingecko_id"]),
        invested=invested,
        entry=entry,
        qty=qty,
        current=float(current_price),
        pnl_usd=float(pnl_usd),
        pnl_pct=None if pnl_pct is None else float(pnl_pct),
    )
def fmt_levels(entry: float, pcts: List[int], kind: str) -> str:
    if entry <= 0 or not pcts:
        return "—"
    parts = []
    for p in sorted(set(pcts)):
        if kind == "RISK":
            price = entry * (1 - p / 100.0)
            parts.append(f"{fmt_usd(price)} (-{p}%)")
        else:
            price = entry * (1 + p / 100.0)
            parts.append(f"{fmt_usd(price)} (+{p}%)")
    return ", ".join(parts)

def asset_card(comp: AssetComputed, risk_pcts: List[int], tp_pcts: List[int]) -> str:
    title = f"🛠 {comp.symbol}" + (f" ({comp.name})" if comp.name else "")
    breakeven = comp.entry

    risk_line = fmt_levels(comp.entry, risk_pcts, "RISK")
    tp_line = fmt_levels(comp.entry, tp_pcts, "TP")

    if comp.current is None or comp.pnl_usd is None:
        cur_line = "Текущая:   —"
        pnl_line = "PNL:       —"
    else:
        cur_line = f"Текущая:   {fmt_price(comp.current)}"
        pct_text = "—" if comp.pnl_pct is None else sign_pct(comp.pnl_pct)
        pnl_line = f"{pnl_icon(comp.pnl_usd)} PNL:      {sign_money(comp.pnl_usd)} ({pct_text})"

    return "\n".join([
        title,
        f"📝 Вход:     {fmt_usd(comp.entry)}",
        f"🔒 Б/У:      {fmt_usd(breakeven)}",
        f"📉 Риск:     {risk_line}",
        f"📈 Профит:   {tp_line}",
        f"💵 Сумма:    {fmt_usd(comp.invested)}",
        f"🪙 Кол-во:   {fmt_qty(comp.qty)}",
        "",
        cur_line,
        pnl_line
    ])

async def build_summary_text(user_id: int) -> str:
    assets = await list_assets(user_id)
    if not assets:
        return (
            "📊 <b>Сводка портфеля</b>\n\n"
            "Активов пока нет.\n"
            "Нажми «➕ Добавить актив» и заведём первый."
        )

    ids = list({a["coingecko_id"] for a in assets})

    price_map: Dict[str, float] = {}
    try:
        price_map = await cg.simple_prices_usd(ids)
    except Exception as e:
        log.warning("Price fetch failed: %r", e)

    known = sum(1 for cid in ids if cid in price_map)
    total_assets = len(ids)

    computed: List[AssetComputed] = []
    total_invested = 0.0
    total_value = 0.0

    for a in assets:
        cp = price_map.get(a["coingecko_id"])
        comp = compute_asset(a, cp)
        computed.append(comp)

        total_invested += comp.invested
        if comp.current is not None:
            total_value += comp.qty * comp.current

    computed.sort(key=lambda x: (x.pnl_usd is None, -(x.pnl_usd or 0.0)))

    blocks: List[str] = []
    for comp in computed:
        alerts = await list_alerts_for_asset(comp.asset_id)
        risk_pcts = sorted({int(r["pct"]) for r in alerts if r["type"] == "RISK"})
        tp_pcts = sorted({int(r["pct"]) for r in alerts if r["type"] == "TP"})

        sym = escape(comp.symbol)
        qty_text = fmt_qty(comp.qty)

        if comp.current is None or comp.pnl_usd is None:
            line_top = f"• <b>{sym}</b> · PNL —"
        else:
            icon = pnl_icon(comp.pnl_usd)
            pct_text = "—" if comp.pnl_pct is None else sign_pct(comp.pnl_pct)
            line_top = f"• <b>{sym}</b> · {icon} {sign_money(comp.pnl_usd)} ({pct_text})"

        IND = "\u00A0\u00A0"  # 2 неразрывных пробела для красивого отступа

        line_invested = f"{IND}Вложено: {money_usd(comp.invested)}"
        line_qty = f"{IND}Кол-во монет: {qty_text}"
        line_alert = f"{IND}<b>{format_alert_line(risk_pcts, tp_pcts)}</b>"

        blocks.append("\n".join([line_top, line_invested, line_qty, line_alert]))

    footer_lines: List[str] = [
        f"Токены: {known}/{total_assets}",
        f"Вложено: {money_usd(total_invested)}",
    ]

    if known != total_assets:
        footer_lines.append("Текущая стоимость: —")
        footer_lines.append("<b>ОБЩИЙ PNL: —</b>")
    else:
        footer_lines.append(f"Текущая стоимость: {money_usd(total_value)}")
        total_pnl = total_value - total_invested
        total_pnl_pct = None if total_invested == 0 else (total_pnl / total_invested * 100.0)
        pct_text = "—" if total_pnl_pct is None else sign_pct(total_pnl_pct)
        footer_lines.append(
            f"<b>{pnl_icon(total_pnl)} ОБЩИЙ PNL: {sign_money(total_pnl)} ({pct_text})</b>"
        )

    return "📊 <b>Сводка портфеля</b>\n\n" + "\n\n".join(blocks) + "\n\n" + "\n".join(footer_lines)
# ---------------------------- FSM ----------------------------
class AddAssetFSM(StatesGroup):
    ticker = State()
    choose_coin = State()
    invested = State()
    entry = State()
    quantity = State()
    alerts = State()

class EditAssetFSM(StatesGroup):
    choose_asset = State()
    invested = State()
    entry = State()
    quantity = State()

# ---------------------------- keyboards for flows ----------------------------
def coin_choice_kb(coins: List[dict]) -> InlineKeyboardMarkup:
    kb = []
    for c in coins[:6]:
        cid = c.get("id")
        if not cid:
            continue
        name = c.get("name") or ""
        sym = (c.get("symbol") or "").upper()
        kb.append([InlineKeyboardButton(
            text=f"{sym} — {name}",
            callback_data=f"add:coin:{cid}"
        )])
    kb.append([InlineKeyboardButton(text="❌ Отмена", callback_data="flow:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def alerts_kb(selected: Set[str]) -> InlineKeyboardMarkup:
    # selected holds "RISK:5" "TP:10"
    rows = []
    r1 = []
    for p in RISK_LEVELS:
        key = f"RISK:{p}"
        mark = "✅ " if key in selected else ""
        r1.append(InlineKeyboardButton(text=f"{mark}📉 -{p}%", callback_data=f"add:alert:{key}"))
    rows.append(r1)

    r2 = []
    for p in TP_LEVELS:
        key = f"TP:{p}"
        mark = "✅ " if key in selected else ""
        r2.append(InlineKeyboardButton(text=f"{mark}📈 +{p}%", callback_data=f"add:alert:{key}"))
    rows.append(r2)

    rows.append([
        InlineKeyboardButton(text="🚫 Без алертов", callback_data="add:alert:none"),
        InlineKeyboardButton(text="💾 Готово", callback_data="add:alert:done"),
    ])
    rows.append([InlineKeyboardButton(text="❌ Отмена", callback_data="flow:cancel")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def assets_list_kb(assets_rows, prefix: str) -> InlineKeyboardMarkup:
    kb = []
    for a in assets_rows:
        kb.append([InlineKeyboardButton(
            text=f"{a['symbol']} — {fmt_usd(a['invested_usd'])} @ {fmt_usd(a['entry_price'])}",
            callback_data=f"{prefix}:asset:{a['id']}"
        )])
    kb.append([InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="nav:menu")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

# ---------------------------- router/handlers ----------------------------
router = Router()

@router.message(CommandStart())
async def on_start(m: Message):
    await upsert_user(m.from_user.id)
    await m.answer(
        "Здарова! Я бот-учёт активов: считаю PNL, показываю сводку и шлёпну алертом, если цена дошла до уровня.\n\n"
        "Выбирай действие в меню.",
        reply_markup=main_menu_kb()
    )

@router.message(F.text == "📊 Сводка")
async def on_summary(m: Message):
    await upsert_user(m.from_user.id)
    text = await build_summary_text(m.from_user.id)
    msg = await m.answer(text, reply_markup=summary_kb())
    await set_last_summary_message(m.from_user.id, m.chat.id, msg.message_id)

@router.callback_query(F.data == "summary:refresh")
async def on_summary_refresh(cb: CallbackQuery):
    await upsert_user(cb.from_user.id)
    text = await build_summary_text(cb.from_user.id)

    # Если текст уже такой же — просто отвечаем, без edit_text
    try:
        await cb.message.edit_text(text, reply_markup=summary_kb())
    except TelegramBadRequest as e:
        # Telegram ругается, если "ничего не поменялось"
        if "message is not modified" in str(e):
            return await cb.answer("Уже актуально")
        raise

    await cb.answer("Обновлено")

@router.callback_query(F.data == "nav:menu")
async def on_nav_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.answer("Меню:", reply_markup=main_menu_kb())
    await cb.answer()

@router.callback_query(F.data == "nav:add")
async def on_nav_add(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await state.set_state(AddAssetFSM.ticker)
    await cb.message.answer("Введи тикер/название монеты (пример: BTC, ETH, solana):")
    await cb.answer()

@router.message(F.text == "➕ Добавить актив")
async def on_add_asset_start(m: Message, state: FSMContext):
    await upsert_user(m.from_user.id)
    await state.clear()
    await state.set_state(AddAssetFSM.ticker)
    await m.answer("Введи тикер/название монеты (пример: BTC, ETH, solana):")

@router.message(AddAssetFSM.ticker)
async def on_add_ticker(m: Message, state: FSMContext):
    q = (m.text or "").strip()
    if not q or len(q) > 40:
        return await m.answer("Тикер слишком странный. Давай проще: BTC / ETH / SOL.")

    try:
        coins = await cg.search(q)
    except Exception as e:
        log.warning("CoinGecko search failed: %r", e)
        return await m.answer("CoinGecko не ответил. Попробуй ещё раз чуть позже.")

    if not coins:
        return await m.answer("Ничего не нашёл. Попробуй другой запрос (например: bitcoin).")

    q_up = q.upper()
    coins_sorted = sorted(coins, key=lambda c: (c.get("symbol") != q_up, c.get("name") or ""))
    await state.update_data(coins=coins_sorted[:10])
    await state.set_state(AddAssetFSM.choose_coin)
    await m.answer(
    "Выбери монету (у тикеров бывают совпадения):",
    reply_markup=coin_choice_kb(coins_sorted)
)

@router.callback_query(AddAssetFSM.choose_coin, F.data.startswith("add:coin:"))
async def on_add_choose_coin(cb: CallbackQuery, state: FSMContext):
    cid = cb.data.split("add:coin:", 1)[1].strip()
    data = await state.get_data()
    coins = data.get("coins", [])
    chosen = next((c for c in coins if c.get("id") == cid), None)

    if not chosen:
        await cb.answer("Не нашёл монету. Начни заново.")
        await state.clear()
        return

    await state.update_data(
        coingecko_id=chosen["id"],
        symbol=(chosen.get("symbol") or "").upper(),
        name=chosen.get("name") or ""
    )
    await state.set_state(AddAssetFSM.invested)
    await cb.message.answer("Введи сумму, на которую купил (в USD), например 1000:")
    await cb.answer()

@router.callback_query(F.data == "flow:cancel")
async def on_flow_cancel(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.answer("Ок, отменил.", reply_markup=main_menu_kb())
    await cb.answer()

@router.message(AddAssetFSM.invested)
async def on_add_invested(m: Message, state: FSMContext):
    v = safe_float(m.text or "")
    if v is None or v < 0:
        return await m.answer("Сумма не может быть отрицательной.")
    await state.update_data(invested=float(v))
    await state.set_state(AddAssetFSM.entry)
    await m.answer("Введи цену входа (USD), например 40000:")

@router.message(AddAssetFSM.entry)
async def on_add_entry(m: Message, state: FSMContext):
    v = safe_float(m.text or "")
    if v is None or v < 0:
        return await m.answer("Цена входа не может быть отрицательной.")

    entry = float(v)
    await state.update_data(entry=entry)

    if entry == 0:
        await state.set_state(AddAssetFSM.quantity)
        return await m.answer(
            "Это бесплатная позиция.\n"
            "Введи количество монет (например 123.4567):"
        )

    await state.update_data(selected_alerts=set(), qty_override=None)
    data = await state.get_data()
    sym = data.get("symbol", "")
    nm = data.get("name", "")
    invested = float(data["invested"])

    preview = "\n".join([
        f"Ок, добавляем: {sym} ({nm})",
        f"Сумма: {fmt_usd(invested)}",
        f"Цена входа: {fmt_usd(entry)}",
        "",
        "Выбери алерты (можно несколько) и нажми «💾 Готово»:"
    ])
    await state.set_state(AddAssetFSM.alerts)
    await m.answer(preview, reply_markup=alerts_kb(set()))

@router.message(AddAssetFSM.quantity)
async def on_add_quantity(m: Message, state: FSMContext):
    qty = safe_float(m.text or "")
    if qty is None or qty <= 0:
        return await m.answer("Количество должно быть больше нуля.")

    data = await state.get_data()
    sym = (data.get("symbol") or "").upper()
    nm = data.get("name") or ""
    coingecko_id = data.get("coingecko_id")
    invested = float(data.get("invested"))
    entry = float(data.get("entry"))
    qty_override = float(qty)

    await add_asset_row(
        m.from_user.id,
        sym,
        coingecko_id,
        nm,
        invested,
        entry,
        qty_override=qty_override,
    )
    await state.clear()
    await m.answer(
        "Готово ✅ Бесплатная позиция сохранена.\n"
        "Алерты по процентам для неё недоступны.",
        reply_markup=main_menu_kb()
    )

@router.callback_query(AddAssetFSM.alerts, F.data.startswith("add:alert:"))
async def on_add_alerts(cb: CallbackQuery, state: FSMContext):
    action = cb.data.split("add:alert:", 1)[1]
    data = await state.get_data()
    selected: Set[str] = set(data.get("selected_alerts", set()))

    if action == "none":
        selected = set()
        await state.update_data(selected_alerts=selected)
        await cb.message.edit_reply_markup(reply_markup=alerts_kb(selected))
        return await cb.answer("Без алертов")

    if action == "done":
        data = await state.get_data()
        sym = (data.get("symbol") or "").upper()
        nm = data.get("name") or ""
        coingecko_id = data.get("coingecko_id")
        invested = float(data.get("invested"))
        entry = float(data.get("entry"))

        if entry == 0:
            await cb.answer("Для бесплатной позиции алерты не сохраняются.")
            return

        asset_id = await add_asset_row(
            cb.from_user.id,
            sym,
            coingecko_id,
            nm,
            invested,
            entry,
            qty_override=None,
        )

        alert_rows: List[Tuple[str, int, float]] = []
        for s in sorted(selected):
            t, pct_str = s.split(":")
            pct = int(pct_str)
            target = entry * (1 - pct / 100.0) if t == "RISK" else entry * (1 + pct / 100.0)
            alert_rows.append((t, pct, float(target)))

        if alert_rows:
            await replace_alerts(asset_id, alert_rows)

        await state.clear()
        await cb.message.answer("Готово ✅ Актив добавлен.", reply_markup=main_menu_kb())
        return await cb.answer("Сохранено")
    # toggle
    allowed = {f"RISK:{p}" for p in RISK_LEVELS} | {f"TP:{p}" for p in TP_LEVELS}
    if action in allowed:
        if action in selected:
            selected.remove(action)
        else:
            selected.add(action)
        await state.update_data(selected_alerts=selected)
        await cb.message.edit_reply_markup(reply_markup=alerts_kb(selected))
        return await cb.answer("Ок")

    await cb.answer("Не понял")

# ------- delete -------
@router.message(F.text == "🗑 Удалить актив")
async def on_delete_menu(m: Message):
    assets = await list_assets(m.from_user.id)
    if not assets:
        return await m.answer("Активов пока нет — удалять нечего.", reply_markup=main_menu_kb())
    await m.answer("Выбери актив для удаления:", reply_markup=assets_list_kb(assets, "del"))

@router.callback_query(F.data == "nav:delete")
async def on_delete_menu_cb(cb: CallbackQuery):
    assets = await list_assets(cb.from_user.id)
    if not assets:
        await cb.message.answer("Активов пока нет — удалять нечего.", reply_markup=main_menu_kb())
        return await cb.answer()
    await cb.message.answer("Выбери актив для удаления:", reply_markup=assets_list_kb(assets, "del"))
    await cb.answer()

@router.callback_query(F.data.startswith("del:asset:"))
async def on_delete_asset(cb: CallbackQuery):
    asset_id = int(cb.data.split("del:asset:", 1)[1])
    a = await get_asset(cb.from_user.id, asset_id)
    if not a:
        return await cb.answer("Актив не найден")

    await delete_asset_row(cb.from_user.id, asset_id)
    await cb.message.answer(f"Удалил {a['symbol']} ✅", reply_markup=main_menu_kb())
    await cb.answer("Удалено")

# ------- edit -------
@router.message(F.text == "✏️ Редактировать список активов")
async def on_edit_menu(m: Message, state: FSMContext):
    assets = await list_assets(m.from_user.id)
    if not assets:
        return await m.answer("Активов пока нет — редактировать нечего.", reply_markup=main_menu_kb())
    await state.clear()
    await state.set_state(EditAssetFSM.choose_asset)
    await m.answer("Выбери актив для редактирования:", reply_markup=assets_list_kb(assets, "edit"))

@router.callback_query(F.data == "nav:edit")
async def on_edit_menu_cb(cb: CallbackQuery, state: FSMContext):
    assets = await list_assets(cb.from_user.id)
    if not assets:
        await cb.message.answer("Активов пока нет — редактировать нечего.", reply_markup=main_menu_kb())
        return await cb.answer()
    await state.clear()
    await state.set_state(EditAssetFSM.choose_asset)
    await cb.message.answer("Выбери актив для редактирования:", reply_markup=assets_list_kb(assets, "edit"))
    await cb.answer()

@router.callback_query(EditAssetFSM.choose_asset, F.data.startswith("edit:asset:"))
async def on_edit_choose(cb: CallbackQuery, state: FSMContext):
    asset_id = int(cb.data.split("edit:asset:", 1)[1])
    a = await get_asset(cb.from_user.id, asset_id)
    if not a:
        await cb.answer("Актив не найден")
        await state.clear()
        return

    await state.update_data(asset_id=asset_id)
    await state.set_state(EditAssetFSM.invested)
    await cb.message.answer(
        "\n".join([
            f"Редактируем {a['symbol']} ({a['name'] or ''})",
            f"Текущая сумма: {fmt_usd(a['invested_usd'])}",
            f"Текущая цена входа: {fmt_usd(a['entry_price'])}",
            "",
            "Введи новую сумму (USD):"
        ])
    )
    await cb.answer()

@router.message(EditAssetFSM.invested)
async def on_edit_invested(m: Message, state: FSMContext):
    v = safe_float(m.text or "")
    if v is None or v <= 0:
        return await m.answer("Введи положительное число.")
    await state.update_data(invested=float(v))
    await state.set_state(EditAssetFSM.entry)
    await m.answer("Введи новую цену входа (USD):")

@router.message(EditAssetFSM.entry)
async def on_edit_entry(m: Message, state: FSMContext):
    v = safe_float(m.text or "")
    if v is None or v < 0:
        return await m.answer("Цена не может быть отрицательной.")

    entry = float(v)
    data = await state.get_data()
    asset_id = int(data["asset_id"])
    invested = float(data["invested"])

    if entry == 0:
        await state.update_data(entry=entry)
        await state.set_state(EditAssetFSM.quantity)
        return await m.answer("Введи количество монет для позиции (например 12.34):")

    await update_asset_row(m.from_user.id, asset_id, invested, entry, qty_override=None)
    await recompute_alert_targets(asset_id, entry)
    await state.clear()
    await m.answer("Обновил ✅", reply_markup=main_menu_kb())

@router.message(EditAssetFSM.quantity)
async def on_edit_quantity(m: Message, state: FSMContext):
    qty = safe_float(m.text or "")
    if qty is None or qty <= 0:
        return await m.answer("Количество должно быть больше нуля.")

    data = await state.get_data()
    asset_id = int(data["asset_id"])
    invested = float(data["invested"])
    entry = float(data["entry"])

    await update_asset_row(m.from_user.id, asset_id, invested, entry, qty_override=float(qty))
    await recompute_alert_targets(asset_id, entry)
    await state.clear()
    await m.answer("Обновил ✅", reply_markup=main_menu_kb())

# ------- pnl periods -------
@router.message(F.text.in_(["📅 PNL за неделю", "🗓 PNL за месяц"]))
async def on_pnl_period(m: Message):
    await upsert_user(m.from_user.id)
    latest = await get_snapshot_latest(m.from_user.id)
    if not latest:
        return await m.answer(
            "Пока нет истории для недели/месяца.\n"
            "Я записываю снапшоты раз в час — чуть времени и будет статистика.",
            reply_markup=main_menu_kb()
        )

    days = 7 if m.text.startswith("📅") else 30
    cutoff = int(time.time()) - days * 24 * 3600
    then = await get_snapshot_at_or_before(m.from_user.id, cutoff)
    if not then:
        return await m.answer(
            f"Недостаточно данных, чтобы посчитать за {days} дней.\n"
            "Нужно, чтобы накопились снапшоты.",
            reply_markup=main_menu_kb()
        )

    now_pnl = float(latest["total_pnl_usd"])
    then_pnl = float(then["total_pnl_usd"])
    delta = now_pnl - then_pnl
    icon = pnl_icon(delta)

    await m.answer(
        "\n".join([
            f"{'📅' if days == 7 else '🗓'} PNL за {days} дней",
            f"{icon} Изменение PNL: {sign_money(delta)}",
            "",
            f"PNL тогда: {sign_money(then_pnl)}",
            f"PNL сейчас: {sign_money(now_pnl)}",
        ]),
        reply_markup=main_menu_kb()
    )

# ---------------------------- background loops ----------------------------
async def alerts_loop(bot: Bot):
    while True:
        try:
            rows = await pending_alerts_joined()
            if rows:
                ids = list({r["coingecko_id"] for r in rows})
                price_map = await cg.simple_prices_usd(ids)

                for r in rows:
                    current = price_map.get(r["coingecko_id"])
                    if current is None:
                        continue

                    t = r["type"]  # RISK/TP
                    target = float(r["target_price"])
                    hit = (float(current) <= target) if t == "RISK" else (float(current) >= target)
                    if not hit:
                        continue

                    invested = float(r["invested_usd"])
                    entry = float(r["entry_price"])
                    qty_override = float(r.get("qty_override") or 0.0)
                    qty = invested / entry if entry > 0 else qty_override
                    if qty == 0:
                        continue

                    pnl_usd = qty * float(current) - invested
                    pnl_pct = None if invested == 0 else (pnl_usd / invested * 100.0)
                    pct_text = "—" if pnl_pct is None else sign_pct(pnl_pct)

                    pct = int(r["pct"])
                    sym = str(r["symbol"] or "")

                    move_icon = "🔴" if t == "RISK" else "🟢"
                    move_text = f"Цена снизилась на {pct}%" if t == "RISK" else f"Цена увеличилась на {pct}%"

                    text = "\n".join([
                        f"<b>🔔 АЛЕРТ: {escape(sym)}</b>",
                        f"{move_icon} {move_text}",
                        f"Текущая цена: {fmt_price(float(current))}",
                        f"{pnl_icon(pnl_usd)} PNL сейчас: {sign_money(pnl_usd)} ({pct_text})",
                    ])

                    await bot.send_message(chat_id=int(r["user_id"]), text=text)
                    await mark_alert_triggered(int(r["alert_id"]))
        except Exception as e:
            log.exception("alerts_loop error: %r", e)

        await asyncio.sleep(PRICE_POLL_SECONDS)

async def snapshots_loop():
    while True:
        try:
            users = await all_users()
            for uid in users:
                assets = await list_assets(uid)
                if not assets:
                    continue

                ids = list({a["coingecko_id"] for a in assets})
                price_map = await cg.simple_prices_usd(ids)
                known = sum(1 for cid in ids if cid in price_map)
                if known != len(ids):
                    log.warning("Skip snapshot for uid=%s: prices coverage %d/%d", uid, known, len(ids))
                    continue

                total_invested = 0.0
                total_value = 0.0
                for a in assets:
                    invested = float(a["invested_usd"])
                    entry = float(a["entry_price"])
                    qty_override = float(a.get("qty_override") or 0.0)
                    qty = invested / entry if entry > 0 else qty_override
                    if qty == 0:
                        continue
                    total_invested += invested

                    cp = price_map.get(a["coingecko_id"])
                    if cp is not None:
                        total_value += qty * float(cp)

                await insert_snapshot(uid, total_value=total_value, total_invested=total_invested)
        except Exception as e:
            log.exception("snapshots_loop error: %r", e)

        await asyncio.sleep(SNAPSHOT_EVERY_SECONDS)

# ---------------------------- main ----------------------------
async def main():
    await init_db()
    log.info("CWD=%s", os.getcwd())
    log.info("DB_BACKEND=%s", DB_BACKEND)

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await bot.delete_webhook(drop_pending_updates=True)

    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    health_task = asyncio.create_task(run_health_server())
    alert_task = asyncio.create_task(alerts_loop(bot))
    snap_task = asyncio.create_task(snapshots_loop())

    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        for t in (health_task, alert_task, snap_task):
            t.cancel()
        await cg.close()
        if pg_pool is not None:
            await pg_pool.close()


if __name__ == "__main__":
    asyncio.run(main())