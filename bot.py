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
from aiogram.filters import CommandStart, Command
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

PRICE_POLL_SECONDS = int(os.getenv("PRICE_POLL_SECONDS", "180"))
SNAPSHOT_EVERY_SECONDS = int(os.getenv("SNAPSHOT_EVERY_SECONDS", "14400"))

if not BOT_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN. Put it into your .env (BOT_TOKEN=...)")

if DB_BACKEND == "postgres" and not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL (Neon). Set it in Render env.")

RISK_LEVELS = [5, 10, 25]
TP_LEVELS = [5, 10, 25]
ALERT_REARM_PCT = float(os.getenv("ALERT_REARM_PCT", "0.3"))
# 0.3% = небольшой запас, чтобы алерт не “дребезжал” туда-сюда вокруг target
VERSION = "1.3.0"

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
        self._min_interval_sec = float(os.getenv("COINGECKO_MIN_INTERVAL_SEC", "0.8"))
        # 0.8–1.0 сек — быстрее ответа, но всё ещё щадяще для free-tier

        # NEW: adaptive backoff (when CoinGecko returns 429)
        self._base_min_interval_sec = self._min_interval_sec
        self._penalty_until_ts = 0.0
        self._penalty_min_interval_sec = self._min_interval_sec
        self._penalty_ttl_sec = 0  # больше не увеличиваем TTL кэша в штрафе
        # stats
        self._stats_calls = 0
        self._stats_time = 0.0
        self._stats_429 = 0

        # NEW: serialize actual HTTP calls too (prevents parallel in-flight requests)
        self._net_lock = asyncio.Lock()

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
        # Понижаем агрессивно, но без раздувания TTL
        now = time.time()
        window = max(90.0, retry_after, 0.0)
        self._penalty_until_ts = max(self._penalty_until_ts, now + window)

        # min interval: чуть подрастить, но не выше 1.6s
        self._penalty_min_interval_sec = min(
            max(self._penalty_min_interval_sec * 1.3, self._min_interval_sec),
            1.6
        )

        # не трогаем TTL кэша
        self._penalty_ttl_sec = 0

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
        async with self._net_lock:
            url = f"{self.BASE}{path}"
            backoff = 1.0
            last_exc: Optional[BaseException] = None

            for attempt in range(1, tries + 1):
                try:
                    t0 = time.perf_counter()
                    s = await self.session()

                    await self._rate_limit_wait()

                    # network request (rate-limit already applied)
                    async with s.get(url, params=params) as r:
                        status = r.status
                        text = await r.text()
                        headers = dict(r.headers)

                    if status == 200:
                        try:
                            obj = json.loads(text) if text else {}
                        except Exception as e:
                            raise RuntimeError(f"CoinGecko bad JSON ({path}): {text[:200]}") from e

                        # stats
                        dur = time.perf_counter() - t0
                        self._stats_calls += 1
                        self._stats_time += dur
                        if self._stats_calls % 50 == 0:
                            avg = self._stats_time / max(1, self._stats_calls)
                            log.info("CG avg latency=%.3fs calls=%d 429=%d", avg, self._stats_calls, self._stats_429)

                        return obj

                    if status == 429:
                        self._stats_429 += 1
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
                    log.warning(
                        "CoinGecko network error on %s (attempt %d/%d): %r",
                        path, attempt, tries, e
                    )
                    await asyncio.sleep(backoff + random.random() * 0.25)
                    backoff = min(backoff * 2.0, 30.0)

                except Exception as e:
                    last_exc = e
                    log.warning(
                        "CoinGecko error on %s (attempt %d/%d): %r",
                        path, attempt, tries, e
                    )
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
        effective_ttl = ttl_sec  # не увеличиваем TTL в штрафе

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
INSTANCE_LOCK_KEY = int(os.getenv("INSTANCE_LOCK_KEY", "912345678901234567"))
instance_lock_conn: Optional[asyncpg.Connection] = None

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

async def acquire_instance_lock() -> bool:
    """
    Берём pg_try_advisory_lock на выделенном соединении.
    Если lock не взят — это значит, что другой инстанс уже работает.
    """
    global instance_lock_conn
    assert pg_pool is not None

    # если вдруг уже брали — считаем ок
    if instance_lock_conn is not None:
        return True

    conn = await pg_pool.acquire()
    try:
        row = await conn.fetchrow("SELECT pg_try_advisory_lock($1) AS ok", INSTANCE_LOCK_KEY)
        ok = bool(row["ok"])
        if ok:
            # ВАЖНО: не release() — держим соединение живым, иначе lock пропадёт
            instance_lock_conn = conn
            log.info("Instance lock acquired (key=%s)", INSTANCE_LOCK_KEY)
            return True
    finally:
        # lock не взяли — возвращаем соединение в пул
        if instance_lock_conn is None:
            await pg_pool.release(conn)

    log.warning("Instance lock NOT acquired (key=%s). Another instance is running.", INSTANCE_LOCK_KEY)
    return False


async def release_instance_lock():
    global instance_lock_conn
    if instance_lock_conn is None or pg_pool is None:
        return

    try:
        await instance_lock_conn.execute("SELECT pg_advisory_unlock($1)", INSTANCE_LOCK_KEY)
    except Exception:
        # даже если unlock упал, при закрытии соединения lock всё равно уйдёт
        pass

    try:
        await pg_pool.release(instance_lock_conn)
    except Exception:
        pass

    instance_lock_conn = None

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

async def list_assets_with_alerts(user_id: int) -> Tuple[List[dict], Dict[int, List[dict]]]:
    """
    Один запрос вместо двух:
    - забираем assets пользователя
    - и сразу приклеиваем к каждому asset его alerts (LEFT JOIN)
    Это экономит одну “поездку” (сетевую задержку) до Neon.
    """
    rows = await db_fetchall(
        """
        SELECT
          a.id AS asset_id,
          a.user_id,
          a.symbol,
          a.coingecko_id,
          a.name,
          a.invested_usd,
          a.entry_price,
          a.qty_override,
          a.created_at,

          al.type AS alert_type,
          al.pct  AS alert_pct

        FROM assets a
        LEFT JOIN alerts al ON al.asset_id = a.id
        WHERE a.user_id=$1
        ORDER BY a.id DESC
        """,
        (user_id,)
    )

    assets_by_id: Dict[int, dict] = {}
    alerts_by_asset: Dict[int, List[dict]] = {}

    for r in rows:
        aid = int(r["asset_id"])

        if aid not in assets_by_id:
            assets_by_id[aid] = {
                "id": aid,
                "user_id": int(r["user_id"]),
                "symbol": str(r["symbol"]),
                "coingecko_id": str(r["coingecko_id"]),
                "name": r.get("name") or "",
                "invested_usd": float(r["invested_usd"]),
                "entry_price": float(r["entry_price"]),
                "qty_override": r.get("qty_override"),
                "created_at": int(r["created_at"]),
            }

        at = r.get("alert_type")
        ap = r.get("alert_pct")
        if at is not None and ap is not None:
            alerts_by_asset.setdefault(aid, []).append({
                "type": str(at),
                "pct": int(ap),
            })

    assets = sorted(assets_by_id.values(), key=lambda x: x["id"], reverse=True)
    return assets, alerts_by_asset

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
          al.triggered, al.triggered_at,
          a.id AS asset_id, a.user_id, a.symbol, a.coingecko_id, a.name,
          a.invested_usd, a.entry_price, a.qty_override
        FROM alerts al
        JOIN assets a ON a.id = al.asset_id
        """
    )

async def mark_alert_triggered(alert_id: int):
    await db_exec(
        "UPDATE alerts SET triggered=1, triggered_at=$1 WHERE id=$2",
        (int(time.time()), alert_id)
    )

async def reset_alert_triggered(alert_id: int):
    await db_exec("UPDATE alerts SET triggered=0, triggered_at=NULL WHERE id=$1", (alert_id,))

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
    if qty_override > 0:
        qty = qty_override
    elif entry > 0 and invested > 0:
        qty = invested / entry
    else:
        qty = 0.0

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

    # базовая сумма для расчёта PNL:
    # - если invested > 0: классика (от вложений)
    # - если invested == 0 и entry > 0: считаем от стоимости по цене входа (qty*entry)
    base_invested = invested if invested > 0 else (qty * entry if entry > 0 else 0.0)

    pnl_usd = current_value - base_invested
    pnl_pct = None if base_invested == 0 else (pnl_usd / base_invested * 100.0)

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
    ts_text = time.strftime("%H:%M:%S", time.localtime())
    price_ttl = 180  # TTL кэша цен в simple_prices_usd
    assets, alerts_by_asset = await list_assets_with_alerts(user_id)
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
        alerts = alerts_by_asset.get(comp.asset_id, []) or []
        risk_pcts = sorted({int(r["pct"]) for r in alerts if r.get("type") == "RISK"})
        tp_pcts = sorted({int(r["pct"]) for r in alerts if r.get("type") == "TP"})

        sym = escape(comp.symbol)
        qty_text = fmt_qty(comp.qty)

        IND = "\u00A0\u00A0"  # 2 неразрывных пробела для красивого отступа

        # FREE позиции (invested=0): показываем стоимость и Δ от entry (цены получения)
        if comp.invested == 0:
            if comp.current is None:
                line_top = f"• <b>{sym}</b> · Стоимость —"
                line_mid = f"{IND}Δ от входа: —"
                line_base = f"{IND}База: —"
            else:
                current_value = comp.qty * float(comp.current)
                line_top = f"• <b>{sym}</b> · Стоимость {money_usd(current_value)}"

                if comp.entry > 0 and comp.qty > 0:
                    base_value = comp.qty * comp.entry
                    delta_usd = current_value - base_value
                    delta_pct = None if base_value == 0 else (delta_usd / base_value * 100.0)
                    pct_text = "—" if delta_pct is None else sign_pct(delta_pct)
                    line_mid = f"{IND}Δ от входа: {sign_money(delta_usd)} ({pct_text})"
                    line_base = f"{IND}База: {money_usd(base_value)}"
                else:
                    line_mid = f"{IND}Δ от входа: —"
                    line_base = f"{IND}База: —"

        # Обычные позиции: старый формат PNL от вложенной суммы
        else:
            if comp.current is None or comp.pnl_usd is None:
                line_top = f"• <b>{sym}</b> · PNL —"
            else:
                icon = pnl_icon(comp.pnl_usd)
                pct_text = "—" if comp.pnl_pct is None else sign_pct(comp.pnl_pct)
                line_top = f"• <b>{sym}</b> · {icon} {sign_money(comp.pnl_usd)} ({pct_text})"
            line_mid = f"{IND}Вложено: {money_usd(comp.invested)}"

        line_qty = f"{IND}Кол-во монет: {qty_text}"
        line_alert = f"{IND}<b>{format_alert_line(risk_pcts, tp_pcts)}</b>"

        rows_block = [line_top, line_mid]
        if comp.invested == 0:
            rows_block.append(line_base)
        rows_block.extend([line_qty, line_alert])

        blocks.append("\n".join(rows_block))
    footer_lines: List[str] = [
        ("⚠️ Цены: " if known != total_assets else "✅ Цены: ") + f"{known}/{total_assets}",
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

    footer_lines.append(f"Обновлено: {ts_text}, источник: CoinGecko, TTL: {price_ttl}s")

    return "📊 <b>Сводка портфеля</b>\n\n" + "\n\n".join(blocks) + "\n\n" + "\n".join(footer_lines)

# ---------------------------- FSM ----------------------------
class AddAssetFSM(StatesGroup):
    mode = State()
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

class EditAlertsFSM(StatesGroup):
    alerts = State()

# ---------------------------- keyboards for flows ----------------------------
def add_mode_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Обычная позиция", callback_data="add:mode:paid")],
        [InlineKeyboardButton(text="Бесплатная позиция", callback_data="add:mode:free")],
        [InlineKeyboardButton(text="Отмена", callback_data="flow:cancel")],
    ])

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

def assets_edit_list_kb(assets_rows) -> InlineKeyboardMarkup:
    kb = []
    for a in assets_rows:
        kb.append([
            InlineKeyboardButton(
                text=f"✏️ {a['symbol']} — {fmt_usd(a['invested_usd'])} @ {fmt_usd(a['entry_price'])}",
                callback_data=f"edit:asset:{a['id']}"
            ),
            InlineKeyboardButton(
                text="🔔",
                callback_data=f"edit:alerts:{a['id']}"
            ),
            InlineKeyboardButton(
                text="🗑",
                callback_data=f"edit:delete:{a['id']}"
            )
        ])
    kb.append([InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="nav:menu")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def edit_actions_kb(asset_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 Удалить этот актив", callback_data=f"edit:delete:{asset_id}")],
        [InlineKeyboardButton(text="⬅️ К списку", callback_data="nav:edit"),
         InlineKeyboardButton(text="⬅️ В меню", callback_data="nav:menu")]
    ])

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

@router.message(Command("help"))
async def on_help(m: Message):
    await m.answer(
        "Что умею:\n"
        "• Сводка портфеля, PNL, алерты по уровням.\n"
        "• Free-позиции: задаёшь цену входа и количество — PNL считается от базы entry*qty.\n"
        "• Алерты 'решёткой': при достижении уровня цель сдвигается ещё на тот же % от текущей цены.\n\n"
        "Как работают алерты-решётка:\n"
        "— Дошли до +10%: пришло уведомление, новая цель = текущая цена * 1.10.\n"
        "— Дошли до -10%: пришло уведомление, новая цель = текущая цена * 0.90.\n"
        "Так продолжается дальше по тренду."
    )

@router.message(Command("about"))
async def on_about(m: Message):
    await m.answer(
        f"Версия бота: {VERSION}\n"
        "Источник цен: CoinGecko (free tier)\n"
        "Автор: you\n"
        "Репо: https://github.com/your/repo"
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

    # один ответ на callback — сразу закрываем “спиннер” (важно для телефона)
    await cb.answer("Обновляю...")

    t0 = time.perf_counter()
    text = await build_summary_text(cb.from_user.id)
    log.info("summary_refresh uid=%s took %.3fs", cb.from_user.id, time.perf_counter() - t0)

    try:
        await cb.message.edit_text(text, reply_markup=summary_kb())
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            await cb.answer("Актуально")
            return
        raise

@router.callback_query(F.data == "nav:menu")
async def on_nav_menu(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.answer("Меню:", reply_markup=main_menu_kb())
    await cb.answer()

@router.callback_query(F.data == "nav:add")
async def on_nav_add(cb: CallbackQuery, state: FSMContext):
    await upsert_user(cb.from_user.id)
    await state.clear()
    await state.set_state(AddAssetFSM.mode)
    await cb.message.answer("Выбери тип позиции:", reply_markup=add_mode_kb())
    await cb.answer()

@router.message(F.text == "➕ Добавить актив")
async def on_add_asset_start(m: Message, state: FSMContext):
    await upsert_user(m.from_user.id)
    await state.clear()
    await state.set_state(AddAssetFSM.mode)
    await m.answer("Выбери тип позиции:", reply_markup=add_mode_kb())

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

    mode = (data.get("add_mode") or "paid").strip().lower()

    await state.update_data(
        coingecko_id=chosen["id"],
        symbol=(chosen.get("symbol") or "").upper(),
        name=chosen.get("name") or ""
    )

    if mode == "free":
        await state.update_data(invested=0.0)
        await state.set_state(AddAssetFSM.entry)
        kb_info = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="ℹ️ Как считать free-позиции", callback_data="info:free")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="flow:cancel")]
        ])
        await cb.message.answer(
            "Бесплатная позиция.\n"
            "Введи цену, по которой досталась монета (USD). Нужно > 0, чтобы считать PNL и алерты:",
            reply_markup=kb_info
        )
        await cb.answer()
        return

    await state.set_state(AddAssetFSM.invested)
    await cb.message.answer("Введи сумму, на которую купил (в USD). Например 1000:")
    await cb.answer()

@router.callback_query(F.data == "flow:cancel")
async def on_flow_cancel(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await cb.message.answer("Ок, отменил.", reply_markup=main_menu_kb())
    await cb.answer()

@router.callback_query(AddAssetFSM.mode, F.data.startswith("add:mode:"))
async def on_add_mode(cb: CallbackQuery, state: FSMContext):
    mode = cb.data.split("add:mode:", 1)[1].strip().lower()
    if mode not in ("paid", "free"):
        return await cb.answer("Не понял")

    await state.update_data(add_mode=mode)
    await state.set_state(AddAssetFSM.ticker)

    # Убираем клавиатуру, чтобы не нажимали второй раз и не было “вечной загрузки” в клиенте
    try:
        await cb.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass

    await cb.message.answer("Введи тикер/название монеты (пример: BTC, ETH, SOL):")
    await cb.answer()

@router.callback_query(F.data == "info:free")
async def on_info_free(cb: CallbackQuery):
    await cb.answer()  # закрыть спиннер
    await cb.message.answer(
        "Как считать free-позиции:\n"
        "1) Укажи цену входа (>0) — по ней считаются база и алерты.\n"
        "2) Укажи количество монет — по нему считается стоимость и PNL.\n"
        "PNL идёт от базы (entry * qty), даже если вложено = 0."
    )

@router.callback_query(F.data.startswith("edit:alerts:"))
async def on_edit_alerts_start(cb: CallbackQuery, state: FSMContext):
    try:
        asset_id = int(cb.data.split("edit:alerts:", 1)[1])
    except Exception:
        return await cb.answer("Некорректный id")

    a = await get_asset(cb.from_user.id, asset_id)
    if not a:
        return await cb.answer("Актив не найден")

    # соберём выбранные алерты
    rows = await list_alerts_for_asset(asset_id)
    selected: Set[str] = set()
    for r in rows:
        t = str(r["type"])
        pct = int(r["pct"])
        selected.add(f"{t}:{pct}")

    await state.clear()
    await state.update_data(
        asset_id=asset_id,
        entry=float(a["entry_price"]),
        selected_alerts=selected
    )
    await state.set_state(EditAlertsFSM.alerts)

    sym = a["symbol"]
    entry = float(a["entry_price"])
    msg = "\n".join([
        f"Редактируем алерты для {sym}",
        f"Цена входа: {fmt_usd(entry)}",
        "",
        "Отметь уровни и нажми «💾 Готово»"
    ])
    await cb.message.answer(msg, reply_markup=alerts_kb(selected))
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
    if v is None or v <= 0:
        return await m.answer("Цена входа должна быть больше 0.")

    entry = float(v)
    data = await state.get_data()
    invested = float(data.get("invested", 0.0))

    # для free-позиций (invested=0) цена входа обязана быть >0 — уже проверили выше
    await state.update_data(entry=entry)

    # Если сумму/цену нельзя использовать для auto-qty — вводим количество вручную
    if invested == 0:
        await state.set_state(AddAssetFSM.quantity)
        return await m.answer(
            "Введи количество монет (например 123.4567):\n"
            "PNL и алерты будут считаться от этой цены входа."
        )

    await state.update_data(selected_alerts=set(), qty_override=None)

    sym = data.get("symbol", "")
    nm = data.get("name", "")

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

    await state.update_data(qty_override=float(qty))

    data = await state.get_data()
    sym = (data.get("symbol") or "").upper()
    nm = data.get("name") or ""
    coingecko_id = data.get("coingecko_id")
    invested = float(data.get("invested", 0.0))
    entry = float(data.get("entry", 0.0))
    qty_override = float(qty)

    await state.update_data(selected_alerts=set())

    note = "" if entry > 0 else "\n⚠️ Цена входа = 0, % алерты и PNL не будут посчитаны."

    preview = "\n".join([
        f"Ок, добавляем: {sym} ({nm})",
        f"Сумма: {fmt_usd(invested)}",
        f"Цена входа: {fmt_usd(entry)}",
        f"Количество: {fmt_qty(qty_override)}",
        "",
        "Выбери алерты (можно несколько) и нажми «💾 Готово»:"
    ])

    await state.set_state(AddAssetFSM.alerts)
    await m.answer(preview + note, reply_markup=alerts_kb(set()))

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
        invested = float(data.get("invested", 0.0))
        entry = float(data.get("entry", 0.0))

        qo = data.get("qty_override")
        qty_override: Optional[float] = None
        if qo is not None:
            try:
                qty_override = float(qo)
            except Exception:
                qty_override = None
            if qty_override is not None and qty_override <= 0:
                qty_override = None

        asset_id = await add_asset_row(
            cb.from_user.id,
            sym,
            coingecko_id,
            nm,
            invested,
            entry,
            qty_override=qty_override,
        )

        alert_rows: List[Tuple[str, int, float]] = []
        if entry > 0:
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

@router.callback_query(EditAlertsFSM.alerts, F.data.startswith("add:alert:"))
async def on_edit_alerts(cb: CallbackQuery, state: FSMContext):
    action = cb.data.split("add:alert:", 1)[1]
    data = await state.get_data()
    selected: Set[str] = set(data.get("selected_alerts", set()))
    asset_id = int(data.get("asset_id"))
    entry = float(data.get("entry", 0.0))

    if action == "none":
        selected = set()
        await state.update_data(selected_alerts=selected)
        await cb.message.edit_reply_markup(reply_markup=alerts_kb(selected))
        return await cb.answer("Без алертов")

    if action == "done":
        if entry <= 0:
            await state.clear()
            await cb.message.answer("Цена входа = 0, алерты не сохранены.", reply_markup=main_menu_kb())
            return await cb.answer("Нет цены входа")

        alert_rows: List[Tuple[str, int, float]] = []
        for s in sorted(selected):
            t, pct_str = s.split(":")
            pct = int(pct_str)
            target = entry * (1 - pct / 100.0) if t == "RISK" else entry * (1 + pct / 100.0)
            alert_rows.append((t, pct, float(target)))

        await replace_alerts(asset_id, alert_rows)
        await state.clear()
        await cb.message.answer("Алерты обновлены ✅", reply_markup=main_menu_kb())
        return await cb.answer("Сохранено")

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
    await m.answer(
        "Выбери актив:\n"
        "✏️ — редактировать, 🗑 — удалить",
        reply_markup=assets_edit_list_kb(assets)
    )

@router.callback_query(F.data == "nav:edit")
async def on_edit_menu_cb(cb: CallbackQuery, state: FSMContext):
    assets = await list_assets(cb.from_user.id)
    if not assets:
        await cb.message.answer("Активов пока нет — редактировать нечего.", reply_markup=main_menu_kb())
        return await cb.answer()
    await state.clear()
    await state.set_state(EditAssetFSM.choose_asset)
    await cb.message.answer(
        "Выбери актив:\n"
        "✏️ — редактировать, 🗑 — удалить",
        reply_markup=assets_edit_list_kb(assets)
    )
    await cb.answer()

@router.callback_query(F.data.startswith("edit:delete:"))
async def on_edit_delete_asset(cb: CallbackQuery, state: FSMContext):
    try:
        asset_id = int(cb.data.split("edit:delete:", 1)[1])
    except Exception:
        return await cb.answer("Некорректный id")

    a = await get_asset(cb.from_user.id, asset_id)
    if not a:
        return await cb.answer("Актив не найден")

    await delete_asset_row(cb.from_user.id, asset_id)

    assets = await list_assets(cb.from_user.id)
    if not assets:
        await state.clear()
        await cb.message.answer(f"Удалил {a['symbol']} ✅\nАктивов больше нет.", reply_markup=main_menu_kb())
        await cb.answer("Удалено")
        return

    await state.clear()
    await state.set_state(EditAssetFSM.choose_asset)
    await cb.message.answer(
        f"Удалил {a['symbol']} ✅\n\nВыбери следующий актив:",
        reply_markup=assets_edit_list_kb(assets)
    )
    await cb.answer("Удалено")

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
            "Введи новую сумму (USD). Можно 0 для бесплатной позиции:"
        ]),
        reply_markup=edit_actions_kb(asset_id)
    )
    await cb.answer()

@router.message(EditAssetFSM.invested)
async def on_edit_invested(m: Message, state: FSMContext):
    v = safe_float(m.text or "")
    if v is None or v < 0:
        return await m.answer("Сумма не может быть отрицательной. Можно 0 для бесплатной позиции.")
    await state.update_data(invested=float(v))
    await state.set_state(EditAssetFSM.entry)
    await m.answer("Введи новую цену входа (USD). Можно 0, если хочешь ввести количество вручную:")

@router.message(EditAssetFSM.entry)
async def on_edit_entry(m: Message, state: FSMContext):
    v = safe_float(m.text or "")
    if v is None or v < 0:
        return await m.answer("Цена не может быть отрицательной.")

    entry = float(v)
    data = await state.get_data()
    asset_id = int(data["asset_id"])
    invested = float(data.get("invested", 0.0))

    await state.update_data(entry=entry)

    # Если qty нельзя адекватно посчитать как invested/entry — просим количество
    if invested == 0 or entry == 0:
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
    # rearm_frac не нужен в новой логике, но оставим переменную
    rearm_frac = max(0.0, ALERT_REARM_PCT) / 100.0

    while True:
        try:
            rows = await pending_alerts_joined()
            if rows:
                ids = list({r["coingecko_id"] for r in rows if r.get("coingecko_id")})
                price_map = await cg.simple_prices_usd(ids)

                for r in rows:
                    cid = r.get("coingecko_id")
                    current = price_map.get(cid)
                    if current is None:
                        continue

                    cur = float(current)
                    t = str(r.get("type") or "")
                    target = float(r["target_price"])
                    pct = int(r["pct"])
                    alert_id = int(r["alert_id"])

                    hit = (cur <= target) if t == "RISK" else (cur >= target)

                    if hit:
                        invested = float(r["invested_usd"])
                        entry = float(r["entry_price"])
                        qty_override = float(r.get("qty_override") or 0.0)

                        if qty_override > 0:
                            qty = qty_override
                        elif entry > 0 and invested > 0:
                            qty = invested / entry
                        else:
                            qty = 0.0
                        if qty == 0:
                            # нет количества — нечего считать/слать
                            continue

                        # PNL считаем:
                        # если invested>0 — от вложений
                        # если invested==0 и entry>0 — от базы qty*entry
                        base_invested = invested if invested > 0 else (qty * entry if entry > 0 else 0.0)
                        pnl_usd = qty * cur - base_invested
                        pnl_pct = None if base_invested == 0 else (pnl_usd / base_invested * 100.0)
                        pct_text = "—" if pnl_pct is None else sign_pct(pnl_pct)

                        sym = str(r["symbol"] or "")
                        move_icon = "🔴" if t == "RISK" else "🟢"
                        move_text = f"Цена снизилась на {pct}%" if t == "RISK" else f"Цена увеличилась на {pct}%"

                        text = "\n".join([
                            f"<b>🔔 АЛЕРТ: {escape(sym)}</b>",
                            f"{move_icon} {move_text}",
                            f"Текущая цена: {fmt_price(cur)}",
                            f"{pnl_icon(pnl_usd)} PNL сейчас: {sign_money(pnl_usd)} ({pct_text})",
                        ])

                        await bot.send_message(chat_id=int(r["user_id"]), text=text)

                        # Сдвигаем цель дальше на тот же процент от текущей цены (grid)
                        if t == "RISK":
                            new_target = cur * (1 - pct / 100.0)
                        else:
                            new_target = cur * (1 + pct / 100.0)

                        await db_exec(
                            "UPDATE alerts SET target_price=$1, triggered=0, triggered_at=NULL WHERE id=$2",
                            (float(new_target), alert_id)
                        )

                    else:
                        # В новой логике triggered не используем: алерт всегда «вооружён»
                        pass

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
                    if qty_override > 0:
                        qty = qty_override
                    elif entry > 0:
                        qty = invested / entry
                    else:
                        qty = 0.0
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

    health_task = asyncio.create_task(run_health_server())

    got_lock = await acquire_instance_lock()
    if not got_lock:
        log.error("Another instance is running. Not starting polling/loops.")

        # Мы тут не будем работать как бот — значит можно закрыть лишние ресурсы
        try:
            await cg.close()
        finally:
            if pg_pool is not None:
                await pg_pool.close()

        # держим только health, чтобы Render не считал сервис мёртвым
        await health_task
        return

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    await bot.delete_webhook(drop_pending_updates=True)

    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    alert_task = asyncio.create_task(alerts_loop(bot))
    snap_task = asyncio.create_task(snapshots_loop())

    tasks = (health_task, alert_task, snap_task)

    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        for t in tasks:
            t.cancel()

        # "чисто" дождаться отмены тасков, без спама предупреждениями
        await asyncio.gather(*tasks, return_exceptions=True)

        await release_instance_lock()
        await cg.close()

        # (опционально, но полезно) закрыть HTTP-сессию бота
        try:
            await bot.session.close()
        except Exception:
            pass

        if pg_pool is not None:
            await pg_pool.close()


if __name__ == "__main__":
    asyncio.run(main())