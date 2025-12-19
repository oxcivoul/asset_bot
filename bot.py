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
import aiosqlite

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
DB_PATH = os.getenv("DB_PATH", "bot.db").strip()
PRICE_POLL_SECONDS = int(os.getenv("PRICE_POLL_SECONDS", "90"))
SNAPSHOT_EVERY_SECONDS = int(os.getenv("SNAPSHOT_EVERY_SECONDS", "3600"))

if not BOT_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN. Put it into your .env (BOT_TOKEN=...)")

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

def position_size_icon(invested_usd: float) -> str:
    # Иконка "размера позиции" по сумме вложений. Пороги можешь менять.
    x = abs(invested_usd)
    if x < 100:
        return "🟢"
    if x < 1_000:
        return "🟡"
    if x < 10_000:
        return "🟠"
    return "🔴"

def format_alert_line(risk_pcts: List[int], tp_pcts: List[int]) -> str:
    r = set(int(x) for x in (risk_pcts or []))
    t = set(int(x) for x in (tp_pcts or []))

    both = sorted(r & t)
    only_r = sorted(r - t)
    only_t = sorted(t - r)

    parts: List[str] = []
    parts += [f"-{p}%" for p in only_r]
    parts += [f"+-{p}%" for p in both]   # если выбраны и Risk и TP на один %
    parts += [f"+{p}%" for p in only_t]

    return "АЛЕРТ: " + (" ".join(parts) if parts else "❌")

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
        self._price_cache: Dict[str, Tuple[float, Dict[str, float]]] = {}
        self._lock = asyncio.Lock()

        self._api_key = os.getenv("COINGECKO_API_KEY", "").strip()
        self._headers = {
            "User-Agent": "asset-accountant-bot/1.0 (+https://github.com/your/repo)",
            "Accept": "application/json",
        }
        # Если у тебя появится ключ CoinGecko (demo/pro), он просто начнёт использоваться.
        # Лишние заголовки CoinGecko обычно игнорит, зато не ломают запросы.
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

    async def _get_json(self, path: str, params: Dict[str, str], *, tries: int = 5) -> dict:
        url = f"{self.BASE}{path}"
        backoff = 1.0
        last_exc: Optional[BaseException] = None

        for attempt in range(1, tries + 1):
            try:
                s = await self.session()

                # держим lock только на сетевой запрос, а не на backoff sleep
                async with self._lock:
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
    async def search(self, query: str) -> List[dict]:
        data = await self._get_json("/search", {"query": query})
        coins = data.get("coins", []) or []
        out = []
        for c in coins:
            out.append({
                "id": c.get("id"),
                "name": c.get("name"),
                "symbol": (c.get("symbol") or "").upper(),
            })
        return out

    async def simple_prices_usd(self, ids: List[str], ttl_sec: int = 180) -> Dict[str, float]:
        ids = [i for i in ids if i]
        if not ids:
            return {}

        # cache key
        key = ",".join(sorted(set(ids)))
        now = time.time()
        if key in self._price_cache:
            ts, cached = self._price_cache[key]
            if now - ts <= ttl_sec:
                return cached

        # CoinGecko любит ограничивать размер ids; чанкнем на всякий случай
        uniq = sorted(set(ids))
        out: Dict[str, float] = {}

        CHUNK = 100
        for i in range(0, len(uniq), CHUNK):
            chunk = uniq[i:i + CHUNK]
            data = await self._get_json("/simple/price", {"ids": ",".join(chunk), "vs_currencies": "usd"})
            for cid, row in (data or {}).items():
                try:
                    out[cid] = float(row["usd"])
                except Exception:
                    continue

        self._price_cache[key] = (now, out)
        return out

cg = CoinGeckoClient()

# ---------------------------- DB ----------------------------
SCHEMA_SQL = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS users (
  user_id INTEGER PRIMARY KEY,
  currency TEXT NOT NULL DEFAULT 'USD',
  last_summary_chat_id INTEGER,
  last_summary_message_id INTEGER
);

CREATE TABLE IF NOT EXISTS assets (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  symbol TEXT NOT NULL,
  coingecko_id TEXT NOT NULL,
  name TEXT,
  invested_usd REAL NOT NULL,
  entry_price REAL NOT NULL,
  created_at INTEGER NOT NULL,
  FOREIGN KEY(user_id) REFERENCES users(user_id)
);

CREATE INDEX IF NOT EXISTS idx_assets_user ON assets(user_id);
CREATE INDEX IF NOT EXISTS idx_assets_cgid ON assets(coingecko_id);

CREATE TABLE IF NOT EXISTS alerts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  asset_id INTEGER NOT NULL,
  type TEXT NOT NULL,               -- 'RISK' or 'TP'
  pct INTEGER NOT NULL,             -- 5/10/25
  target_price REAL NOT NULL,
  triggered INTEGER NOT NULL DEFAULT 0,
  triggered_at INTEGER,
  FOREIGN KEY(asset_id) REFERENCES assets(id)
);

CREATE INDEX IF NOT EXISTS idx_alerts_asset ON alerts(asset_id);
CREATE INDEX IF NOT EXISTS idx_alerts_triggered ON alerts(triggered);

CREATE TABLE IF NOT EXISTS pnl_snapshots (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  user_id INTEGER NOT NULL,
  ts INTEGER NOT NULL,
  total_value_usd REAL NOT NULL,
  total_invested_usd REAL NOT NULL,
  total_pnl_usd REAL NOT NULL,
  FOREIGN KEY(user_id) REFERENCES users(user_id)
);

CREATE INDEX IF NOT EXISTS idx_snap_user_ts ON pnl_snapshots(user_id, ts);
"""

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(SCHEMA_SQL)
        await db.commit()

async def db_exec(sql: str, params: tuple = ()):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(sql, params)
        await db.commit()

async def db_fetchone(sql: str, params: tuple = ()):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql, params) as cur:
            return await cur.fetchone()

async def db_fetchall(sql: str, params: tuple = ()):
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(sql, params) as cur:
            return await cur.fetchall()

async def upsert_user(user_id: int):
    await db_exec(
        "INSERT INTO users(user_id) VALUES (?) ON CONFLICT(user_id) DO NOTHING",
        (user_id,)
    )

async def set_last_summary_message(user_id: int, chat_id: int, message_id: int):
    await db_exec(
        "UPDATE users SET last_summary_chat_id=?, last_summary_message_id=? WHERE user_id=?",
        (chat_id, message_id, user_id)
    )

async def list_assets(user_id: int):
    return await db_fetchall(
        "SELECT * FROM assets WHERE user_id=? ORDER BY id DESC",
        (user_id,)
    )

async def get_asset(user_id: int, asset_id: int):
    return await db_fetchone(
        "SELECT * FROM assets WHERE user_id=? AND id=?",
        (user_id, asset_id)
    )

async def add_asset_row(user_id: int, symbol: str, coingecko_id: str, name: str,
                        invested_usd: float, entry_price: float) -> int:
    ts = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            """
            INSERT INTO assets(user_id, symbol, coingecko_id, name, invested_usd, entry_price, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (user_id, symbol.upper(), coingecko_id, name, invested_usd, entry_price, ts)
        )
        await db.commit()
        return int(cur.lastrowid)

async def update_asset_row(user_id: int, asset_id: int, invested_usd: float, entry_price: float):
    await db_exec(
        "UPDATE assets SET invested_usd=?, entry_price=? WHERE user_id=? AND id=?",
        (invested_usd, entry_price, user_id, asset_id)
    )

async def delete_asset_row(user_id: int, asset_id: int):
    await db_exec("DELETE FROM alerts WHERE asset_id=?", (asset_id,))
    await db_exec("DELETE FROM assets WHERE user_id=? AND id=?", (user_id, asset_id))

async def replace_alerts(asset_id: int, alerts: List[Tuple[str, int, float]]):
    await db_exec("DELETE FROM alerts WHERE asset_id=?", (asset_id,))
    async with aiosqlite.connect(DB_PATH) as db:
        for t, pct, target in alerts:
            await db.execute(
                "INSERT INTO alerts(asset_id, type, pct, target_price) VALUES (?, ?, ?, ?)",
                (asset_id, t, pct, target)
            )
        await db.commit()

async def list_alerts_for_asset(asset_id: int):
    return await db_fetchall("SELECT * FROM alerts WHERE asset_id=?", (asset_id,))

async def pending_alerts_joined():
    return await db_fetchall(
        """
        SELECT
          al.id AS alert_id, al.type, al.pct, al.target_price,
          a.id AS asset_id, a.user_id, a.symbol, a.coingecko_id, a.name, a.invested_usd, a.entry_price
        FROM alerts al
        JOIN assets a ON a.id = al.asset_id
        WHERE al.triggered = 0
        """
    )

async def mark_alert_triggered(alert_id: int):
    await db_exec(
        "UPDATE alerts SET triggered=1, triggered_at=? WHERE id=?",
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
        VALUES (?, ?, ?, ?, ?)
        """,
        (user_id, int(time.time()), total_value, total_invested, pnl)
    )

async def get_snapshot_latest(user_id: int):
    return await db_fetchone(
        "SELECT * FROM pnl_snapshots WHERE user_id=? ORDER BY ts DESC LIMIT 1",
        (user_id,)
    )

async def get_snapshot_at_or_before(user_id: int, ts_cutoff: int):
    return await db_fetchone(
        "SELECT * FROM pnl_snapshots WHERE user_id=? AND ts <= ? ORDER BY ts DESC LIMIT 1",
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
    qty = invested / entry if entry > 0 else 0.0
    if current_price is None:
        return AssetComputed(
            asset_id=int(row["id"]),
            symbol=str(row["symbol"]),
            name=str(row["name"] or ""),
            coingecko_id=str(row["coingecko_id"]),
            invested=invested, entry=entry, qty=qty,
            current=None, pnl_usd=None, pnl_pct=None
        )
    current_value = qty * float(current_price)
    pnl_usd = current_value - invested
    pnl_pct = (pnl_usd / invested * 100.0) if invested > 0 else 0.0
    return AssetComputed(
        asset_id=int(row["id"]),
        symbol=str(row["symbol"]),
        name=str(row["name"] or ""),
        coingecko_id=str(row["coingecko_id"]),
        invested=invested, entry=entry, qty=qty,
        current=float(current_price), pnl_usd=float(pnl_usd), pnl_pct=float(pnl_pct)
    )

def fmt_levels(entry: float, pcts: List[int], kind: str) -> str:
    # kind: "RISK" or "TP"
    if not pcts:
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

    if comp.current is None or comp.pnl_usd is None or comp.pnl_pct is None:
        cur_line = "Текущая:   —"
        pnl_line = "PNL:       —"
        icon_line = "📉/📈"
    else:
        icon_line = pnl_icon(comp.pnl_usd)
        cur_line = f"Текущая:   {fmt_usd(comp.current)}"
        pnl_line = f"{icon_line} PNL:      {sign_money(comp.pnl_usd)}  ({sign_pct(comp.pnl_pct)})"

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

    # Цены тянем по уникальным coingecko_id, и "Токены X/Y" тоже считаем по уникальным токенам
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

    # сортировка: сначала известные с лучшим pnl, потом неизвестные
    computed.sort(key=lambda x: (x.pnl_usd is None, -(x.pnl_usd or 0.0)))

    blocks: List[str] = []
    for comp in computed:
        alerts = await list_alerts_for_asset(comp.asset_id)
        risk_pcts = sorted({int(r["pct"]) for r in alerts if r["type"] == "RISK"})
        tp_pcts = sorted({int(r["pct"]) for r in alerts if r["type"] == "TP"})

        sym = escape(comp.symbol)
        qty_text = fmt_qty(comp.qty)
        size_icon = position_size_icon(comp.invested)

        if comp.current is None or comp.pnl_usd is None or comp.pnl_pct is None:
            line_top = f"• <b>{sym}</b> · PNL — {size_icon}"
        else:
            icon = pnl_icon(comp.pnl_usd)
            line_top = f"• <b>{sym}</b> · {icon} {sign_money(comp.pnl_usd)} ({sign_pct(comp.pnl_pct)}) {size_icon}"

        line_qty = f"Кол-во монет: {qty_text}"
        line_alert = format_alert_line(risk_pcts, tp_pcts)

        blocks.append("\n".join([line_top, line_qty, line_alert]))

    footer_lines: List[str] = [
        f"Токены {known}/{total_assets}",
        f"Вложено: {money_usd(total_invested)}",
    ]

    # Если не по всем токенам есть цены — не показываем итоги, чтобы не врать
    if known != total_assets:
        footer_lines.append("Текущая стоимость: —")
        footer_lines.append("<b>ОБЩИЙ PNL: —</b>")
    else:
        footer_lines.append(f"Текущая стоимость: {money_usd(total_value)}")
        total_pnl = total_value - total_invested
        total_pnl_pct = (total_pnl / total_invested * 100.0) if total_invested > 0 else 0.0
        footer_lines.append(
            f"<b>{pnl_icon(total_pnl)} ОБЩИЙ PNL: {sign_money(total_pnl)} ({sign_pct(total_pnl_pct)})</b>"
        )

    return "📊 <b>Сводка портфеля</b>\n\n" + "\n\n".join(blocks) + "\n\n" + "\n".join(footer_lines)
# ---------------------------- FSM ----------------------------
class AddAssetFSM(StatesGroup):
    ticker = State()
    choose_coin = State()
    invested = State()
    entry = State()
    alerts = State()

class EditAssetFSM(StatesGroup):
    choose_asset = State()
    invested = State()
    entry = State()

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
    await m.answer("Выбери монету (у тикеров бывают совпадения):", reply_markup=coin_choice_kb(coins_sorted))

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
    if v is None or v <= 0:
        return await m.answer("Введи положительное число (например 250 или 1000.50).")
    await state.update_data(invested=float(v))
    await state.set_state(AddAssetFSM.entry)
    await m.answer("Введи цену входа (USD), например 40000:")

@router.message(AddAssetFSM.entry)
async def on_add_entry(m: Message, state: FSMContext):
    v = safe_float(m.text or "")
    if v is None or v <= 0:
        return await m.answer("Введи положительное число (например 40000).")

    await state.update_data(entry=float(v), selected_alerts=set())
    await state.set_state(AddAssetFSM.alerts)

    data = await state.get_data()
    sym = data.get("symbol", "")
    nm = data.get("name", "")
    invested = float(data["invested"])
    entry = float(data["entry"])

    preview = "\n".join([
        f"Ок, добавляем: {sym} ({nm})",
        f"Сумма: {fmt_usd(invested)}",
        f"Цена входа: {fmt_usd(entry)}",
        "",
        "Выбери алерты (можно несколько) и нажми «💾 Готово»:"
    ])
    await m.answer(preview, reply_markup=alerts_kb(set()))

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

        asset_id = await add_asset_row(cb.from_user.id, sym, coingecko_id, nm, invested, entry)

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
    if v is None or v <= 0:
        return await m.answer("Введи положительное число.")
    data = await state.get_data()
    asset_id = int(data["asset_id"])
    invested = float(data["invested"])
    entry = float(v)

    await update_asset_row(m.from_user.id, asset_id, invested, entry)
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
                    qty = invested / entry if entry > 0 else 0.0
                    pnl_usd = qty * float(current) - invested
                    pnl_pct = (pnl_usd / invested * 100.0) if invested > 0 else 0.0

                    icon = pnl_icon(pnl_usd)
                    pct = int(r["pct"])
                    sym = r["symbol"]

                    direction = "📉 Риск" if t == "RISK" else "📈 Профит"
                    level_text = f"{'-' if t == 'RISK' else '+'}{pct}%"

                    text = "\n".join([
                        f"⏰ АЛЕРТ: {sym}",
                        f"{direction}: {level_text}",
                        f"Цель: {fmt_usd(target)}",
                        f"Текущая: {fmt_usd(float(current))}",
                        f"{icon} PNL сейчас: {sign_money(pnl_usd)}  ({sign_pct(pnl_pct)})"
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

                total_invested = 0.0
                total_value = 0.0
                for a in assets:
                    invested = float(a["invested_usd"])
                    entry = float(a["entry_price"])
                    qty = invested / entry if entry > 0 else 0.0
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
    log.info("DB_PATH=%s exists=%s", DB_PATH, os.path.exists(DB_PATH))

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


if __name__ == "__main__":
    asyncio.run(main())