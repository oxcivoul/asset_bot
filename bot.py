import os
import re
import time
import math
import asyncio
import logging
from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple, Set, Any
from html import escape  # понадобится в алертах
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
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramRetryAfter,
    TelegramForbiddenError,
    TelegramNotFound,
    TelegramUnauthorizedError,
)
from aiogram.types import BufferedInputFile

from datetime import datetime, timezone, timedelta
try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo  # для Python<3.9, если нужно


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

PRICE_POLL_SECONDS = int(os.getenv("PRICE_POLL_SECONDS", "60"))
PRICE_TTL_SEC = int(os.getenv("PRICE_TTL_SEC", "180"))
SNAPSHOT_EVERY_SECONDS = int(os.getenv("SNAPSHOT_EVERY_SECONDS", "14400"))
SUMMARY_CACHE_TTL_SEC = int(os.getenv("SUMMARY_CACHE_TTL_SEC", str(PRICE_TTL_SEC)))
SUMMARY_PAGE_CHAR_LIMIT = int(os.getenv("SUMMARY_PAGE_CHAR_LIMIT", "3900"))
ASSET_LIST_PAGE_SIZE = int(os.getenv("ASSET_LIST_PAGE_SIZE", "10"))
DEFAULT_ALERT_PRICE_CACHE = max(PRICE_POLL_SECONDS + 5, PRICE_TTL_SEC)
ALERT_PRICE_CACHE_SEC = int(os.getenv("ALERT_PRICE_CACHE_SEC", str(DEFAULT_ALERT_PRICE_CACHE)))
ALERT_RECENT_RESET_SECONDS = int(os.getenv("ALERT_RECENT_RESET_SECONDS", "3600"))
PRICE_CACHE_MAX_AGE_SEC = int(os.getenv("PRICE_CACHE_MAX_AGE_SEC", "86400"))
SEARCH_CACHE_MAX_AGE_SEC = int(os.getenv("SEARCH_CACHE_MAX_AGE_SEC", "86400"))
SEARCH_CACHE_MAX_SIZE = int(os.getenv("SEARCH_CACHE_MAX_SIZE", "500"))
LATEST_PRICE_MAX_AGE_SEC = int(os.getenv("LATEST_PRICE_MAX_AGE_SEC", str(max(PRICE_TTL_SEC * 10, 86400))))
CACHE_CLEANUP_INTERVAL_SEC = int(os.getenv("CACHE_CLEANUP_INTERVAL_SEC", "900"))

SEND_QUEUE_MAXSIZE = int(os.getenv("SEND_QUEUE_MAXSIZE", "500"))
SEND_QUEUE_WORKERS = int(os.getenv("SEND_QUEUE_WORKERS", "2"))
SEND_QUEUE_PUT_TIMEOUT_SEC = float(os.getenv("SEND_QUEUE_PUT_TIMEOUT_SEC", "1.0"))
ALERT_SEND_QUEUE_MAXSIZE = int(os.getenv("ALERT_SEND_QUEUE_MAXSIZE", "200"))
SEND_QUEUE_IDLE_RECHECK_SEC = float(os.getenv("SEND_QUEUE_IDLE_RECHECK_SEC", "0.25"))

if not BOT_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN. Put it into your .env (BOT_TOKEN=...)")

if DB_BACKEND == "postgres" and not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL (Neon). Set it in Render env.")

RISK_LEVELS = [5, 10, 25]
TP_LEVELS = [5, 10, 25]
ALERT_REARM_PCT = float(os.getenv("ALERT_REARM_PCT", "0.3"))
# 0.3% = небольшой запас, чтобы алерт не “дребезжал” туда-сюда вокруг target
ALERT_REARM_FACTOR = max(0.0, ALERT_REARM_PCT / 100.0)
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

@dataclass
class SendTask:
    chat_id: int
    text: str
    kwargs: Dict[str, Any]

send_queue: asyncio.Queue[SendTask] = asyncio.Queue(maxsize=SEND_QUEUE_MAXSIZE)
alert_queue: asyncio.Queue[SendTask] = asyncio.Queue(maxsize=ALERT_SEND_QUEUE_MAXSIZE)

async def queue_text_message(
    chat_id: int,
    text: str,
    *,
    timeout: Optional[float] = None,
    urgent: bool = False,
    **kwargs
) -> bool:
    put_timeout = SEND_QUEUE_PUT_TIMEOUT_SEC if timeout is None else timeout
    target_queue = alert_queue if urgent else send_queue
    queue_label = "alert" if urgent else "default"
    task = SendTask(chat_id=chat_id, text=text, kwargs=kwargs)

    if put_timeout is not None and put_timeout <= 0:
        try:
            target_queue.put_nowait(task)
            return True
        except asyncio.QueueFull:
            log.error(
                "%s send queue saturated (size=%d/%d). Drop chat_id=%s",
                queue_label.capitalize(),
                target_queue.qsize(),
                target_queue.maxsize,
                chat_id
            )
            return False

    try:
        if put_timeout is None:
            await target_queue.put(task)
        else:
            await asyncio.wait_for(target_queue.put(task), timeout=put_timeout)
        if target_queue.full():
            log.warning(
                "%s send queue is full (size=%d/%d). Producers may back off.",
                queue_label.capitalize(),
                target_queue.qsize(),
                target_queue.maxsize
            )
        return True
    except asyncio.TimeoutError:
        log.error(
            "%s send queue put timeout after %.2fs (size=%d/%d). Drop chat_id=%s",
            queue_label.capitalize(),
            put_timeout,
            target_queue.qsize(),
            target_queue.maxsize,
            chat_id
        )
    except asyncio.QueueFull:
        log.error(
            "%s send queue saturated (size=%d/%d). Drop chat_id=%s",
            queue_label.capitalize(),
            target_queue.qsize(),
            target_queue.maxsize,
            chat_id
        )
    return False

async def send_queue_worker(bot: Bot, *, worker_id: int):
    idle_recheck = SEND_QUEUE_IDLE_RECHECK_SEC

    while True:
        source_queue: Optional[asyncio.Queue] = None
        try:
            task: Optional[SendTask] = None

            # 1) Всегда первым делом вычищаем alert_queue (с приоритетом).
            try:
                task = alert_queue.get_nowait()
                source_queue = alert_queue
            except asyncio.QueueEmpty:
                pass

            # 2) Если срочных нет, забираем обычную очередь, но без вечного сна.
            if task is None:
                try:
                    task = send_queue.get_nowait()
                    source_queue = send_queue
                except asyncio.QueueEmpty:
                    try:
                        task = await asyncio.wait_for(send_queue.get(), timeout=idle_recheck)
                        source_queue = send_queue
                    except asyncio.TimeoutError:
                        # Ничего не прилетело — возвращаемся в начало цикла
                        # и снова проверяем alert_queue.
                        continue

        except asyncio.CancelledError:
            break

        if task is None or source_queue is None:
            continue  # защита от невозможных ситуаций

        retries = 0
        while True:
            try:
                await bot.send_message(chat_id=task.chat_id, text=task.text, **task.kwargs)
                break
            except TelegramRetryAfter as e:
                delay = float(e.retry_after) + random.uniform(0.05, 0.3)
                log.warning(
                    "[send-%d] RetryAfter %.2fs for chat_id=%s",
                    worker_id, delay, task.chat_id
                )
                await asyncio.sleep(delay)
            except (TelegramForbiddenError, TelegramNotFound, TelegramUnauthorizedError) as e:
                log.warning(
                    "[send-%d] Permanent failure chat_id=%s (%s). Muting user.",
                    worker_id,
                    task.chat_id,
                    e.__class__.__name__
                )
                await mute_user(task.chat_id, reason=e.__class__.__name__)
                break
            except TelegramBadRequest as e:
                log.warning(
                    "[send-%d] Drop message to chat_id=%s: %s",
                    worker_id, task.chat_id, e
                )
                break
            except asyncio.CancelledError:
                source_queue.task_done()
                raise
            except Exception as e:
                retries += 1
                delay = min(30.0, 2 ** retries)
                log.warning(
                    "[send-%d] Send failed (attempt=%s) chat_id=%s err=%r",
                    worker_id, retries, task.chat_id, e
                )
                await asyncio.sleep(delay)
                if retries >= 5:
                    break

        source_queue.task_done()

# ---------------------------- UI helpers ----------------------------
async def remember_origin_message(state: FSMContext, message: Message):
    data = await state.get_data()
    if data.get("origin_message_id"):
        return
    await state.update_data(
        origin_chat_id=message.chat.id,
        origin_message_id=message.message_id
    )

async def safe_delete(message: Message):
    try:
        await message.delete()
    except TelegramBadRequest:
        pass

PROMPT_CHAT_KEY = "last_prompt_chat_id"
PROMPT_MSG_KEY = "last_prompt_message_id"

async def drop_last_prompt(state: FSMContext, bot: Bot):
    data = await state.get_data()
    chat_id = data.get(PROMPT_CHAT_KEY)
    msg_id = data.get(PROMPT_MSG_KEY)
    if chat_id and msg_id:
        try:
            await bot.delete_message(chat_id, msg_id)
        except TelegramBadRequest:
            pass
    await state.update_data(
        **{PROMPT_CHAT_KEY: None, PROMPT_MSG_KEY: None}
    )

async def send_step_prompt(target: Message, state: FSMContext, text: str, *, reply_markup):
    await drop_last_prompt(state, target.bot)
    msg = await target.answer(text, reply_markup=reply_markup)
    await state.update_data(
        **{PROMPT_CHAT_KEY: msg.chat.id, PROMPT_MSG_KEY: msg.message_id}
    )
    return msg

async def send_menu(bot: Bot, chat_id: int):
    await bot.send_message(chat_id, "Меню снова на месте 👇", reply_markup=main_menu_kb())

def main_menu_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Добавить актив"), KeyboardButton(text="📊 Сводка")],
            [KeyboardButton(text="✏️ Редактировать список активов"), KeyboardButton(text="🗑 Удалить актив")],
            [KeyboardButton(text="📅 PNL за неделю"), KeyboardButton(text="🗓 PNL за месяц")],
        ],
        resize_keyboard=True
    )

def back_to_menu_row() -> List[InlineKeyboardButton]:
    return [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="nav:menu:delete")]

def back_to_menu_inline() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[back_to_menu_row()])

def summary_kb(page: int, total_pages: int) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(text="🔄 Обновить", callback_data=f"summary:refresh:{page}"),
            InlineKeyboardButton(text="ℹ️ Почему не обновляется", callback_data="summary:info")
        ],
        [InlineKeyboardButton(text="➕ Добавить", callback_data="nav:add")],
        [
            InlineKeyboardButton(text="✏️ Редактировать", callback_data="nav:edit"),
            InlineKeyboardButton(text="🗑 Удалить", callback_data="nav:delete")
        ]
    ]

    if total_pages > 1:
        prev_cb = f"summary:page:{page - 1}" if page > 0 else "summary:noop"
        next_cb = f"summary:page:{page + 1}" if page + 1 < total_pages else "summary:noop"
        rows.append([
            InlineKeyboardButton(text="⬅️", callback_data=prev_cb),
            InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data="summary:noop"),
            InlineKeyboardButton(text="➡️", callback_data=next_cb)
        ])

    rows.append(back_to_menu_row())
    return InlineKeyboardMarkup(inline_keyboard=rows)

def fmt_usd(x: float) -> str:
    return f"{x:,.2f}"

def fmt_qty(x: float) -> str:
    # reasonable crypto qty formatting
    if x == 0:
        return "0"
    if abs(x) >= 1:
        return f"{x:,.6f}".rstrip("0").rstrip(".")
    return f"{x:.10f}".rstrip("0").rstrip(".")

def _split_html_block(chunk: str, limit: int) -> List[str]:
    block = (chunk or "").strip()
    if not block:
        return []
    if len(block) <= limit:
        return [block]

    result: List[str] = []
    rest = block
    while len(rest) > limit:
        split_pos = rest.rfind("\n\n", 0, limit)
        if split_pos == -1:
            split_pos = rest.rfind("\n", 0, limit)
        if split_pos == -1 or split_pos < int(limit * 0.6):
            split_pos = limit
        result.append(rest[:split_pos].rstrip())
        rest = rest[split_pos:].lstrip()
    if rest:
        result.append(rest)
    return result


def paginate_text(text: str, limit: int = SUMMARY_PAGE_CHAR_LIMIT) -> List[str]:
    """
    Делит длинный текст на страницы не длиннее limit.
    Сначала пробуем резать по двойному переводу строки, потом по одному,
    иначе — жёстко по limit (уже после HTML-экранирования).
    """
    text = text or ""
    if not text:
        return [""]

    rough_parts: List[str] = []
    rest = text
    while len(rest) > limit:
        split_pos = rest.rfind("\n\n", 0, limit)
        if split_pos == -1:
            split_pos = rest.rfind("\n", 0, limit)
        if split_pos == -1 or split_pos == 0:
            split_pos = limit
        rough_parts.append(rest[:split_pos].rstrip())
        rest = rest[split_pos:].lstrip()
    if rest:
        rough_parts.append(rest)

    pages: List[str] = []
    for part in rough_parts:
        pages.extend(_split_html_block(part, limit))

    pages = [p for p in pages if p]
    return pages or [""]

def prepend_warning_to_pages(
    pages: List[str],
    warning: str,
    limit: int = SUMMARY_PAGE_CHAR_LIMIT
) -> List[str]:
    if not warning:
        return pages

    if not pages:
        updated = [warning]
    else:
        updated = [f"{warning}\n\n{pages[0]}"] + pages[1:]

    safe_pages: List[str] = []
    for chunk in updated:
        safe_pages.extend(_split_html_block(chunk, limit))

    return safe_pages or [warning]

def safe_symbol(sym: str) -> str:
    return escape(sym or "")

def safe_name(name: str) -> str:
    return escape(name or "")

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
        self._price_cache_max_age = PRICE_CACHE_MAX_AGE_SEC
        self._search_cache_max_age = SEARCH_CACHE_MAX_AGE_SEC
        self._search_cache_max_size = SEARCH_CACHE_MAX_SIZE

        # NEW: cache for search(query)
        self._search_cache: Dict[str, Tuple[float, List[dict]]] = {}
        # cache для simple_prices_usd (ключ — отсортированный список id)
        self._simple_price_cache: Dict[str, Dict[str, Any]] = {}

        # NEW: limiter (simple spacing between requests)
        self._rl_lock = asyncio.Lock()
        self._last_request_ts = 0.0
        self._min_interval_sec = float(os.getenv("COINGECKO_MIN_INTERVAL_SEC", "1.2"))
        # 1.2 c ≈ 50 запросов/мин — соответствует лимиту free-tier без 429

        # NEW: adaptive backoff (when CoinGecko returns 429)
        self._base_min_interval_sec = self._min_interval_sec
        self._penalty_until_ts = 0.0
        self._penalty_min_interval_sec = self._min_interval_sec
        self._penalty_ttl_sec = 0  # больше не увеличиваем TTL кэша в штрафе
        # stats
        self._stats_calls = 0
        self._stats_time = 0.0
        self._stats_429 = 0

        # NEW: ограничиваем параллельность HTTP (но не «затыкаем» её в 1 поток)
        self._max_parallel_http = max(1, int(os.getenv("COINGECKO_MAX_PARALLEL", "3")))

        default_user_parallel = min(2, self._max_parallel_http)
        self._max_parallel_user = max(
            1,
            int(os.getenv("COINGECKO_MAX_PARALLEL_USER", str(default_user_parallel)))
        )

        default_bg_parallel = max(1, self._max_parallel_http - self._max_parallel_user)
        self._max_parallel_bg = max(
            1,
            int(os.getenv("COINGECKO_MAX_PARALLEL_BG", str(default_bg_parallel)))
        )

        self._user_sem = asyncio.Semaphore(self._max_parallel_user)
        self._bg_sem = asyncio.Semaphore(self._max_parallel_bg)

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

    def purge_caches(self):
        now = time.time()

        cutoff_price = now - self._price_cache_max_age
        stale_prices = [cid for cid, (ts, _) in self._price_cache_id.items() if ts < cutoff_price]
        for cid in stale_prices:
            self._price_cache_id.pop(cid, None)

        cutoff_search = now - self._search_cache_max_age
        stale_queries = [q for q, (ts, _) in self._search_cache.items() if ts < cutoff_search]
        for q in stale_queries:
            self._search_cache.pop(q, None)

        excess = max(0, len(self._search_cache) - self._search_cache_max_size)
        if excess > 0:
            oldest = sorted(self._search_cache.items(), key=lambda item: item[1][0])[:excess]
            for key, _ in oldest:
                self._search_cache.pop(key, None)

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

    async def _get_json(
        self,
        path: str,
        params: Dict[str, str],
        *,
        tries: int = 5,
        priority: str = "bg",
    ) -> dict:
        url = f"{self.BASE}{path}"
        backoff = 1.0
        last_exc: Optional[BaseException] = None
        sem = self._user_sem if priority == "user" else self._bg_sem

        for attempt in range(1, tries + 1):
            async with sem:
                try:
                    t0 = time.perf_counter()
                    s = await self.session()

                    await self._rate_limit_wait()

                    async with s.get(url, params=params) as r:
                        status = r.status
                        text = await r.text()
                        headers = dict(r.headers)

                    if status == 200:
                        try:
                            obj = json.loads(text) if text else {}
                        except Exception as e:
                            raise RuntimeError(f"CoinGecko bad JSON ({path}): {text[:200]}") from e

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

        data = await self._get_json("/search", {"query": query}, priority="user")
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

    async def simple_prices_usd(
        self,
        ids: List[str],
        ttl_sec: Optional[int] = None,
        return_timestamp: bool = False,
        priority: str = "bg",
    ):
        ids = [cid for cid in ids if cid]
        if not ids:
            return ({}, None) if return_timestamp else {}

        ttl = ttl_sec if ttl_sec is not None else self.PRICE_TTL_SEC
        now = time.time()
        cache_key = ",".join(sorted(ids))

        if cache_key in self._simple_price_cache:
            cached = self._simple_price_cache[cache_key]
            cached_age = now - cached["ts"]
            if cached_age <= ttl:
                if return_timestamp:
                    return cached["data"], cached["ts"]
                return cached["data"]

        out: Dict[str, float] = {}
        last_ts: Optional[float] = None
        chunk_size = 200

        for i in range(0, len(ids), chunk_size):
            chunk = ids[i:i + chunk_size]
            data = await self._get_json(
                "/simple/price",
                {
                    "ids": ",".join(chunk),
                    "vs_currencies": "usd",
                },
                priority=priority,
            )
            for cid, payload in data.items():
                price = payload.get("usd")
                if price is None:
                    continue
                out[cid] = float(price)
            if payload := data.get("timestamp"):
                last_ts = float(payload)

        self._simple_price_cache[cache_key] = {"data": out, "ts": now}
        if return_timestamp:
            return out, last_ts or now
        return out

cg = CoinGeckoClient()

latest_prices: Dict[str, Tuple[float, float]] = {}
latest_prices_lock = asyncio.Lock()
price_direct_last_fetch: Dict[str, float] = {}

async def price_feed_store(new_map: Dict[str, float]):
    if not new_map:
        return
    async with latest_prices_lock:
        now = time.time()
        for cid, price in new_map.items():
            latest_prices[cid] = (now, float(price))

def price_feed_timestamp(cid: str) -> Optional[float]:
    rec = latest_prices.get(cid)
    if not rec:
        return None
    return rec[0]

async def price_feed_get(ids: List[str], *, max_age: float) -> Tuple[Dict[str, float], List[str]]:
    if not ids:
        return {}, []

    cutoff = time.time() - max_age
    out: Dict[str, float] = {}
    missing: List[str] = []

    async with latest_prices_lock:
        for cid in ids:
            rec = latest_prices.get(cid)
            if rec and rec[0] >= cutoff:
                out[cid] = rec[1]
            else:
                missing.append(cid)

    return out, missing

async def ensure_prices(
    ids: List[str],
    *,
    max_age: float,
    direct_ttl: float,
    need_timestamp: bool = False,
    priority: str = "bg",
) -> Tuple[Dict[str, float], List[str], Optional[float]]:
    ids = [cid for cid in ids if cid]
    if not ids:
        if need_timestamp:
            return {}, [], None
        return {}, [], None

    cached, missing = await price_feed_get(ids, max_age=max_age)
    if not missing:
        ts = None
        if need_timestamp and cached:
            ts = max(price_feed_timestamp(cid) for cid in cached.keys())
        return cached, [], ts

    fetched = await cg.simple_prices_usd(
        missing,
        ttl_sec=direct_ttl,
        return_timestamp=need_timestamp,
        priority=priority,
    )

    if need_timestamp:
        fresh_prices, ts = fetched
    else:
        fresh_prices = fetched
        ts = None

    if fresh_prices:
        await price_feed_store(fresh_prices)
        cached.update(fresh_prices)
        missing = [cid for cid in missing if cid not in fresh_prices]

    return cached, missing, ts

async def prune_internal_price_caches():
    cutoff = time.time() - LATEST_PRICE_MAX_AGE_SEC

    async with latest_prices_lock:
        stale_latest = [cid for cid, (ts, _) in latest_prices.items() if ts < cutoff]
        for cid in stale_latest:
            latest_prices.pop(cid, None)

    stale_direct = [cid for cid, ts in price_direct_last_fetch.items() if ts < cutoff]
    for cid in stale_direct:
        price_direct_last_fetch.pop(cid, None)

async def cache_cleanup_loop():
    while True:
        try:
            await prune_internal_price_caches()
            cg.purge_caches()
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.exception("cache_cleanup_loop error: %r", e)

        try:
            await asyncio.sleep(CACHE_CLEANUP_INTERVAL_SEC)
        except asyncio.CancelledError:
            break

# ---------------------------- DB (Postgres / Neon) ----------------------------
pg_pool: Optional[asyncpg.Pool] = None
INSTANCE_LOCK_KEY = int(os.getenv("INSTANCE_LOCK_KEY", "912345678901234567"))
instance_lock_conn: Optional[asyncpg.Connection] = None

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS users (
  user_id BIGINT PRIMARY KEY,
  currency TEXT NOT NULL DEFAULT 'USD',
  last_summary_chat_id BIGINT,
  last_summary_message_id BIGINT,
  tz TEXT NOT NULL DEFAULT 'UTC',
  last_summary_text TEXT,
  last_summary_cached_at BIGINT
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
  total_pnl_usd DOUBLE PRECISION NOT NULL,
  incomplete BOOLEAN NOT NULL DEFAULT FALSE
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
        try:
            await conn.execute("ALTER TABLE assets ADD COLUMN IF NOT EXISTS qty_override DOUBLE PRECISION;")
        except Exception:
            log.exception("Migration failed: ALTER TABLE assets ADD COLUMN qty_override")
            raise

        try:
            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS tz TEXT NOT NULL DEFAULT 'UTC';")
        except Exception:
            log.exception("Migration failed: ALTER TABLE users ADD COLUMN tz")
            raise

        try:
            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS muted BOOLEAN NOT NULL DEFAULT FALSE;")
        except Exception:
            log.exception("Migration failed: ALTER TABLE users ADD COLUMN muted")
            raise

        try:
            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_summary_text TEXT;")
        except Exception:
            log.exception("Migration failed: ALTER TABLE users ADD COLUMN last_summary_text")
            raise

        try:
            await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS last_summary_cached_at BIGINT;")
        except Exception:
            log.exception("Migration failed: ALTER TABLE users ADD COLUMN last_summary_cached_at")
            raise

        try:
            await conn.execute(
                "ALTER TABLE pnl_snapshots ADD COLUMN IF NOT EXISTS incomplete BOOLEAN NOT NULL DEFAULT FALSE;"
            )
        except Exception:
            log.exception("Migration failed: ALTER TABLE pnl_snapshots ADD COLUMN incomplete")

        try:
            await conn.execute(
                "ALTER TABLE alerts ADD COLUMN IF NOT EXISTS rearm_ready BOOLEAN NOT NULL DEFAULT FALSE;"
            )
        except Exception:
            log.exception("Migration failed: ALTER TABLE alerts ADD COLUMN rearm_ready")
            raise

async def acquire_instance_lock() -> bool:
    """
    Берём pg_try_advisory_lock на отдельном соединении (вне пула),
    чтобы не «захоранивать» слот pg_pool навсегда.
    """
    global instance_lock_conn
    if instance_lock_conn is not None:
        return True

    try:
        conn = await asyncpg.connect(
            dsn=DATABASE_URL,
            timeout=30,
            statement_cache_size=0
        )
    except Exception:
        log.exception("acquire_instance_lock: failed to open dedicated connection")
        return False

    try:
        row = await conn.fetchrow("SELECT pg_try_advisory_lock($1) AS ok", INSTANCE_LOCK_KEY)
        ok = bool(row and row.get("ok"))
        if ok:
            instance_lock_conn = conn
            log.info("Instance lock acquired (key=%s)", INSTANCE_LOCK_KEY)
            return True
    except Exception:
        log.exception("acquire_instance_lock: failed to obtain advisory lock")
    finally:
        if instance_lock_conn is None:
            await conn.close()

    log.warning("Instance lock NOT acquired (key=%s). Another instance is running.", INSTANCE_LOCK_KEY)
    return False


async def release_instance_lock():
    global instance_lock_conn
    if instance_lock_conn is None:
        return

    try:
        await instance_lock_conn.execute("SELECT pg_advisory_unlock($1)", INSTANCE_LOCK_KEY)
    except Exception:
        pass

    try:
        await instance_lock_conn.close()
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

async def delete_all_user_data(user_id: int, delete_snapshots: bool = True):
    await db_exec("DELETE FROM assets WHERE user_id=$1", (user_id,))
    if delete_snapshots:
        await db_exec("DELETE FROM pnl_snapshots WHERE user_id=$1", (user_id,))
    await invalidate_summary_cache(user_id)
    # users строку не трогаем — пусть остаются настройки/last_summary

def reset_confirm_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, удалить всё", callback_data="reset:yes")],
        back_to_menu_row(),
    ])

async def upsert_user(user_id: int, *, auto_unmute: bool = True):
    await db_exec(
        "INSERT INTO users(user_id) VALUES ($1) ON CONFLICT(user_id) DO NOTHING",
        (user_id,)
    )

    if auto_unmute:
        await db_exec(
            "UPDATE users SET muted=FALSE WHERE user_id=$1 AND muted",
            (user_id,)
        )

async def set_last_summary_message(user_id: int, chat_id: int, message_id: int):
    await db_exec(
        "UPDATE users SET last_summary_chat_id=$1, last_summary_message_id=$2 WHERE user_id=$3",
        (chat_id, message_id, user_id)
    )

async def get_user_tz_name(user_id: int) -> str:
    row = await db_fetchone("SELECT tz FROM users WHERE user_id=$1", (user_id,))
    return (row or {}).get("tz", "UTC")

async def set_user_tz_name(user_id: int, tz_name: str):
    await db_exec("UPDATE users SET tz=$1 WHERE user_id=$2", (tz_name, user_id))
    await invalidate_summary_cache(user_id)

def resolve_tz(tz_name: str):
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return timezone.utc

async def mute_user(user_id: int, *, reason: str):
    row = await db_fetchone("SELECT muted FROM users WHERE user_id=$1", (user_id,))
    if not row:
        log.warning("mute_user: user_id=%s missing in DB (reason=%s)", user_id, reason)
        return
    if row.get("muted"):
        return
    await db_exec(
        "UPDATE users SET muted=TRUE WHERE user_id=$1",
        (user_id,)
    )
    log.warning(
        "Muted user_id=%s due to %s. Alerts silenced.",
        user_id,
        reason
    )

async def get_cached_summary(user_id: int) -> Optional[str]:
    row = await db_fetchone(
        "SELECT last_summary_text, last_summary_cached_at FROM users WHERE user_id=$1",
        (user_id,)
    )
    if not row:
        return None
    text = row.get("last_summary_text")
    ts = row.get("last_summary_cached_at")
    if not text or ts is None:
        return None
    if time.time() - float(ts) > SUMMARY_CACHE_TTL_SEC:
        return None
    return text

async def save_summary_cache(user_id: int, text: str):
    await db_exec(
        "UPDATE users SET last_summary_text=$1, last_summary_cached_at=$2 WHERE user_id=$3",
        (text, int(time.time()), user_id)
    )


async def invalidate_summary_cache(user_id: int):
    await db_exec(
        "UPDATE users SET last_summary_text=NULL, last_summary_cached_at=NULL WHERE user_id=$1",
        (user_id,)
    )

async def get_summary_text(user_id: int, *, force_refresh: bool = False) -> str:
    if not force_refresh:
        cached = await get_cached_summary(user_id)
        if cached:
            return cached
    text = await build_summary_text(user_id, force_refresh=force_refresh)
    await save_summary_cache(user_id, text)
    return text

async def get_summary_pages(user_id: int, *, force_refresh: bool = False) -> List[str]:
    """
    Возвращает страницы сводки, разбитые по SUMMARY_PAGE_CHAR_LIMIT.
    """
    full_text = await get_summary_text(user_id, force_refresh=force_refresh)
    pages = paginate_text(full_text, SUMMARY_PAGE_CHAR_LIMIT)
    return pages or [full_text]

async def get_summary_pages_safe(
    user_id: int,
    *,
    force_refresh: bool
) -> Tuple[Optional[List[str]], Optional[str]]:
    try:
        pages = await get_summary_pages(user_id, force_refresh=force_refresh)
        return pages, None
    except Exception:
        log.exception(
            "get_summary_pages failed (force_refresh=%s) uid=%s",
            force_refresh,
            user_id
        )
        row = await db_fetchone(
            "SELECT last_summary_text FROM users WHERE user_id=$1",
            (user_id,)
        )
        cached_text = (row or {}).get("last_summary_text")
        if cached_text:
            pages = paginate_text(cached_text, SUMMARY_PAGE_CHAR_LIMIT)
            warning = "⚠️ CoinGecko недоступен — показываю последнюю сохранённую версию."
            return pages, warning
        warning = "⚠️ CoinGecko недоступен. Попробуй чуть позже."
        return None, warning

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

    await invalidate_summary_cache(user_id)
    return int(row["id"])

async def update_asset_row(user_id: int, asset_id: int,
                           invested_usd: float, entry_price: float,
                           qty_override: Optional[float]):
    await db_exec(
        "UPDATE assets SET invested_usd=$1, entry_price=$2, qty_override=$3 "
        "WHERE user_id=$4 AND id=$5",
        (invested_usd, entry_price, qty_override, user_id, asset_id)
    )
    await invalidate_summary_cache(user_id)

async def delete_asset_row(user_id: int, asset_id: int):
    await db_exec("DELETE FROM assets WHERE user_id=$1 AND id=$2", (user_id, asset_id))
    await invalidate_summary_cache(user_id)

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
          al.id AS alert_id,
          al.type,
          al.pct,
          al.target_price,
          al.triggered,
          al.triggered_at,
          al.rearm_ready,
          a.id AS asset_id,
          a.user_id,
          a.symbol,
          a.coingecko_id,
          a.name,
          a.invested_usd,
          a.entry_price,
          a.qty_override
        FROM alerts al
        JOIN assets a ON a.id = al.asset_id
        JOIN users u ON u.user_id = a.user_id
        WHERE NOT u.muted
        """
    )

async def mark_alert_triggered(alert_id: int):
    await db_exec(
        "UPDATE alerts SET triggered=1, triggered_at=$1, rearm_ready=FALSE WHERE id=$2",
        (int(time.time()), alert_id)
    )

async def reset_alert_triggered(alert_id: int):
    await db_exec(
        "UPDATE alerts SET triggered=0, triggered_at=NULL, rearm_ready=FALSE WHERE id=$1",
        (alert_id,)
    )

async def set_alert_rearm_ready(alert_id: int, ready: bool):
    await db_exec("UPDATE alerts SET rearm_ready=$1 WHERE id=$2", (ready, alert_id))

async def all_users() -> List[int]:
    rows = await db_fetchall("SELECT user_id FROM users")
    return [int(r["user_id"]) for r in rows]

async def insert_snapshot(user_id: int, total_value: float, total_invested: float, *, incomplete: bool):
    pnl = total_value - total_invested
    await db_exec(
        """
        INSERT INTO pnl_snapshots(user_id, ts, total_value_usd, total_invested_usd, total_pnl_usd, incomplete)
        VALUES ($1, $2, $3, $4, $5, $6)
        """,
        (user_id, int(time.time()), total_value, total_invested, pnl, incomplete)
    )


async def get_snapshot_latest(user_id: int):
    return await db_fetchone(
        "SELECT * FROM pnl_snapshots WHERE user_id=$1 AND NOT incomplete ORDER BY ts DESC LIMIT 1",
        (user_id,)
    )

async def get_snapshot_at_or_before(user_id: int, ts_cutoff: int):
    return await db_fetchone(
        "SELECT * FROM pnl_snapshots WHERE user_id=$1 AND ts <= $2 AND NOT incomplete ORDER BY ts DESC LIMIT 1",
        (user_id, ts_cutoff)
    )

async def get_snapshot_at_or_after(user_id: int, ts_cutoff: int):
    return await db_fetchone(
        "SELECT * FROM pnl_snapshots WHERE user_id=$1 AND ts >= $2 AND NOT incomplete ORDER BY ts ASC LIMIT 1",
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
    base_invested: float
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

    base_invested = invested if invested > 0 else (qty * entry if entry > 0 else 0.0)

    if current_price is None:
        return AssetComputed(
            asset_id=int(row["id"]),
            symbol=str(row["symbol"]),
            name=str(row["name"] or ""),
            coingecko_id=str(row["coingecko_id"]),
            invested=invested,
            base_invested=base_invested,
            entry=entry,
            qty=qty,
            current=None,
            pnl_usd=None,
            pnl_pct=None,
        )

    current_value = qty * float(current_price)
    pnl_usd = current_value - base_invested
    pnl_pct = None if base_invested == 0 else (pnl_usd / base_invested * 100.0)

    return AssetComputed(
        asset_id=int(row["id"]),
        symbol=str(row["symbol"]),
        name=str(row["name"] or ""),
        coingecko_id=str(row["coingecko_id"]),
        invested=invested,
        base_invested=base_invested,
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
    title = f"🛠 {safe_symbol(comp.symbol)}" + (f" ({safe_name(comp.name)})" if comp.name else "")
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

def format_top_block(label: str, comp: AssetComputed) -> str:
    name_part = f" ({escape(comp.name)})" if comp.name else ""
    pct_text = "—" if comp.pnl_pct is None else sign_pct(comp.pnl_pct)
    current_value = comp.qty * comp.current if comp.current is not None else 0.0
    invested_line = (
        f"Вложено: {money_usd(comp.invested)}"
        if comp.invested > 0
        else "Вложено: 0 (free-позиция)"
    )

    return "\n".join([
        f"{label}: <b>{escape(comp.symbol)}</b>{name_part}",
        f"{pnl_icon(comp.pnl_usd or 0.0)} PNL: {sign_money(comp.pnl_usd or 0.0)} ({pct_text})",
        f"Текущая цена: {fmt_price(comp.current)}",
        f"Стоимость позиции: {money_usd(current_value)}",
        f"Количество: {fmt_qty(comp.qty)}",
        invested_line,
    ])


async def build_summary_text(user_id: int, *, force_refresh: bool = False) -> str:
    tz_name = await get_user_tz_name(user_id)
    tz = resolve_tz(tz_name)
    assets, alerts_by_asset = await list_assets_with_alerts(user_id)
    if not assets:
        return (
            "📊 <b>Сводка портфеля</b>\n\n"
            "Активов пока нет.\n"
            "Нажми «➕ Добавить актив» и заведём первый."
        )

    ids = sorted({a["coingecko_id"] for a in assets})

    price_map: Dict[str, float] = {}
    price_ts: Optional[float] = None
    attempts = 3 if force_refresh else 1
    delay_between = 3.0
    last_exc: Optional[Exception] = None

    for attempt in range(attempts):
        try:
            price_map, missing, price_ts = await ensure_prices(
                ids,
                max_age=0 if force_refresh else PRICE_POLL_SECONDS,
                direct_ttl=0 if force_refresh else PRICE_TTL_SEC,
                need_timestamp=True,
                priority="user",
            )
            if not missing:
                break
            log.warning(
                "Summary: missing prices (%d ids) after ensure_prices attempt %d/%d",
                len(missing), attempt + 1, attempts
            )
        except Exception as e:
            last_exc = e
            log.warning(
                "Summary price fetch failed (attempt %d/%d): %r",
                attempt + 1, attempts, e
            )

        if attempt + 1 < attempts:
            await asyncio.sleep(delay_between)

    if last_exc and not price_map:
        raise last_exc

    price_dt = datetime.fromtimestamp(price_ts, tz) if price_ts else datetime.now(tz)
    price_time_text = price_dt.strftime("%H:%M:%S")

    unique_total = len(ids)
    unique_known = sum(1 for cid in ids if cid in price_map)
    total_positions = len(assets)
    known_positions = sum(1 for a in assets if a["coingecko_id"] in price_map)

    computed: List[AssetComputed] = []
    total_base_invested = 0.0
    total_value = 0.0

    for a in assets:
        cp = price_map.get(a["coingecko_id"])
        comp = compute_asset(a, cp)
        computed.append(comp)

        total_base_invested += comp.base_invested
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
        indent = "\u00A0\u00A0"

        if comp.invested == 0:
            if comp.current is None:
                line_top = f"• <b>{sym}</b> · Стоимость —"
                line_mid = f"{indent}Δ от входа: —"
                line_base = f"{indent}База: —"
            else:
                current_value = comp.qty * float(comp.current)
                line_top = f"• <b>{sym}</b> · Стоимость {money_usd(current_value)}"

                if comp.entry > 0 and comp.qty > 0:
                    base_value = comp.qty * comp.entry
                    delta_usd = current_value - base_value
                    delta_pct = None if base_value == 0 else (delta_usd / base_value * 100.0)
                    pct_text = "—" if delta_pct is None else sign_pct(delta_pct)
                    line_mid = f"{indent}Δ от входа: {sign_money(delta_usd)} ({pct_text})"
                    line_base = f"{indent}База: {money_usd(base_value)}"
                else:
                    line_mid = f"{indent}Δ от входа: —"
                    line_base = f"{indent}База: —"
        else:
            if comp.current is None or comp.pnl_usd is None:
                line_top = f"• <b>{sym}</b> · PNL —"
            else:
                icon = pnl_icon(comp.pnl_usd)
                pct_text = "—" if comp.pnl_pct is None else sign_pct(comp.pnl_pct)
                line_top = f"• <b>{sym}</b> · {icon} {sign_money(comp.pnl_usd)} ({pct_text})"
            line_mid = f"{indent}Вложено: {money_usd(comp.invested)}"

        line_qty = f"{indent}Кол-во монет: {qty_text}"
        line_alert = f"{indent}<b>{format_alert_line(risk_pcts, tp_pcts)}</b>"

        rows_block = [line_top, line_mid]
        if comp.invested == 0:
            rows_block.append(line_base)
        rows_block.extend([line_qty, line_alert])

        blocks.append("\n".join(rows_block))

    coverage_text = f"{known_positions}/{total_positions} поз."
    if unique_total > 0:
        coverage_text += f" • {unique_known}/{unique_total} мон."

    footer_lines: List[str] = [
        ("⚠️ Цены: " if known_positions != total_positions else "✅ Цены: ") + coverage_text,
        f"Вложено: {money_usd(total_base_invested)}",
    ]

    if known_positions != total_positions:
        footer_lines.append("Текущая стоимость: —")
        footer_lines.append("<b>ОБЩИЙ PNL: —</b>")
    else:
        footer_lines.append(f"Текущая стоимость: {money_usd(total_value)}")
        total_pnl = total_value - total_base_invested
        total_pnl_pct = None if total_base_invested == 0 else (total_pnl / total_base_invested * 100.0)
        pct_text = "—" if total_pnl_pct is None else sign_pct(total_pnl_pct)
        footer_lines.append(
            f"<b>{pnl_icon(total_pnl)} ОБЩИЙ PNL: {sign_money(total_pnl)} ({pct_text})</b>"
        )

    footer_lines.append("_________________________________________________________")
    footer_lines.extend([
        "<b>🛠 FAQ</b>",
        f"🕒 Цены CoinGecko: {price_time_text} ({tz_name})",
        "• /about • /help • /reset",
    ])

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
        back_to_menu_row(),
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
    kb.append(back_to_menu_row())
    return InlineKeyboardMarkup(inline_keyboard=kb)

def alerts_kb(selected: Set[str]) -> InlineKeyboardMarkup:
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
    rows.append(back_to_menu_row())
    return InlineKeyboardMarkup(inline_keyboard=rows)

def assets_list_kb(assets_rows, prefix: str, page: int = 0) -> InlineKeyboardMarkup:
    page_size = ASSET_LIST_PAGE_SIZE
    total = len(assets_rows)
    total_pages = max(1, math.ceil(total / page_size)) if total else 1
    page = max(0, min(page, total_pages - 1))
    start = page * page_size
    end = start + page_size
    visible = assets_rows[start:end]

    kb: List[List[InlineKeyboardButton]] = []
    for a in visible:
        kb.append([InlineKeyboardButton(
            text=f"{a['symbol']} — {fmt_usd(a['invested_usd'])} @ {fmt_usd(a['entry_price'])}",
            callback_data=f"{prefix}:asset:{a['id']}"
        )])

    if total_pages > 1:
        prev_cb = f"{prefix}:page:{page - 1}" if page > 0 else f"{prefix}:noop"
        next_cb = f"{prefix}:page:{page + 1}" if page + 1 < total_pages else f"{prefix}:noop"
        kb.append([
            InlineKeyboardButton(text="⬅️", callback_data=prev_cb),
            InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data=f"{prefix}:noop"),
            InlineKeyboardButton(text="➡️", callback_data=next_cb),
        ])

    kb.append(back_to_menu_row())
    return InlineKeyboardMarkup(inline_keyboard=kb)

def assets_edit_list_kb(assets_rows, *, page: int = 0) -> InlineKeyboardMarkup:
    page_size = ASSET_LIST_PAGE_SIZE
    total = len(assets_rows)
    total_pages = max(1, math.ceil(total / page_size)) if total else 1
    page = max(0, min(page, total_pages - 1))
    start = page * page_size
    end = start + page_size
    visible = assets_rows[start:end]
    prefix = "editlist"  # ← добавь вот эту строку

    kb: List[List[InlineKeyboardButton]] = []
    for a in visible:
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

    if total_pages > 1:
        prev_cb = f"{prefix}:page:{page - 1}" if page > 0 else f"{prefix}:noop"
        next_cb = f"{prefix}:page:{page + 1}" if page + 1 < total_pages else f"{prefix}:noop"
        kb.append([
            InlineKeyboardButton(text="⬅️", callback_data=prev_cb),
            InlineKeyboardButton(text=f"{page + 1}/{total_pages}", callback_data=f"{prefix}:noop"),
            InlineKeyboardButton(text="➡️", callback_data=next_cb),
        ])

    kb.append(back_to_menu_row())
    return InlineKeyboardMarkup(inline_keyboard=kb)

def edit_actions_kb(asset_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🗑 Удалить этот актив", callback_data=f"edit:delete:{asset_id}")],
        [InlineKeyboardButton(text="⬅️ К списку", callback_data="nav:edit")],
        back_to_menu_row(),
    ])

# ---------------------------- router/handlers ----------------------------
router = Router()

@router.message(CommandStart())
async def on_start(m: Message):
    await upsert_user(m.from_user.id)
    await m.answer(
        "Здарова! Я бот-менеджер активов: считаю PNL, показываю сводку и присылаю алерты, если цена дошла до уровня.\n\n"
        "Выбирай действие в меню.",
        reply_markup=main_menu_kb()
    )

@router.message(Command("help"))
async def on_help(m: Message):
    await safe_delete(m)
    text = (
        "📚 <b>Что умеет бот</b>\n"
        "• «📊 Сводка» — полный отчёт по активам, PNL и алертам\n"
        "• Алерты фиксированных уровней с гистерезисом\n"
        "• Free-позиции считают базу по entry × qty\n"
        "• «📅/🗓 PNL» — изменения за 7 и 30 дней\n"
        "\n"
        "Команды:\n"
        "/about — информация о проекте\n"
        "/reset — удалить все данные"
    )
    await m.answer(text, reply_markup=back_to_menu_inline())

@router.message(Command("about"))
async def on_about(m: Message):
    await safe_delete(m)
    await m.answer(
        f"Версия бота: alpha {VERSION}\n"
        "Источник цен: CoinGecko (FREE)\n"
        "Автор: @playerholygrail\n"
        "Репо: https://github.com/oxcivoul/asset_bot",
        reply_markup=back_to_menu_inline()
    )

@router.message(F.text == "📊 Сводка")
async def on_summary(m: Message):
    await safe_delete(m)  # ← вот эта строка
    await upsert_user(m.from_user.id)
    pages, warning = await get_summary_pages_safe(m.from_user.id, force_refresh=False)
    if pages is None:
        await m.answer(warning or "⚠️ CoinGecko недоступен. Попробуй позже.")
        return
    if warning:
        pages = prepend_warning_to_pages(pages, warning)
    total_pages = len(pages)
    page = 0
    msg = await m.answer(pages[page], reply_markup=summary_kb(page, total_pages))
    await set_last_summary_message(m.from_user.id, m.chat.id, msg.message_id)

@router.message(Command("reset"))
async def on_reset(m: Message):
    await safe_delete(m)
    await m.answer(
        "Вы уверены, что хотите удалить ВСЕ свои активы и снимки PNL? Это действие необратимо.",
        reply_markup=reset_confirm_kb()
    )

@router.callback_query(F.data == "reset:yes")
async def on_reset_yes(cb: CallbackQuery, state: FSMContext):
    await state.clear()
    await delete_all_user_data(cb.from_user.id, delete_snapshots=True)
    await cb.answer("Данные удалены ✅", show_alert=True)
    await send_menu(cb.bot, cb.message.chat.id)

@router.callback_query(F.data.startswith("summary:refresh"))
async def on_summary_refresh(cb: CallbackQuery):
    await upsert_user(cb.from_user.id)

    try:
        page = int(cb.data.split(":")[2])
    except Exception:
        page = 0

    row = await db_fetchone(
        "SELECT last_summary_cached_at FROM users WHERE user_id=$1",
        (cb.from_user.id,)
    )
    last_ts = float(row["last_summary_cached_at"]) if row and row.get("last_summary_cached_at") else None
    now = time.time()

    use_cache = last_ts is not None and (now - last_ts) < SUMMARY_CACHE_TTL_SEC
    if use_cache:
        wait_left = int(SUMMARY_CACHE_TTL_SEC - (now - last_ts))
        await cb.answer(f"Показываю кэш. Новые цены через ~{max(wait_left, 0)} s")
    else:
        await cb.answer("Запрашиваю свежие цены CoinGecko…")

    pages, warning = await get_summary_pages_safe(cb.from_user.id, force_refresh=not use_cache)
    if pages is None:
        await cb.message.answer(warning or "⚠️ CoinGecko недоступен. Попробуй позже.")
        return

    if warning:
        pages = prepend_warning_to_pages(pages, warning)

    total_pages = len(pages)
    page = max(0, min(page, total_pages - 1))

    try:
        await cb.message.edit_text(pages[page], reply_markup=summary_kb(page, total_pages))
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            await cb.answer("У тебя уже самая свежая версия 👍")
            return
        raise

@router.callback_query(F.data.startswith("summary:page:"))
async def on_summary_page(cb: CallbackQuery):
    await upsert_user(cb.from_user.id)
    try:
        page = int(cb.data.split("summary:page:", 1)[1])
    except Exception:
        return await cb.answer("Некорректная страница")

    pages, warning = await get_summary_pages_safe(cb.from_user.id, force_refresh=False)
    if pages is None:
        await cb.message.answer(warning or "⚠️ CoinGecko недоступен. Попробуй позже.")
        await cb.answer("Ошибка")
        return

    if warning:
        pages = prepend_warning_to_pages(pages, warning)

    total_pages = len(pages)
    if total_pages == 0:
        return await cb.answer("Нет данных")

    page = max(0, min(page, total_pages - 1))

    try:
        await cb.message.edit_text(pages[page], reply_markup=summary_kb(page, total_pages))
    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            await cb.answer("Уже тут 👍")
            return
        raise
    await cb.answer()

@router.callback_query(F.data == "summary:noop")
async def on_summary_noop(cb: CallbackQuery):
    await cb.answer()

@router.callback_query(F.data.in_(("nav:menu", "nav:menu:delete")))
async def on_nav_menu(cb: CallbackQuery, state: FSMContext):
    await drop_last_prompt(state, cb.bot)
    data = await state.get_data()
    origin_chat_id = data.get("origin_chat_id")
    origin_message_id = data.get("origin_message_id")

    await state.clear()

    try:
        await cb.message.delete()
    except TelegramBadRequest:
        pass

    if origin_chat_id and origin_message_id:
        try:
            await cb.bot.delete_message(origin_chat_id, origin_message_id)
        except TelegramBadRequest:
            pass

    await cb.answer("Меню открыто")

@router.callback_query(F.data == "nav:add")
async def on_nav_add(cb: CallbackQuery, state: FSMContext):
    await safe_delete(cb.message)
    await upsert_user(cb.from_user.id)
    await state.clear()
    await state.set_state(AddAssetFSM.mode)
    await send_step_prompt(cb.message, state, "Выбери тип позиции:", reply_markup=add_mode_kb())
    await cb.answer()

@router.callback_query(F.data == "summary:info")
async def on_summary_info(cb: CallbackQuery):
    await upsert_user(cb.from_user.id)
    tz_name = await get_user_tz_name(cb.from_user.id)
    tz = resolve_tz(tz_name)
    row = await db_fetchone(
        "SELECT last_summary_cached_at FROM users WHERE user_id=$1",
        (cb.from_user.id,)
    )
    last_ts = (row or {}).get("last_summary_cached_at")
    if last_ts:
        last_dt = datetime.fromtimestamp(float(last_ts), tz)
        age = int(time.time() - float(last_ts))
        last_line = (
            f"Последнее обновление: {last_dt.strftime('%H:%M:%S')} ({tz_name}), {age}s назад."
        )
    else:
        last_line = "Сводка ещё ни разу не обновлялась."

    text = "\n".join([
        "ℹ️ <b>Почему сводка не обновляется мгновенно?</b>",
        f"CoinGecko даёт новые цены не чаще, чем раз в {SUMMARY_CACHE_TTL_SEC} секунд.",
        "Если ты жмёшь «Обновить» раньше — бот показывает кэш и не трогает лимит.",
        last_line,
        "Как только таймер истечёт, кнопка даст свежие цены."
    ])
    await cb.message.answer(text, reply_markup=back_to_menu_inline())
    await cb.answer("Готово")

@router.message(F.text == "➕ Добавить актив")
async def on_add_asset_start(m: Message, state: FSMContext):
    await safe_delete(m)
    await upsert_user(m.from_user.id)
    await state.clear()
    await remember_origin_message(state, m)
    await state.set_state(AddAssetFSM.mode)
    await send_step_prompt(m, state, "Выбери тип позиции:", reply_markup=add_mode_kb())

@router.message(AddAssetFSM.ticker)
async def on_add_ticker(m: Message, state: FSMContext):
    await safe_delete(m)
    q = (m.text or "").strip()
    if not q or len(q) > 40:
        await send_step_prompt(
            m, state,
            "Тикер слишком странный. Давай проще: BTC / ETH / SOL.",
            reply_markup=back_to_menu_inline()
        )
        return

    try:
        coins = await cg.search(q)
    except Exception as e:
        log.warning("CoinGecko search failed: %r", e)
        await send_step_prompt(
            m, state,
            "CoinGecko не ответил. Попробуй ещё раз чуть позже.",
            reply_markup=back_to_menu_inline()
        )
        return

    if not coins:
        await send_step_prompt(
            m, state,
            "Ничего не нашёл. Попробуй другой запрос (например: bitcoin).",
            reply_markup=back_to_menu_inline()
        )
        return

    q_up = q.upper()
    coins_sorted = sorted(coins, key=lambda c: (c.get("symbol") != q_up, c.get("name") or ""))
    await state.update_data(coins=coins_sorted[:10])
    await state.set_state(AddAssetFSM.choose_coin)
    await send_step_prompt(
        m, state,
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
        await drop_last_prompt(state, cb.bot)
        await state.clear()
        await cb.answer("Не нашёл монету. Начни заново.")
        return

    await safe_delete(cb.message)

    mode = (data.get("add_mode") or "paid").strip().lower()

    await state.update_data(
        coingecko_id=chosen["id"],
        symbol=(chosen.get("symbol") or "").upper(),
        name=chosen.get("name") or ""
    )

    if mode == "free":
        await state.update_data(invested=0.0)
        await state.set_state(AddAssetFSM.entry)
        await send_step_prompt(
            cb.message, state,
            "Бесплатная позиция.\n"
            "Введи цену, по которой досталась монета (USD). Нужно > 0, чтобы считать PNL и алерты:",
            reply_markup=back_to_menu_inline()   # ← больше никаких лишних кнопок
        )
        await cb.answer()
        return

    await state.set_state(AddAssetFSM.invested)
    await send_step_prompt(
        cb.message, state,
        "Введи сумму, на которую купил (в USD). Например 1000:",
        reply_markup=back_to_menu_inline()
    )
    await cb.answer()

@router.callback_query(AddAssetFSM.mode, F.data.startswith("add:mode:"))
async def on_add_mode(cb: CallbackQuery, state: FSMContext):
    mode = cb.data.split("add:mode:", 1)[1].strip().lower()
    if mode not in ("paid", "free"):
        return await cb.answer("Не понял")

    await state.update_data(add_mode=mode)
    await state.set_state(AddAssetFSM.ticker)

    await safe_delete(cb.message)

    await send_step_prompt(
        cb.message, state,
        "Введи тикер/название монеты (пример: BTC, ETH, SOL):",
        reply_markup=back_to_menu_inline()
    )
    await cb.answer()

@router.callback_query(F.data == "info:free")
async def on_info_free(cb: CallbackQuery):
    await cb.answer(
        "Бесплатные позиции считаются так:\n"
        "• entry > 0 — база и алерты\n"
        "• qty — руками, от него считаем стоимость и PNL",
        show_alert=True
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
        selected_alerts=list(selected)
    )
    await state.set_state(EditAlertsFSM.alerts)

    sym = a["symbol"]
    entry = float(a["entry_price"])
    ssym = safe_symbol(sym)
    msg = "\n".join([
        f"Редактируем алерты для {ssym}",
        f"Цена входа: {fmt_usd(entry)}",
        "",
        "Отметь уровни и нажми «💾 Готово»"
    ])
    await safe_delete(cb.message)
    await cb.message.answer(msg, reply_markup=alerts_kb(selected))
    await cb.answer()

@router.message(AddAssetFSM.invested)
async def on_add_invested(m: Message, state: FSMContext):
    await safe_delete(m)

    v = safe_float(m.text or "")
    if v is None or v < 0:
        await send_step_prompt(
            m, state,
            "Сумма не может быть отрицательной.",
            reply_markup=back_to_menu_inline()
        )
        return

    await state.update_data(invested=float(v))
    await state.set_state(AddAssetFSM.entry)
    await send_step_prompt(
        m, state,
        "Введи цену входа (USD), например 40000:",
        reply_markup=back_to_menu_inline()
    )

@router.message(AddAssetFSM.entry)
async def on_add_entry(m: Message, state: FSMContext):
    await safe_delete(m)

    v = safe_float(m.text or "")
    if v is None or v <= 0:
        await send_step_prompt(
            m, state,
            "Цена входа должна быть больше 0.",
            reply_markup=back_to_menu_inline()
        )
        return

    entry = float(v)
    data = await state.get_data()
    invested = float(data.get("invested", 0.0))

    await state.update_data(entry=entry)

    if invested == 0:
        await state.set_state(AddAssetFSM.quantity)
        await send_step_prompt(
            m, state,
            "Введи количество монет (например 123.4567):\n"
            "PNL и алерты будут считаться от этой цены входа.",
            reply_markup=back_to_menu_inline()
        )
        return

    await state.update_data(selected_alerts=[], qty_override=None)

    sym = (data.get("symbol") or "").upper()
    nm = data.get("name") or ""
    ssym = safe_symbol(sym)
    sname = safe_name(nm)

    preview = "\n".join([
        f"Ок, добавляем: {ssym} ({sname})",
        f"Сумма: {fmt_usd(invested)}",
        f"Цена входа: {fmt_usd(entry)}",
        "",
        "Выбери алерты (можно несколько) и нажми «💾 Готово»:"
    ])
    await state.set_state(AddAssetFSM.alerts)
    await send_step_prompt(m, state, preview, reply_markup=alerts_kb(set()))

@router.message(AddAssetFSM.quantity)
async def on_add_quantity(m: Message, state: FSMContext):
    await safe_delete(m)

    qty = safe_float(m.text or "")
    if qty is None or qty <= 0:
        await send_step_prompt(
            m, state,
            "Количество должно быть больше нуля.",
            reply_markup=back_to_menu_inline()
        )
        return

    await state.update_data(qty_override=float(qty))

    data = await state.get_data()
    sym = (data.get("symbol") or "").upper()
    nm = data.get("name") or ""
    invested = float(data.get("invested", 0.0))
    entry = float(data.get("entry", 0.0))
    qty_override = float(qty)

    await state.update_data(selected_alerts=[])

    ssym = safe_symbol(sym)
    sname = safe_name(nm)

    note = "" if entry > 0 else "\n⚠️ Цена входа = 0, % алерты и PNL не будут посчитаны."

    preview = "\n".join([
        f"Ок, добавляем: {ssym} ({sname})",
        f"Сумма: {fmt_usd(invested)}",
        f"Цена входа: {fmt_usd(entry)}",
        f"Количество: {fmt_qty(qty_override)}",
        "",
        "Выбери алерты (можно несколько) и нажми «💾 Готово»:"
    ])

    await state.set_state(AddAssetFSM.alerts)
    await send_step_prompt(m, state, preview + note, reply_markup=alerts_kb(set()))

@router.callback_query(AddAssetFSM.alerts, F.data.startswith("add:alert:"))
async def on_add_alerts(cb: CallbackQuery, state: FSMContext):
    action = cb.data.split("add:alert:", 1)[1]
    data = await state.get_data()
    selected: Set[str] = set(data.get("selected_alerts", []))
    force_finish = False

    if action == "none":
        selected = set()
        await state.update_data(selected_alerts=list(selected))
        force_finish = True

    if force_finish:
        action = "done"

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

        await invalidate_summary_cache(cb.from_user.id)
        await drop_last_prompt(state, cb.bot)
        await state.clear()
        await safe_delete(cb.message)

        toast = "Актив добавлен без алертов ✅" if force_finish else "Актив добавлен ✅"
        await cb.answer(toast, show_alert=True)
        await send_menu(cb.bot, cb.message.chat.id)
        return

    allowed = {f"RISK:{p}" for p in RISK_LEVELS} | {f"TP:{p}" for p in TP_LEVELS}
    if action in allowed:
        if action in selected:
            selected.remove(action)
        else:
            selected.add(action)
        await state.update_data(selected_alerts=list(selected))
        await cb.message.edit_reply_markup(reply_markup=alerts_kb(selected))
        return await cb.answer("Ок")

    await cb.answer("Не понял")

@router.callback_query(EditAlertsFSM.alerts, F.data.startswith("add:alert:"))
async def on_edit_alerts(cb: CallbackQuery, state: FSMContext):
    action = cb.data.split("add:alert:", 1)[1]
    data = await state.get_data()
    selected: Set[str] = set(data.get("selected_alerts", []))
    asset_id = int(data.get("asset_id"))
    entry = float(data.get("entry", 0.0))
    force_finish = False

    if action == "none":
        selected = set()
        await state.update_data(selected_alerts=list(selected))
        force_finish = True

    if force_finish:
        action = "done"

    if action == "done":
        if entry <= 0:
            await drop_last_prompt(state, cb.bot)
            await state.clear()
            await safe_delete(cb.message)
            await cb.answer("Нет цены входа", show_alert=True)
            return

        alert_rows: List[Tuple[str, int, float]] = []
        for s in sorted(selected):
            t, pct_str = s.split(":")
            pct = int(pct_str)
            target = entry * (1 - pct / 100.0) if t == "RISK" else entry * (1 + pct / 100.0)
            alert_rows.append((t, pct, float(target)))

        await replace_alerts(asset_id, alert_rows)
        await invalidate_summary_cache(cb.from_user.id)
        await state.clear()
        await safe_delete(cb.message)

        toast = "Алерты отключены ✅" if force_finish else "Алерты обновлены ✅"
        await cb.answer(toast, show_alert=True)
        await send_menu(cb.bot, cb.message.chat.id)
        return

    allowed = {f"RISK:{p}" for p in RISK_LEVELS} | {f"TP:{p}" for p in TP_LEVELС}
    if action in allowed:
        if action in selected:
            selected.remove(action)
        else:
            selected.add(action)
        await state.update_data(selected_alerts=list(selected))
        await cb.message.edit_reply_markup(reply_markup=alerts_kb(selected))
        return await cb.answer("Ок")

    await cb.answer("Не понял")

# ------- delete -------
@router.message(F.text == "🗑 Удалить актив")
async def on_delete_menu(m: Message, state: FSMContext):
    await safe_delete(m)
    assets = await list_assets(m.from_user.id)
    if not assets:
        return await m.answer(
            "Активов пока нет — удалять нечего.",
            reply_markup=back_to_menu_inline()
        )

    await state.clear()
    await remember_origin_message(state, m)
    await send_step_prompt(
        m, state,
        "Выбери актив для удаления:",
        reply_markup=assets_list_kb(assets, "del")
    )

@router.callback_query(F.data == "nav:delete")
async def on_delete_menu_cb(cb: CallbackQuery, state: FSMContext):
    assets = await list_assets(cb.from_user.id)
    if not assets:
        await cb.message.answer(
            "Активов пока нет — удалять нечего.",
            reply_markup=back_to_menu_inline()
        )
        return await cb.answer()

    await safe_delete(cb.message)
    await send_step_prompt(
        cb.message, state,
        "Выбери актив для удаления:",
        reply_markup=assets_list_kb(assets, "del")
    )
    await cb.answer()

@router.callback_query(F.data.startswith("del:page:"))
async def on_delete_page(cb: CallbackQuery):
    try:
        page = int(cb.data.split("del:page:", 1)[1])
    except ValueError:
        await cb.answer("Страница недоступна")
        return

    assets = await list_assets(cb.from_user.id)
    if not assets:
        await cb.message.edit_text("Активов пока нет — удалять нечего.", reply_markup=main_menu_kb())
        await cb.answer("Портфель пуст")
        return

    kb = assets_list_kb(assets, "del", page=page)
    await cb.message.edit_text("Выбери актив для удаления:", reply_markup=kb)
    await cb.answer()

@router.callback_query(F.data == "del:noop")
async def on_delete_noop(cb: CallbackQuery):
    await cb.answer("Это крайняя страница")

@router.callback_query(F.data.startswith("del:asset:"))
async def on_delete_asset(cb: CallbackQuery, state: FSMContext):
    asset_id = int(cb.data.split("del:asset:", 1)[1])
    a = await get_asset(cb.from_user.id, asset_id)
    if not a:
        return await cb.answer("Актив не найден")

    await delete_asset_row(cb.from_user.id, asset_id)

    await drop_last_prompt(state, cb.bot)
    await safe_delete(cb.message)
    await cb.answer("Актив удалён ✅", show_alert=True)
    await send_menu(cb.bot, cb.message.chat.id)

# ------- edit -------
@router.message(F.text == "✏️ Редактировать список активов")
async def on_edit_menu(m: Message, state: FSMContext):
    await safe_delete(m)

    assets = await list_assets(m.from_user.id)
    if not assets:
        return await m.answer(
            "Активов пока нет — редактировать нечего.",
            reply_markup=back_to_menu_inline()
        )

    await state.clear()
    await remember_origin_message(state, m)
    await state.set_state(EditAssetFSM.choose_asset)

    await send_step_prompt(
        m, state,
        "Выбери актив:\n✏️ — редактировать, 🗑 — удалить",
        reply_markup=assets_edit_list_kb(assets)
    )

@router.callback_query(F.data == "nav:edit")
async def on_edit_menu_cb(cb: CallbackQuery, state: FSMContext):
    assets = await list_assets(cb.from_user.id)
    if not assets:
        await cb.message.answer(
            "Активов пока нет — редактировать нечего.",
            reply_markup=back_to_menu_inline()
        )
        return await cb.answer()

    await state.clear()
    await state.set_state(EditAssetFSM.choose_asset)

    await safe_delete(cb.message)
    await send_step_prompt(
        cb.message, state,
        "Выбери актив:\n✏️ — редактировать, 🗑 — удалить",
        reply_markup=assets_edit_list_kb(assets)
    )
    await cb.answer()

@router.callback_query(F.data.startswith("editlist:page:"))
async def on_edit_list_page(cb: CallbackQuery, state: FSMContext):
    try:
        page = int(cb.data.split("editlist:page:", 1)[1])
    except ValueError:
        await cb.answer("Страница недоступна")
        return

    assets = await list_assets(cb.from_user.id)
    if not assets:
        await state.clear()
        await cb.message.edit_text("Активов пока нет — редактировать нечего.", reply_markup=main_menu_kb())
        await cb.answer("Портфель пуст")
        return

    await state.set_state(EditAssetFSM.choose_asset)
    text = (
        "Выбери актив:\n"
        "✏️ — редактировать, 🗑 — удалить"
    )
    await cb.message.edit_text(text, reply_markup=assets_edit_list_kb(assets, page=page))
    await cb.answer()

@router.callback_query(F.data == "editlist:noop")
async def on_edit_list_noop(cb: CallbackQuery):
    await cb.answer("Это крайняя страница")

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
    await drop_last_prompt(state, cb.bot)
    await safe_delete(cb.message)

    assets = await list_assets(cb.from_user.id)
    removed_sym = safe_symbol(a['symbol'])

    if not assets:
        await state.clear()
        await cb.answer("Актив удалён ✅", show_alert=True)
        await send_menu(cb.bot, cb.message.chat.id)
        return

    await state.clear()
    await state.set_state(EditAssetFSM.choose_asset)
    await send_step_prompt(
        cb.message, state,
        f"Удалил {removed_sym} ✅\n\nВыбери следующий актив:",
        reply_markup=assets_edit_list_kb(assets)
    )
    await cb.answer("Актив удалён ✅", show_alert=True)

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

    ssym = safe_symbol(a['symbol'])
    sname = safe_name(a['name'] or "")
    text = "\n".join([
        f"Редактируем {ssym} ({sname})",
        f"Текущая сумма: {fmt_usd(a['invested_usd'])}",
        f"Текущая цена входа: {fmt_usd(a['entry_price'])}",
        "",
        "Введи новую сумму (USD). Можно 0 для бесплатной позиции:"
    ])

    await safe_delete(cb.message)
    await send_step_prompt(cb.message, state, text, reply_markup=edit_actions_kb(asset_id))
    await cb.answer()

@router.message(EditAssetFSM.invested)
async def on_edit_invested(m: Message, state: FSMContext):
    await safe_delete(m)

    data = await state.get_data()
    asset_id = int(data["asset_id"])

    v = safe_float(m.text or "")
    if v is None or v < 0:
        await send_step_prompt(
            m, state,
            "Сумма не может быть отрицательной. Можно 0 для бесплатной позиции.",
            reply_markup=edit_actions_kb(asset_id)
        )
        return

    await state.update_data(invested=float(v))
    await state.set_state(EditAssetFSM.entry)
    await send_step_prompt(
        m, state,
        "Введи новую цену входа (USD). Можно 0, если хочешь ввести количество вручную:",
        reply_markup=edit_actions_kb(asset_id)
    )

@router.message(EditAssetFSM.entry)
async def on_edit_entry(m: Message, state: FSMContext):
    await safe_delete(m)

    data = await state.get_data()
    asset_id = int(data["asset_id"])
    invested = float(data.get("invested", 0.0))

    v = safe_float(m.text or "")
    if v is None or v < 0:
        await send_step_prompt(
            m, state,
            "Цена не может быть отрицательной.",
            reply_markup=edit_actions_kb(asset_id)
        )
        return

    entry = float(v)
    await state.update_data(entry=entry)

    if invested == 0 or entry == 0:
        await state.set_state(EditAssetFSM.quantity)
        await send_step_prompt(
            m, state,
            "Введи количество монет для позиции (например 12.34):",
            reply_markup=edit_actions_kb(asset_id)
        )
        return

    await drop_last_prompt(state, m.bot)
    await update_asset_row(m.from_user.id, asset_id, invested, entry, qty_override=None)
    await recompute_alert_targets(asset_id, entry)
    await state.clear()
    await m.answer("Обновил ✅")
    await send_menu(m.bot, m.chat.id)

@router.message(EditAssetFSM.quantity)
async def on_edit_quantity(m: Message, state: FSMContext):
    await safe_delete(m)

    data = await state.get_data()
    asset_id = int(data["asset_id"])
    invested = float(data["invested"])
    entry = float(data["entry"])

    qty = safe_float(m.text or "")
    if qty is None or qty <= 0:
        await send_step_prompt(
            m, state,
            "Количество должно быть больше нуля.",
            reply_markup=edit_actions_kb(asset_id)
        )
        return

    await drop_last_prompt(state, m.bot)
    await update_asset_row(m.from_user.id, asset_id, invested, entry, qty_override=float(qty))
    await recompute_alert_targets(asset_id, entry)
    await state.clear()
    await m.answer("Обновил ✅")
    await send_menu(m.bot, m.chat.id)

# ------- pnl periods -------
@router.message(F.text.in_(["📅 PNL за неделю", "🗓 PNL за месяц"]))
async def on_pnl_period(m: Message):
    await safe_delete(m)
    await upsert_user(m.from_user.id)
    latest = await get_snapshot_latest(m.from_user.id)
    if not latest:
        return await m.answer(
            "Пока нет истории для недели/месяца.\n"
            "Я записываю снапшоты раз в час — чуть времени и будет статистика.",
            reply_markup=back_to_menu_inline()
        )

    days = 7 if m.text.startswith("📅") else 30
    cutoff = int(time.time()) - days * 24 * 3600
    then = await get_snapshot_at_or_before(m.from_user.id, cutoff)
    if not then:
        return await m.answer(
            f"Недостаточно данных, чтобы посчитать за {days} дней.\n"
            "Нужно, чтобы накопились снапшоты.",
            reply_markup=back_to_menu_inline()
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
        reply_markup=back_to_menu_inline()
    )

# ---------------------------- background loops ----------------------------
async def price_feed_loop():
    while True:
        try:
            rows = await db_fetchall("SELECT DISTINCT coingecko_id FROM assets")
            ids = [str(r["coingecko_id"]) for r in rows if r.get("coingecko_id")]
            if not ids:
                await asyncio.sleep(PRICE_POLL_SECONDS)
                continue

            now = time.time()
            stale_ids = [
                cid for cid in ids
                if cid not in price_direct_last_fetch
                or (now - price_direct_last_fetch[cid]) >= PRICE_TTL_SEC
            ]
            if not stale_ids:
                await asyncio.sleep(PRICE_POLL_SECONDS)
                continue

            CHUNK = int(os.getenv("COINGECKO_SIMPLE_PRICE_CHUNK", "40"))
            stale_ids.sort(key=lambda cid: price_direct_last_fetch.get(cid, 0))
            batch = stale_ids[:CHUNK]

            try:
                fresh = await cg.simple_prices_usd(batch, ttl_sec=PRICE_TTL_SEC)
            except Exception as e:
                log.warning("price_feed_loop batch failed: %r", e)
                await asyncio.sleep(PRICE_POLL_SECONDS)
                continue

            await price_feed_store(fresh)

            fetch_ts = time.time()
            for cid in fresh:
                price_direct_last_fetch[cid] = fetch_ts

            if len(fresh) != len(batch):
                missing_ids = [cid for cid in batch if cid not in fresh]
                log.debug(
                    "price_feed_loop: CoinGecko skipped %d ids: %s",
                    len(missing_ids),
                    ", ".join(missing_ids)
                )

        except asyncio.CancelledError:
            break
        except Exception as e:
            log.exception("price_feed_loop error: %r", e)

        try:
            await asyncio.sleep(PRICE_POLL_SECONDS)
        except asyncio.CancelledError:
            break

async def alerts_loop():
    while True:
        try:
            rows = await pending_alerts_joined()
            if not rows:
                await asyncio.sleep(PRICE_POLL_SECONDS + random.uniform(0, 5))
                continue

            ids = tuple(sorted({r["coingecko_id"] for r in rows if r.get("coingecko_id")}))
            if not ids:
                await asyncio.sleep(PRICE_POLL_SECONDS + random.uniform(0, 5))
                continue

            price_map, missing = await price_feed_get(list(ids), max_age=ALERT_PRICE_CACHE_SEC)

            if missing:
                await asyncio.sleep(1.0)
                price_map_retry, missing_retry = await price_feed_get(
                    list(ids),
                    max_age=ALERT_PRICE_CACHE_SEC
                )
                price_map = price_map_retry
                missing = missing_retry

            if missing:
                now = time.time()
                to_fetch = [
                    cid for cid in missing
                    if (now - price_direct_last_fetch.get(cid, 0)) >= ALERT_PRICE_CACHE_SEC
                ]

                if to_fetch:
                    fresh = await cg.simple_prices_usd(
                        to_fetch,
                        ttl_sec=ALERT_PRICE_CACHE_SEC
                    )
                    await price_feed_store(fresh)
                    price_map.update(fresh)

                    fetch_ts = time.time()
                    for cid in fresh:
                        price_direct_last_fetch[cid] = fetch_ts

                    missing = [cid for cid in missing if cid not in fresh]

                if missing:
                    log.debug(
                        "alerts_loop: waiting for price feed refresh (missing: %s)",
                        ", ".join(missing)
                    )
                    await asyncio.sleep(1.0)
                    continue

            await price_feed_store(price_map)
            for r in rows:
                cid = r.get("coingecko_id")

                current_price = price_map.get(cid)
                if current_price is None:
                    continue

                cur = float(current_price)
                alert_type = str(r.get("type") or "")
                target = float(r["target_price"])
                pct = int(r["pct"])
                alert_id = int(r["alert_id"])
                triggered = bool(r.get("triggered"))
                rearm_ready = bool(r.get("rearm_ready"))

                lower_band = target * (1 - ALERT_REARM_FACTOR)
                upper_band = target * (1 + ALERT_REARM_FACTOR)
                inside_band = lower_band <= cur <= upper_band

                if triggered:
                    if rearm_ready:
                        if inside_band:
                            await reset_alert_triggered(alert_id)
                            triggered = False
                        else:
                            continue
                    else:
                        if not inside_band:
                            await set_alert_rearm_ready(alert_id, True)
                        continue

                if not triggered:
                    should_fire = (
                        (alert_type == "RISK" and cur <= target) or
                        (alert_type == "TP"   and cur >= target)
                    )
                    if not should_fire:
                        continue

                    qty_override = float(r.get("qty_override") or 0.0)
                    invested = float(r["invested_usd"])
                    entry = float(r["entry_price"])

                    if qty_override > 0:
                        qty = qty_override
                    elif entry > 0 and invested > 0:
                        qty = invested / entry
                    else:
                        qty = 0.0
                    if qty == 0:
                        continue

                    base_invested = invested if invested > 0 else qty * entry
                    pnl_usd = qty * cur - base_invested
                    pnl_pct = None if base_invested == 0 else pnl_usd / base_invested * 100.0
                    pct_text = "—" if pnl_pct is None else sign_pct(pnl_pct)

                    icon = "🔴" if alert_type == "RISK" else "🟢"
                    verb = "снизилась" if alert_type == "RISK" else "выросла"

                    if entry > 0:
                        entry_delta_pct = (cur - entry) / entry * 100.0
                        delta_line = f"Δ от входа: {sign_pct(entry_delta_pct)}"
                    else:
                        approx_delta = float(pct if alert_type == "TP" else -pct)
                        delta_line = f"Δ от входа: {sign_pct(approx_delta)}"

                    text = "\n".join([
                        f"<b>🔔 АЛЕРТ: {escape(r['symbol'] or '')}</b>",
                        f"{icon} Цена {verb} на {pct}% (уровень {fmt_price(target)})",
                        delta_line,
                        f"Текущая цена: {fmt_price(cur)}",
                        f"{pnl_icon(pnl_usd)} PNL сейчас: {sign_money(pnl_usd)} ({pct_text})",
                    ])
                    queued = await queue_text_message(int(r["user_id"]), text, urgent=True)
                    if not queued:
                        log.error(
                            "alerts_loop: drop alert_id=%s for chat_id=%s (send queue saturated)",
                            alert_id,
                            r["user_id"]
                        )
                        continue
                    await mark_alert_triggered(alert_id)

        except asyncio.CancelledError:
            break
        except Exception as e:
            log.exception("alerts_loop error: %r", e)

        try:
            await asyncio.sleep(PRICE_POLL_SECONDS + random.uniform(0, 5))
        except asyncio.CancelledError:
            break

async def snapshots_loop():
    while True:
        try:
            users = await all_users()
            for uid in users:
                assets = await list_assets(uid)
                if not assets:
                    continue

                ids = list({a["coingecko_id"] for a in assets})
                price_map, missing = await price_feed_get(ids, max_age=PRICE_POLL_SECONDS)

                if missing:
                    await asyncio.sleep(1.0)
                    price_map_retry, missing_retry = await price_feed_get(
                        ids,
                        max_age=PRICE_POLL_SECONDS
                    )
                    price_map = price_map_retry
                    missing = missing_retry

                if missing:
                    now = time.time()
                    to_fetch = [
                        cid for cid in missing
                        if (now - price_direct_last_fetch.get(cid, 0)) >= PRICE_TTL_SEC
                    ]

                    if to_fetch:
                        fresh = await cg.simple_prices_usd(
                            to_fetch,
                            ttl_sec=PRICE_TTL_SEC
                        )
                        await price_feed_store(fresh)
                        price_map.update(fresh)

                        fetch_ts = time.time()
                        for cid in fresh:
                            price_direct_last_fetch[cid] = fetch_ts

                        missing = [cid for cid in missing if cid not in fresh]

                    if missing:
                        log.debug(
                            "snapshots_loop: waiting for price feed refresh (missing: %s)",
                            ", ".join(missing)
                        )
                        await asyncio.sleep(1.0)
                        continue
                known = sum(1 for cid in ids if cid in price_map)
                if known == 0:
                    log.warning("Skip snapshot for uid=%s: no prices", uid)
                    continue

                incomplete = known != len(ids)
                if incomplete:
                    log.warning("Partial snapshot uid=%s: prices %d/%d", uid, known, len(ids))

                total_base_invested = 0.0
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

                    base_invested = invested if invested > 0 else (qty * entry if entry > 0 else 0.0)
                    total_base_invested += base_invested

                    cp = price_map.get(a["coingecko_id"])
                    if cp is not None:
                        total_value += qty * float(cp)

                await insert_snapshot(
                    uid,
                    total_value=total_value,
                    total_invested=total_base_invested,
                    incomplete=incomplete
                )
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.exception("snapshots_loop error: %r", e)

        try:
            await asyncio.sleep(SNAPSHOT_EVERY_SECONDS + random.uniform(0, 5))
        except asyncio.CancelledError:
            break
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

    send_workers = [
        asyncio.create_task(send_queue_worker(bot, worker_id=i + 1))
        for i in range(SEND_QUEUE_WORKERS)
    ]
    price_task = asyncio.create_task(price_feed_loop())
    alert_task = asyncio.create_task(alerts_loop())
    snap_task = asyncio.create_task(snapshots_loop())
    cleanup_task = asyncio.create_task(cache_cleanup_loop())
    tasks = (health_task, *send_workers, price_task, alert_task, snap_task, cleanup_task)

    try:
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        for t in tasks:
            t.cancel()
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