import os
import threading
import time
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, Any, List

import ccxt
import requests
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo

# ================= CONFIG =================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
PUBLIC_URL = os.environ.get("PUBLIC_URL", "")

if not TELEGRAM_BOT_TOKEN or not PUBLIC_URL:
    raise RuntimeError("❌ Set TELEGRAM_BOT_TOKEN and PUBLIC_URL environment variables!")

WEBHOOK_PATH = f"/webhook/{TELEGRAM_BOT_TOKEN}"
WEBHOOK_URL = PUBLIC_URL.rstrip("/") + WEBHOOK_PATH

EX_IDS = ["binance", "gateio", "mexc", "bingx", "lbank"]
QUOTE = "USDT"
SPREAD_THRESHOLD_MIN = 1.0
SPREAD_THRESHOLD_MAX = 50.0
COMMISSION = 0.0005  # 0.05%
DB_FILE = "miniapp.db"
FETCH_INTERVAL = 30  # Увеличим интервал, т.к. запросов стало больше

SIGNALS: List[Dict[str, Any]] = []
SIGNALS_LOCK = threading.Lock()

# ================= FASTAPI =================
app = FastAPI()
templates = Jinja2Templates(directory="templates")
if os.path.isdir("static"):
    app.mount("/static", StaticFiles(directory="static"), name="static")


# ================= DATABASE =================
def init_db():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        wallet REAL
    )""")
    conn.commit()
    return conn


DB_CONN = init_db()
DB_LOCK = threading.Lock()


def set_wallet(user_id: int, amount: float):
    with DB_LOCK:
        cur = DB_CONN.cursor()
        cur.execute("INSERT OR REPLACE INTO users (user_id, wallet) VALUES (?, ?)", (user_id, amount))
        DB_CONN.commit()


def get_wallet(user_id: int):
    with DB_LOCK:
        cur = DB_CONN.cursor()
        cur.execute("SELECT wallet FROM users WHERE user_id=?", (user_id,))
        row = cur.fetchone()
        return row[0] if row else None


# ================= HELPERS =================
def safe_float(x):
    try:
        return float(x) if x is not None else None
    except (ValueError, TypeError):
        return None


def pct(a, b):
    try:
        lo = min(a, b)
        if lo <= 0: return None
        return abs(a - b) / lo * 100.0
    except (ValueError, TypeError):
        return None


def next_funding_time():
    now = datetime.utcnow()
    hours = [0, 8, 16]
    for h in hours:
        t = now.replace(hour=h, minute=0, second=0, microsecond=0)
        if t > now:
            return t
    return (now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1))


EXCHANGES_MAP = {}


def init_exchange(ex_id: str):
    if ex_id in EXCHANGES_MAP:
        return EXCHANGES_MAP[ex_id]
    try:
        cls = getattr(ccxt, ex_id)
        ex = cls({"enableRateLimit": True, "options": {"defaultType": "swap"}})
        EXCHANGES_MAP[ex_id] = ex
        return ex
    except Exception as e:
        print(f"Failed to init exchange {ex_id}: {e}")
        return None


def load_futures_markets(ex):
    base_map = {}
    symbols = []
    try:
        markets = ex.load_markets()
        for m in markets.values():
            if m.get("swap") and m.get("quote") == QUOTE and m.get("active"):
                base = (m.get("base") or "").upper()
                symbol = m.get("symbol")
                if base and symbol:
                    symbols.append(symbol)
                    if base not in base_map:
                        base_map[base] = m
    except Exception as e:
        print(f"Failed to load markets for {ex.id}: {e}")
    return {"base_map": base_map, "symbols": symbols}


# ИЗМЕНЕНО: Функция теперь возвращает bid/ask цены
def fetch_prices(ex, symbols):
    prices = {}
    volumes = {}
    try:
        tickers = ex.fetch_tickers(symbols)
        for symbol, t in tickers.items():
            base = t['symbol'].split('/')[0]
            bid = safe_float(t.get('bid'))
            ask = safe_float(t.get('ask'))
            vol = safe_float(t.get('quoteVolume')) or 0

            if bid and ask:
                prices[base] = {'bid': bid, 'ask': ask}
                volumes[base] = vol
    except Exception as e:
        print(f"Could not fetch tickers for {ex.id}: {e}")
    return prices, volumes


# ИЗМЕНЕНО: Функция теперь принимает реальные ставки фандинга
def calculate_spreads(signal):
    price_a = signal.get('price_a', {})
    price_b = signal.get('price_b', {})

    # Мы продаем (short) на бирже A, значит смотрим на цену bid (покупки)
    # Мы покупаем (long) на бирже B, значит смотрим на цену ask (продажи)
    short_price = price_a.get('bid')
    long_price = price_b.get('ask')

    funding_a = signal.get('funding_a', 0)
    funding_b = signal.get('funding_b', 0)

    if not short_price or not long_price:
        return None

    # Основной спред между ценой продажи и покупки
    spread = ((short_price - long_price) / long_price) * 100

    funding_spread = (funding_a - funding_b) * 100
    total_commission_pct = COMMISSION * 2 * 100

    # Спред входа = основной спред - комиссия
    entry_spread = spread - total_commission_pct
    # Спред выхода (предполагаем симметричную ситуацию)
    exit_spread = -spread - total_commission_pct

    return {
        **signal,
        'spread': spread,
        'funding_spread': funding_spread,
        'entry_spread': entry_spread,
        'exit_spread': exit_spread,
        'total_commission': total_commission_pct,
    }


# ================= BACKGROUND FETCHER =================
def fetcher_loop():
    global SIGNALS
    print("[fetcher] starting background fetcher")
    markets_info = {}
    for ex_id in EX_IDS:
        ex = init_exchange(ex_id)
        if ex:
            markets_info[ex_id] = load_futures_markets(ex)

    while True:
        try:
            local_signals = []
            prices_cache = {}
            volumes_cache = {}
            funding_cache = {}

            # Шаг 1: Получаем все цены и объемы
            for ex_id in EX_IDS:
                if ex_id in markets_info:
                    p, v = fetch_prices(EXCHANGES_MAP[ex_id], markets_info[ex_id]["symbols"])
                    prices_cache[ex_id] = p
                    volumes_cache[ex_id] = v

            # Шаг 2: Получаем все ставки фандинга
            for ex_id in EX_IDS:
                if ex_id in markets_info:
                    try:
                        rates = EXCHANGES_MAP[ex_id].fetch_funding_rates(markets_info[ex_id]["symbols"])
                        funding_cache[ex_id] = {rate['symbol'].split('/')[0]: safe_float(rate.get('fundingRate')) for
                                                rate in rates.values()}
                    except Exception:
                        funding_cache[ex_id] = {}

            ex_ids = list(EXCHANGES_MAP.keys())
            for i in range(len(ex_ids)):
                for j in range(i + 1, len(ex_ids)):
                    a, b = ex_ids[i], ex_ids[j]
                    if a not in prices_cache or b not in prices_cache: continue

                    commons = set(prices_cache[a].keys()) & set(prices_cache[b].keys())
                    for base in commons:
                        pa = prices_cache[a].get(base)
                        pb = prices_cache[b].get(base)
                        if not pa or not pb or not pa.get('bid') or not pb.get('ask'): continue

                        # Расчет спреда для A(short) -> B(long)
                        sp = pct(pa.get('bid'), pb.get('ask'))
                        if sp is None or not (SPREAD_THRESHOLD_MIN <= sp <= SPREAD_THRESHOLD_MAX):
                            continue

                        va = volumes_cache.get(a, {}).get(base, 0)
                        vb = volumes_cache.get(b, {}).get(base, 0)
                        fa = funding_cache.get(a, {}).get(base)
                        fb = funding_cache.get(b, {}).get(base)

                        if fa is None or fb is None: continue

                        signal_data = {
                            "pair": f"{base}/{QUOTE}",
                            "ex_a": a, "ex_b": b,
                            "price_a": pa, "price_b": pb,
                            "vol_a": va, "vol_b": vb,
                            "funding_a": fa, "funding_b": fb,
                            "next_funding": next_funding_time().strftime("%Y-%m-%d %H:%M UTC")
                        }

                        full_signal = calculate_spreads(signal_data)
                        if full_signal:
                            local_signals.append(full_signal)

            local_signals.sort(key=lambda x: -x["spread"])
            with SIGNALS_LOCK:
                SIGNALS = local_signals[:200]

            if SIGNALS:
                print(f"[fetcher] {len(SIGNALS)} signals; top: {SIGNALS[0]['pair']} {SIGNALS[0]['spread']:.2f}%")
            else:
                print("[fetcher] no signals found")
        except Exception as e:
            print(f"[fetcher] error: {e}", exc_info=True)
        time.sleep(FETCH_INTERVAL)


# ================= TELEGRAM BOT =================
TELE_BOT_APP = Application.builder().token(TELEGRAM_BOT_TOKEN).build()


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = PUBLIC_URL.rstrip("/")
    kb = [[InlineKeyboardButton("📊 Open MiniApp", web_app=WebAppInfo(url=url))]]
    await update.message.reply_text("👋 Welcome! Open the MiniApp to see current signals.",
                                    reply_markup=InlineKeyboardMarkup(kb))


async def cmd_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("💰 Send the wallet amount in USD. Example: `200`", parse_mode="Markdown")


async def wallet_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    try:
        val = float(update.message.text.strip())
        set_wallet(user_id, val)
        await update.message.reply_text(f"✅ Wallet set to ${val:.2f}")
    except Exception:
        await update.message.reply_text("❌ Could not parse amount. Send a number, e.g. `200`", parse_mode="Markdown")


# ================= FASTAPI ROUTES =================
@app.get("/", response_class=HTMLResponse)
async def miniapp(request: Request):
    return templates.TemplateResponse("miniapp.html", {"request": request})


@app.get("/api/signals")
async def api_signals(request: Request):
    limit = int(request.query_params.get("limit", "50"))
    with SIGNALS_LOCK:
        return JSONResponse(SIGNALS[:limit])


# НОВЫЙ МАРШРУТ для получения исторических данных для графика
@app.get("/api/ohlcv")
async def api_ohlcv(request: Request):
    try:
        symbol = request.query_params.get("symbol")
        exchange_id = request.query_params.get("exchange_id")
        timeframe = request.query_params.get("timeframe", "1h")
        limit = int(request.query_params.get("limit", "100"))

        if not symbol or not exchange_id:
            raise HTTPException(status_code=400, detail="Symbol and exchange_id are required")

        ex = init_exchange(exchange_id)
        if not ex or not ex.has.get('fetchOHLCV'):
            raise HTTPException(status_code=404, detail="Exchange not found or doesn't support OHLCV")

        ohlcv = ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        return JSONResponse(ohlcv)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/health")
async def health():
    return {"status": "ok", "signals": len(SIGNALS)}


# ================= TELEGRAM WEBHOOK =================
@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    data = await request.json()
    update = Update.de_json(data, TELE_BOT_APP.bot)
    await TELE_BOT_APP.process_update(update)
    return {"ok": True}


# ================= STARTUP =================
@app.on_event("startup")
async def startup_event():
    await TELE_BOT_APP.initialize()
    TELE_BOT_APP.add_handler(CommandHandler("start", cmd_start))
    TELE_BOT_APP.add_handler(CommandHandler("wallet", cmd_wallet))
    TELE_BOT_APP.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), wallet_text_handler))

    threading.Thread(target=fetcher_loop, daemon=True).start()

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setWebhook"
    resp = requests.post(url, data={"url": WEBHOOK_URL})
    print("[telegram] setWebhook:", resp.json())
    print(f"[app] startup done; fetcher started, webhook registered at {WEBHOOK_URL}")