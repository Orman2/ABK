import os
import threading
import time
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, Any, List
import asyncio
import websockets
import json
import ccxt
import requests
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo, Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

# ================= CONFIG =================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
PUBLIC_URL = os.environ.get("PUBLIC_URL", "")

if not TELEGRAM_BOT_TOKEN or not PUBLIC_URL:
    raise RuntimeError("❌ Set TELEGRAM_BOT_TOKEN and PUBLIC_URL environment variables!")

WEBHOOK_PATH = f"/webhook/{TELEGRAM_BOT_TOKEN}"
WEBHOOK_URL = PUBLIC_URL.rstrip("/") + WEBHOOK_PATH

EX_IDS = ["binance", "mexc", "bingx", "gateio", "bybit", "lbank", "kucoin"]
QUOTE = "USDT"
SPREAD_THRESHOLD_MIN = 1.0
SPREAD_THRESHOLD_MAX = 50.0
COMMISSION = 0.0005
DB_FILE = "miniapp.db"
FETCH_INTERVAL = 1.0

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
    cur.execute("""
    CREATE TABLE IF NOT EXISTS tracking (
        user_id INTEGER,
        exchange TEXT,
        funding_time TEXT,
        PRIMARY KEY (user_id, exchange)
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
    except:
        return None


def pct(a, b):
    try:
        lo = min(a, b)
        if lo <= 0:
            return None
        return abs(a - b) / lo * 100.0
    except:
        return None


def next_funding_time():
    now = datetime.utcnow()
    hours = [0, 8, 16]
    for h in hours:
        t = now.replace(hour=h, minute=0, second=0, microsecond=0)
        if t > now:
            return t
    return (now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1))


def init_exchange(ex_id: str):
    try:
        cls = getattr(ccxt, ex_id)
        ex = cls({"enableRateLimit": True, "options": {"defaultType": "swap"}})
        try:
            ex.load_markets()
        except Exception:
            pass
        return ex
    except Exception:
        return None


def load_futures_markets(ex):
    base_map = {}
    symbols = []
    try:
        markets = ex.load_markets()
    except Exception:
        return {"base_map": {}, "symbols": []}
    for m in markets.values():
        if not m.get("swap") or m.get("quote") != QUOTE or not m.get("active"):
            continue
        base = (m.get("base") or "").upper()
        symbol = m.get("symbol")
        if base and symbol:
            symbols.append(symbol)
            if base not in base_map:
                base_map[base] = m
    return {"base_map": base_map, "symbols": symbols}


def fetch_tickers(ex, symbols):
    try:
        return ex.fetch_tickers(symbols)
    except Exception:
        return {}


def fetch_market_data(ex, base_map, symbols):
    prices = {}
    bids = {}
    asks = {}
    volumes = {}
    funding_rates = {}
    funding_times = {}

    tickers = fetch_tickers(ex, symbols)

    for base, m in base_map.items():
        sym = m.get("symbol")
        t = tickers.get(sym)
        if not t:
            continue

        last = safe_float(t.get("last")) or safe_float(t.get("close"))
        bid = safe_float(t.get("bid"))
        ask = safe_float(t.get("ask"))
        vol = safe_float(t.get("quoteVolume")) or safe_float(t.get("baseVolume")) or 0

        funding_rate = safe_float(t.get("fundingRate"))
        next_funding_time_ms = safe_float(t.get("info", {}).get("nextFundingTime"))

        if funding_rate is None:
            funding_rate = None

        if next_funding_time_ms is None:
            next_funding_time_ms = next_funding_time().timestamp() * 1000

        prices[base] = last
        bids[base] = bid
        asks[base] = ask
        volumes[base] = vol
        funding_rates[base] = funding_rate
        funding_times[base] = next_funding_time_ms

    return prices, bids, asks, volumes, funding_rates, funding_times


def calculate_spreads(signal):
    funding_a = signal.get('funding_a')
    funding_b = signal.get('funding_b')

    funding_a_pct = funding_a * 100 if funding_a is not None else None
    funding_b_pct = funding_b * 100 if funding_b is not None else None

    price_a = signal.get('ask_a', 0)
    price_b = signal.get('bid_b', 0)

    if price_a > 0:
        spread = ((price_b - price_a) / price_a) * 100
    else:
        spread = 0

    funding_spread = None
    if funding_a_pct is not None and funding_b_pct is not None:
        funding_spread = funding_b_pct - funding_a_pct

    entry_spread = spread - (COMMISSION * 2 * 100)
    exit_spread = spread * 0.9 - (COMMISSION * 2 * 100)
    total_commission = COMMISSION * 2 * 100

    return {
        **signal,
        'funding_a': funding_a_pct,
        'funding_b': funding_b_pct,
        'funding_spread': funding_spread,
        'entry_spread': entry_spread,
        'exit_spread': exit_spread,
        'total_commission': total_commission
    }


# ================= WEBSOCKET FETCHER =================
BINANCE_PRICES = {}
BINANCE_FUNDING = {}
MEXC_PRICES = {}
MEXC_FUNDING = {}


async def binance_ws_listener():
    uri_ticker = "wss://fstream.binance.com/ws/!ticker@arr"
    uri_funding = "wss://fstream.binance.com/ws/!fundingRate@arr"

    ticker_task = asyncio.create_task(listen_ws(uri_ticker, 'binance_ticker'))
    funding_task = asyncio.create_task(listen_ws(uri_funding, 'binance_funding'))

    await asyncio.gather(ticker_task, funding_task)


async def mexc_ws_listener():
    # URL для публичных данных MEXC - исправлен
    uri = "wss://contract.mexc.com/ws"

    async for ws in websockets.connect(uri):
        try:
            # Подписываемся на все тикеры (цены) и фандинг
            subscribe_message = {
                "method": "subscription",
                "params": [
                    "spot@ticker@all",
                    "swap@ticker@all",
                    "swap@funding_rate@all"
                ]
            }
            await ws.send(json.dumps(subscribe_message))

            while True:
                message = await ws.recv()
                data = json.loads(message)

                # Обработка данных
                if 'channel' in data and data['channel'] == 'swap@ticker':
                    handle_mexc_ticker_data(data['data'])
                elif 'channel' in data and data['channel'] == 'swap@funding_rate':
                    handle_mexc_funding_data(data['data'])

        except websockets.ConnectionClosed:
            print(f"Connection to MEXC closed. Reconnecting...")
            continue
        except Exception as e:
            print(f"Error in MEXC listener: {e}")
            continue


def handle_mexc_ticker_data(data):
    for symbol, ticker_data in data.items():
        base = symbol.replace('_USDT', '').replace('USDT_', '').replace('USDT', '')
        if not base:
            continue

        MEXC_PRICES[base] = {
            'bid': float(ticker_data['bid']),
            'ask': float(ticker_data['ask']),
            'volume': float(ticker_data['volume24'])
        }


def handle_mexc_funding_data(data):
    for symbol, funding_data in data.items():
        base = symbol.replace('_USDT', '').replace('USDT_', '').replace('USDT', '')
        if not base:
            continue

        MEXC_FUNDING[base] = {
            'rate': float(funding_data['fundingRate']),
            'time': float(funding_data['nextFundingTime'])
        }


async def listen_ws(uri, stream_type):
    async for websocket in websockets.connect(uri):
        try:
            while True:
                message = await websocket.recv()
                data = json.loads(message)

                if stream_type == 'binance_ticker':
                    handle_binance_ticker_data(data)
                elif stream_type == 'binance_funding':
                    handle_binance_funding_data(data)

        except websockets.ConnectionClosed:
            print(f"Connection to {uri} closed. Reconnecting...")
            continue
        except Exception as e:
            print(f"Error in {uri} listener: {e}")
            continue


def handle_binance_ticker_data(data):
    if not isinstance(data, list):
        return
    for d in data:
        symbol = d['s'].replace('USDT', '')
        BINANCE_PRICES[symbol] = {
            'bid': float(d['b']),
            'ask': float(d['a']),
            'volume': float(d['q'])
        }


def handle_binance_funding_data(data):
    if not isinstance(data, list):
        return
    for d in data:
        symbol = d['s'].replace('USDT', '')
        BINANCE_FUNDING[symbol] = {
            'rate': float(d['r']),
            'time': float(d['T'])
        }


async def fetcher_loop_websocket():
    # Запускаем слушателей веб-сокетов для всех бирж
    asyncio.create_task(binance_ws_listener())
    asyncio.create_task(mexc_ws_listener())

    # Используем CCXT для бирж, которые пока не поддерживают веб-сокеты в нашем коде
    exchanges_map = {ex_id: init_exchange(ex_id) for ex_id in EX_IDS if ex_id not in ['binance', 'mexc']}
    markets_info = {ex_id: load_futures_markets(exchanges_map[ex_id]) for ex_id in exchanges_map}

    while True:
        try:
            local_signals = []

            # Данные с Binance и MEXC получаем из кэша
            binance_data = {'prices': BINANCE_PRICES, 'funding': BINANCE_FUNDING}
            mexc_data = {'prices': MEXC_PRICES, 'funding': MEXC_FUNDING}

            # Далее вам нужно будет реализовать логику для поиска связок между всеми биржами.
            # Например, между Binance и MEXC

            # commons = set(binance_data['prices'].keys()) & set(mexc_data['prices'].keys())
            # for base in commons:
            #     ask_a = binance_data['prices'][base]['ask']
            #     bid_b = mexc_data['prices'][base]['bid']
            #     ...

            # и так далее для всех пар бирж.

            print(f"Binance Tickers: {len(BINANCE_PRICES)}, MEXC Tickers: {len(MEXC_PRICES)}")

            await asyncio.sleep(1)
        except Exception as e:
            print(f"[fetcher] error: {e}")
            await asyncio.sleep(5)


# ================= TELEGRAM BOT =================
TELE_BOT_APP = Application.builder().token(TELEGRAM_BOT_TOKEN).build()


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    url = PUBLIC_URL.rstrip("/") + "/miniapp"
    kb = [[InlineKeyboardButton("📊 Open MiniApp", web_app=WebAppInfo(url=url))]]
    await update.message.reply_text("👋 Здравствуйте!"

                                    " Arb-bot - арбитражный бот, который показывает спред и готовые связки для арбитража фьючерсов, курсового спреда и фандинга..",
                                    reply_markup=InlineKeyboardMarkup(kb))


async def cmd_wallet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("💰 Send the wallet amount in USD. Example: `200`", parse_mode="Markdown")


async def wallet_text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = update.message.from_user.id
    txt = update.message.text.strip()
    try:
        val = float(txt)
        set_wallet(user_id, val)
        await update.message.reply_text(f"✅ Wallet set to ${val:.2f}")
    except Exception:
        await update.message.reply_text("❌ Could not parse amount. Send a number, e.g. `200`", parse_mode="Markdown")


# ================= FASTAPI ROUTES =================
@app.get("/miniapp", response_class=HTMLResponse)
async def miniapp(request: Request):
    return templates.TemplateResponse("miniapp.html", {"request": request})


@app.get("/api/signals")
async def api_signals(request: Request):
    limit = int(request.query_params.get("limit", "50"))
    user_id = request.query_params.get("user_id")
    wallet = get_wallet(int(user_id)) if user_id else None
    out = []
    with SIGNALS_LOCK:
        arr = SIGNALS[:limit]
    for s in arr:
        est = None
        if wallet:
            est = (s.get("spread", 0) / 100) * wallet
        o = dict(s)
        o["estimated_earn"] = est
        out.append(o)
    return JSONResponse(out)


@app.get("/health")
async def health():
    return {"status": "ok", "signals": len(SIGNALS)}


# ================= TELEGRAM WEBHOOK =================
@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    secret_token = os.environ.get("TELEGRAM_WEBHOOK_SECRET")
    if secret_token and request.headers.get("x-telegram-bot-api-secret-token") != secret_token:
        raise HTTPException(status_code=403, detail="Forbidden")

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

    asyncio.create_task(fetcher_loop_websocket())

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setWebhook"
    secret_token = os.environ.get("TELEGRAM_WEBHOOK_SECRET")
    resp = requests.post(
        url,
        data={
            "url": WEBHOOK_URL,
            "secret_token": secret_token
        }
    )
    print("[telegram] setWebhook:", resp.json())
    print(f"[app] startup done; fetcher started, webhook registered at {WEBHOOK_URL}")


# ================= MAIN =================
if __name__ == "__main__":
    import uvicorn

    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)))