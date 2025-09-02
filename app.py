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
WS_EX_IDS = ["binance", "mexc", "bingx", "bybit", "gateio"]
CCXT_EX_IDS = ["lbank", "kucoin"]

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
EX_DATA = {ex_id: {'bids': {}, 'asks': {}, 'funding_rates': {}, 'funding_times': {}} for ex_id in EX_IDS}
EX_WS_STATUS = {ex_id: "active" for ex_id in WS_EX_IDS}


async def listen_ws(uri, ex_id, subscription_message=None):
    try:
        async with websockets.connect(uri) as websocket:
            print(f"[{ex_id}] Connection successful. Sending subscription message...")
            if subscription_message:
                await websocket.send(json.dumps(subscription_message))
            while True:
                message = await websocket.recv()
                data = json.loads(message)

                if ex_id == 'binance':
                    if isinstance(data, dict):
                        if data.get('e') == '24hrTicker':
                            handle_binance_ticker_data(data)
                        elif data.get('e') == 'fundingRate':
                            handle_binance_funding_data(data)
                elif ex_id == 'mexc':
                    if isinstance(data, dict) and 'channel' in data:
                        if data['channel'] == 'swap@ticker':
                            handle_mexc_ticker_data(data['data'])
                        elif data['channel'] == 'swap@funding_rate':
                            handle_mexc_funding_data(data['data'])
                elif ex_id == 'bingx':
                    if isinstance(data.get('data'), dict) and data.get('dataType') and data['dataType'].startswith(
                            'ticker'):
                        for ticker_data in data.get('data', {}).values():
                            handle_bingx_ticker_data(ticker_data)
                elif ex_id == 'bybit':
                    if isinstance(data.get('data'), list) and data.get('topic') and data['topic'].startswith('tickers'):
                        for ticker_data in data.get('data', []):
                            handle_bybit_ticker_data(ticker_data)
                elif ex_id == 'gateio':
                    if isinstance(data.get('result'), list) and data.get('channel') == 'futures.tickers':
                        handle_gateio_ticker_data(data.get('result', []))
    except Exception as e:
        print(f"[{ex_id}] Error connecting to websocket: {e}. Switching to CCXT for this exchange.")
        EX_WS_STATUS[ex_id] = "disabled"


def handle_binance_ticker_data(data):
    symbol = data['s'].replace('USDT', '')
    EX_DATA['binance']['bids'][symbol] = safe_float(data['b'])
    EX_DATA['binance']['asks'][symbol] = safe_float(data['a'])


def handle_binance_funding_data(data):
    symbol = data['s'].replace('USDT', '')
    EX_DATA['binance']['funding_rates'][symbol] = safe_float(data['r'])
    EX_DATA['binance']['funding_times'][symbol] = safe_float(data['T'])


def handle_mexc_ticker_data(data):
    for symbol, ticker_data in data.items():
        base = symbol.replace('_USDT', '').replace('USDT_', '').replace('USDT', '')
        if not base: continue
        EX_DATA['mexc']['bids'][base] = safe_float(ticker_data['bid'])
        EX_DATA['mexc']['asks'][base] = safe_float(ticker_data['ask'])


def handle_mexc_funding_data(data):
    for symbol, funding_data in data.items():
        base = symbol.replace('_USDT', '').replace('USDT_', '').replace('USDT', '')
        if not base: continue
        EX_DATA['mexc']['funding_rates'][base] = safe_float(funding_data['fundingRate'])
        EX_DATA['mexc']['funding_times'][base] = safe_float(funding_data['nextFundingTime'])


def handle_bingx_ticker_data(data):
    symbol = data['symbol'].replace('-USDT', '')
    EX_DATA['bingx']['bids'][symbol] = safe_float(data['bidPrice'])
    EX_DATA['bingx']['asks'][symbol] = safe_float(data['askPrice'])
    EX_DATA['bingx']['funding_rates'][symbol] = safe_float(data['fundingRate'])
    EX_DATA['bingx']['funding_times'][symbol] = safe_float(data['nextFundingTime'])


def handle_bybit_ticker_data(data):
    symbol = data['symbol'].replace('USDT', '')
    EX_DATA['bybit']['bids'][symbol] = safe_float(data['bidPrice'])
    EX_DATA['bybit']['asks'][symbol] = safe_float(data['askPrice'])
    EX_DATA['bybit']['funding_rates'][symbol] = safe_float(data['fundingRate'])
    EX_DATA['bybit']['funding_times'][symbol] = safe_float(data['nextFundingFeeTime'])


def handle_gateio_ticker_data(data_list):
    for ticker_data in data_list:
        symbol = ticker_data['contract'].replace('_USDT', '')
        EX_DATA['gateio']['bids'][symbol] = safe_float(ticker_data['highest_bid'])
        EX_DATA['gateio']['asks'][symbol] = safe_float(ticker_data['lowest_ask'])
        EX_DATA['gateio']['funding_rates'][symbol] = safe_float(ticker_data['funding_rate'])
        EX_DATA['gateio']['funding_times'][symbol] = safe_float(ticker_data['next_funding_rate_time'])


async def fetcher_loop():
    exchanges_map = {ex_id: init_exchange(ex_id) for ex_id in EX_IDS}
    markets_info = {ex_id: load_futures_markets(exchanges_map[ex_id]) for ex_id in exchanges_map}

    # Запускаем слушателей веб-сокетов
    ws_tasks = []

    # Binance
    ws_tasks.append(asyncio.create_task(listen_ws("wss://fstream.binance.com/ws/!ticker@arr", "binance")))
    ws_tasks.append(asyncio.create_task(listen_ws("wss://fstream.binance.com/ws/!fundingRate@arr", "binance")))

    # MEXC
    ws_tasks.append(asyncio.create_task(listen_ws("wss://contract.mexc.com/ws", "mexc", {"method": "subscription",
                                                                                         "params": ["swap@ticker@all",
                                                                                                    "swap@funding_rate@all"]})))

    # BingX
    ws_tasks.append(asyncio.create_task(listen_ws("wss://fstream-ws.bingx.com/ws/V1_ticker", "bingx",
                                                  {"id": "test", "reqType": "sub", "dataType": "ticker.all"})))

    # Bybit
    bybit_symbols = [s.replace('USDT', '') for s in markets_info['bybit']['symbols']]
    bybit_topics = [f"tickers.{s}USDT" for s in bybit_symbols]
    bybit_ws_uri = "wss://stream.bybit.com/v5/public/linear"
    bybit_subscription = {"op": "subscribe", "args": bybit_topics}
    ws_tasks.append(asyncio.create_task(listen_ws(bybit_ws_uri, "bybit", bybit_subscription)))

    # Gate.io
    gateio_symbols = [s.replace('_USDT', '') for s in markets_info['gateio']['symbols']]
    gateio_ws_uri = "wss://api.gateio.ws/ws/v4/"
    gateio_subscription = {"time": int(time.time()), "channel": "futures.tickers", "event": "subscribe",
                           "payload": [f"USDT_{s}" for s in gateio_symbols]}
    ws_tasks.append(asyncio.create_task(listen_ws(gateio_ws_uri, "gateio", gateio_subscription)))

    while True:
        try:
            # Сбор данных со всех источников
            all_ex_data = {}
            for ex_id in EX_IDS:
                if ex_id in WS_EX_IDS and EX_WS_STATUS.get(ex_id) == "active":
                    # Используем данные с веб-сокета
                    all_ex_data[ex_id] = EX_DATA.get(ex_id, {})
                else:
                    # Используем CCXT в качестве резерва или основного источника
                    ex = exchanges_map.get(ex_id)
                    info = markets_info.get(ex_id)
                    if ex and info:
                        p, b, a, v, f, t = fetch_market_data(ex, info["base_map"], info["symbols"])
                        all_ex_data[ex_id] = {'bids': b, 'asks': a, 'funding_rates': f, 'funding_times': t}

            # Логика для поиска арбитражных связок
            found_signals = []
            exchanges_list = list(all_ex_data.keys())

            for i in range(len(exchanges_list)):
                for j in range(i + 1, len(exchanges_list)):
                    ex_a_id = exchanges_list[i]
                    ex_b_id = exchanges_list[j]

                    data_a = all_ex_data.get(ex_a_id)
                    data_b = all_ex_data.get(ex_b_id)

                    if not data_a or not data_b: continue

                    common_bases = set(data_a.get('bids', {}).keys()) & set(data_b.get('bids', {}).keys())

                    for base in common_bases:
                        ask_a = data_a.get('asks', {}).get(base)
                        bid_b = data_b.get('bids', {}).get(base)

                        if ask_a and bid_b:
                            spread_ab = pct(ask_a, bid_b)
                            if spread_ab and SPREAD_THRESHOLD_MIN <= spread_ab <= SPREAD_THRESHOLD_MAX:
                                signal_ab = {
                                    'base': base,
                                    'exchange_a': ex_a_id,
                                    'exchange_b': ex_b_id,
                                    'ask_a': ask_a,
                                    'bid_b': bid_b,
                                    'spread': spread_ab,
                                    'direction': f"{ex_a_id} -> {ex_b_id}",
                                    'funding_a': data_a.get('funding_rates', {}).get(base),
                                    'funding_b': data_b.get('funding_rates', {}).get(base),
                                }
                                found_signals.append(calculate_spreads(signal_ab))

                        ask_b = data_b.get('asks', {}).get(base)
                        bid_a = data_a.get('bids', {}).get(base)

                        if ask_b and bid_a:
                            spread_ba = pct(ask_b, bid_a)
                            if spread_ba and SPREAD_THRESHOLD_MIN <= spread_ba <= SPREAD_THRESHOLD_MAX:
                                signal_ba = {
                                    'base': base,
                                    'exchange_a': ex_b_id,
                                    'exchange_b': ex_a_id,
                                    'ask_a': ask_b,
                                    'bid_b': bid_a,
                                    'spread': spread_ba,
                                    'direction': f"{ex_b_id} -> {ex_a_id}",
                                    'funding_a': data_b.get('funding_rates', {}).get(base),
                                    'funding_b': data_a.get('funding_rates', {}).get(base),
                                }
                                found_signals.append(calculate_spreads(signal_ba))

            with SIGNALS_LOCK:
                SIGNALS.clear()
                SIGNALS.extend(sorted(found_signals, key=lambda x: x['spread'], reverse=True))

            print(f"Total signals found: {len(SIGNALS)}")

        except Exception as e:
            print(f"[fetcher] error: {e}")

        await asyncio.sleep(FETCH_INTERVAL)


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

    asyncio.create_task(fetcher_loop())

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