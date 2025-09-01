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
COMMISSION = 0.0005
DB_FILE = "miniapp.db"
FETCH_INTERVAL = 0.1  # Обновление каждые 0.1 секунды

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

        # Если время фандинга не предоставлено, используем нашу универсальную логику
        if next_funding_time_ms is None:
            next_funding_time_ms = next_funding_time().timestamp() * 1000

        prices[base] = last
        bids[base] = bid
        asks[base] = ask
        volumes[base] = vol
        funding_rates[base] = funding_rate if funding_rate is not None else 0.0
        funding_times[base] = next_funding_time_ms

    return prices, bids, asks, volumes, funding_rates, funding_times


def calculate_spreads(signal):
    # Теперь funding_a и funding_b это десятичные дроби
    funding_a = float(signal.get('funding_a', 0))
    funding_b = float(signal.get('funding_b', 0))

    # Расчет в процентах происходит здесь, перед отображением
    funding_a_pct = funding_a * 100
    funding_b_pct = funding_b * 100

    price_a = signal.get('ask_a', 0)
    price_b = signal.get('bid_b', 0)

    if price_a > 0:
        spread = ((price_b - price_a) / price_a) * 100
    else:
        spread = 0

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


# ================= BACKGROUND FETCHER =================
def fetcher_loop():
    global SIGNALS
    print("[fetcher] starting background fetcher")
    exchanges_map = {}
    markets_info = {}
    for ex_id in EX_IDS:
        ex = init_exchange(ex_id)
        if ex:
            exchanges_map[ex_id] = ex
            markets_info[ex_id] = load_futures_markets(ex)

    while True:
        try:
            local_signals = []
            prices_cache = {}
            bids_cache = {}
            asks_cache = {}
            volumes_cache = {}
            funding_rates_cache = {}
            funding_times_cache = {}

            for ex_id, ex in exchanges_map.items():
                p, b, a, v, f, t = fetch_market_data(ex, markets_info[ex_id]["base_map"],
                                                     markets_info[ex_id]["symbols"])
                prices_cache[ex_id] = p
                bids_cache[ex_id] = b
                asks_cache[ex_id] = a
                volumes_cache[ex_id] = v
                funding_rates_cache[ex_id] = f
                funding_times_cache[ex_id] = t

            ex_ids = list(exchanges_map.keys())
            for i in range(len(ex_ids)):
                for j in range(i + 1, len(ex_ids)):
                    a, b = ex_ids[i], ex_ids[j]
                    commons = set(markets_info[a]["base_map"].keys()) & set(markets_info[b]["base_map"].keys())
                    for base in commons:
                        ma = markets_info[a]["base_map"].get(base)
                        mb = markets_info[b]["base_map"].get(base)
                        if not ma or not mb: continue
                        if ma.get("quote") != QUOTE or mb.get("quote") != QUOTE: continue

                        bid_a = bids_cache[a].get(base)
                        ask_a = asks_cache[a].get(base)
                        bid_b = bids_cache[b].get(base)
                        ask_b = asks_cache[b].get(base)

                        vol_a = volumes_cache[a].get(base, 0)
                        vol_b = volumes_cache[b].get(base, 0)

                        funding_a = funding_rates_cache[a].get(base, 0)
                        funding_b = funding_rates_cache[b].get(base, 0)

                        next_funding_time_ms = funding_times_cache[a].get(base, 0)
                        # Рассчитываем время фандинга в формате UTC и передаем его
                        next_fund = datetime.utcfromtimestamp(next_funding_time_ms / 1000)

                        if not ask_a or not bid_b: continue

                        sp = pct(ask_a, bid_b)
                        if sp is None: continue
                        if sp < SPREAD_THRESHOLD_MIN or sp > SPREAD_THRESHOLD_MAX: continue

                        signal_data = {
                            "pair": f"{base}/{QUOTE}",
                            "ex_a": a,
                            "ex_b": b,
                            "bid_a": bid_a,
                            "ask_a": ask_a,
                            "bid_b": bid_b,
                            "ask_b": ask_b,
                            "vol_a": vol_a,
                            "vol_b": vol_b,
                            "funding_a": funding_a,  # Теперь десятичная дробь
                            "funding_b": funding_b,  # Теперь десятичная дробь
                            "spread": sp,
                            "next_funding": next_fund.isoformat()
                        }

                        full_signal = calculate_spreads(signal_data)
                        local_signals.append(full_signal)

            local_signals.sort(key=lambda x: -x["spread"])
            with SIGNALS_LOCK:
                SIGNALS = local_signals[:200]
            if SIGNALS:
                print(f"[fetcher] {len(SIGNALS)} signals; top: {SIGNALS[0]['pair']} {SIGNALS[0]['spread']:.2f}%")
            else:
                print("[fetcher] no signals found")
        except Exception as e:
            print("[fetcher] error:", e)
        time.sleep(FETCH_INTERVAL)


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

    threading.Thread(target=fetcher_loop, daemon=True).start()

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