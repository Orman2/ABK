import os
import threading
import time
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, Any, List

import ccxt
import requests
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo, Update
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters

# ================= CONFIG =================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
PUBLIC_URL = os.environ.get("PUBLIC_URL", "")  # например https://arb-bot.up.railway.app

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
FETCH_INTERVAL = 20

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
        ex = cls({"enableRateLimit": True, "options":{"defaultType":"swap"}})
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

def fetch_prices(ex, base_map, symbols):
    result = {}
    volumes = {}
    try:
        tickers = ex.fetch_tickers(symbols)
    except Exception:
        tickers = {}
    for base, m in base_map.items():
        sym = m.get("symbol")
        t = tickers.get(sym) if sym in tickers else None
        if not t:
            continue
        last = safe_float(t.get("last")) or safe_float(t.get("close"))
        vol = safe_float(t.get("quoteVolume")) or safe_float(t.get("baseVolume")) or 0
        if last:
            result[base] = last
            volumes[base] = vol
    return result, volumes

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
            volumes_cache = {}
            for ex_id, ex in exchanges_map.items():
                p, v = fetch_prices(ex, markets_info[ex_id]["base_map"], markets_info[ex_id]["symbols"])
                prices_cache[ex_id] = p
                volumes_cache[ex_id] = v

            ex_ids = list(exchanges_map.keys())
            for i in range(len(ex_ids)):
                for j in range(i+1, len(ex_ids)):
                    a, b = ex_ids[i], ex_ids[j]
                    commons = set(markets_info[a]["base_map"].keys()) & set(markets_info[b]["base_map"].keys())
                    for base in commons:
                        ma = markets_info[a]["base_map"].get(base)
                        mb = markets_info[b]["base_map"].get(base)
                        if not ma or not mb: continue
                        if ma.get("quote") != QUOTE or mb.get("quote") != QUOTE: continue
                        pa = prices_cache[a].get(base)
                        pb = prices_cache[b].get(base)
                        va = volumes_cache[a].get(base, 0)
                        vb = volumes_cache[b].get(base, 0)
                        if not pa or not pb: continue
                        sp = pct(pa, pb)
                        if sp is None: continue
                        if sp < SPREAD_THRESHOLD_MIN or sp > SPREAD_THRESHOLD_MAX: continue
                        next_fund = next_funding_time()
                        local_signals.append({
                            "pair": f"{base}/{QUOTE}",
                            "ex_a": a,
                            "ex_b": b,
                            "price_a": pa,
                            "price_b": pb,
                            "vol_a": va,
                            "vol_b": vb,
                            "spread": sp,
                            "next_funding": next_fund.strftime("%Y-%m-%d %H:%M UTC")
                        })
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
    await update.message.reply_text("👋 Welcome! Open the MiniApp to see current signals.", reply_markup=InlineKeyboardMarkup(kb))

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

TELE_BOT_APP.add_handler(CommandHandler("start", cmd_start))
TELE_BOT_APP.add_handler(CommandHandler("wallet", cmd_wallet))
TELE_BOT_APP.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), wallet_text_handler))

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
            price_diff = abs(s["price_a"] - s["price_b"])
            est = (price_diff / max(s["price_a"], s["price_b"])) * wallet - (wallet * COMMISSION * 2)
        o = dict(s)
        o["estimated_earn"] = est
        out.append(o)
    return JSONResponse(out)

@app.get("/health")
async def health():
    return {"status":"ok", "signals": len(SIGNALS)}

# ================= TELEGRAM WEBHOOK =================
@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    data = await request.json()
    await TELE_BOT_APP.update_queue.put(Update.de_json(data, TELE_BOT_APP.bot))
    return {"ok": True}

# ================= STARTUP =================
@app.on_event("startup")
def startup_event():
    threading.Thread(target=fetcher_loop, daemon=True).start()

    # Установить webhook при старте
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setWebhook"
    resp = requests.post(url, data={"url": WEBHOOK_URL})
    print("[telegram] setWebhook:", resp.json())

    print(f"[app] startup done; fetcher started, webhook registered at {WEBHOOK_URL}")

# ================= MAIN =================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app:app", host="0.0.0.0", port=int(os.getenv("PORT", 8000)))
