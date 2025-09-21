import os
import threading
import time
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import ccxt
import requests
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, WebAppInfo, Update, Message
from telegram.ext import Application, CommandHandler, ContextTypes, MessageHandler, filters, ExtBot
from pydantic import BaseModel
import asyncio

# ==================== ИМПОРТ НОВЫХ МОДУЛЕЙ ====================
# ! ВАЖНО: Убедитесь, что эти файлы находятся в той же директории
from arb_analyzer import ArbAnalyzer, GLOBAL_ARB_DATA
from database import get_settings, create_db, save_settings

# =============================================================

# ================= CONFIG =================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
PUBLIC_URL = os.environ.get("PUBLIC_URL", "")

if not TELEGRAM_BOT_TOKEN or not PUBLIC_URL:
    raise RuntimeError("❌ Set TELEGRAM_BOT_TOKEN and PUBLIC_URL environment variables!")

WEBHOOK_PATH = f"/webhook/{TELEGRAM_BOT_TOKEN}"
WEBHOOK_URL = PUBLIC_URL.rstrip("/") + WEBHOOK_PATH
WEBAPP_BASE_URL = PUBLIC_URL.rstrip("/") + "/webapp"
FETCH_INTERVAL = 3.0
PORT = int(os.environ.get('PORT', 8080))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

# ==================== FASTAPI & TELEGRAM INIT ====================
app = FastAPI()


class CustomBot(ExtBot):
    pass


TELE_BOT_APP = (
    Application.builder()
    .token(TELEGRAM_BOT_TOKEN)
    .updater(None)
    .bot(CustomBot)  # ИСПРАВЛЕНО
    .build()
)

templates = Jinja2Templates(directory=".")

arb_thread: Optional[ArbAnalyzer] = None


# ==================== СХЕМЫ ДАННЫХ ДЛЯ API ====================
class SettingsData(BaseModel):
    user_id: int
    exchanges: List[str]
    blacklist: List[str]
    min_spread: float
    min_funding_spread: float


# ==================== TELEGRAM HANDLERS ====================

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message or not update.message.from_user:
        return

    user_id = update.message.from_user.id

    # URL для открытия Web App (miniapp.html)
    webapp_url = f"{WEBAPP_BASE_URL}?user_id={user_id}"

    keyboard = [
        [InlineKeyboardButton("📊 Открыть Арбитраж Радар", web_app=WebAppInfo(url=webapp_url))]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    await update.message.reply_text(
        "👋 Добро пожаловать!\n\nИспользуйте кнопку ниже, чтобы открыть арбитражный радар.",
        reply_markup=reply_markup
    )


async def echo(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        await update.message.reply_text(update.message.text)


# ==================== WEB APP (HTML) ENDPOINTS ====================

@app.get("/webapp", response_class=HTMLResponse)
async def webapp_html(request: Request):
    return templates.TemplateResponse("miniapp.html", {"request": request})


# ==================== API ENDPOINTS (Для JS в Web App) ====================

# 1. API для получения актуальных арбитражных связок
@app.get("/api/get_arb_data", response_class=JSONResponse)
async def api_get_arb_data(user_id: int):
    """Возвращает актуальный список связок с учетом фильтров пользователя."""

    settings = get_settings(user_id)
    all_spreads = GLOBAL_ARB_DATA.get('latest_spreads', [])

    filtered_spreads = []

    for s in all_spreads:
        # Фильтр по минимальному спреду
        if s['net_spread'] * 100 < settings['min_spread']:
            continue

        # Фильтр по черному списку монет
        if s['coin'] in settings['blacklist']:
            continue

        # Фильтр по выбранным биржам (упрощенный, нужен хотя бы один)
        is_exchange_ok = not settings['exchanges'] or \
                         s['long_exchange'] in settings['exchanges'] or \
                         s['short_exchange'] in settings['exchanges']

        if is_exchange_ok:
            filtered_spreads.append(s)

    return {"spreads": filtered_spreads}


# 2. API для получения текущих настроек пользователя
@app.get("/api/get_user_settings/{user_id}", response_class=JSONResponse)
async def api_get_user_settings(user_id: int):
    """Возвращает настройки пользователя из БД для заполнения формы в Web App."""
    settings = get_settings(user_id)
    return JSONResponse({
        "settings": settings,
        "supported_exchanges": ArbAnalyzer.SUPPORTED_EXCHANGES
    })


# 3. API для сохранения настроек пользователя (обработка POST из Web App)
@app.post("/api/save_settings", response_class=JSONResponse)
async def api_save_user_settings(data: SettingsData):
    """Сохраняет настройки пользователя, полученные из Web App, в БД."""
    try:
        settings_dict = {
            "exchanges": data.exchanges,
            # Обработка ввода монет через пробел
            "blacklist": data.blacklist[0].upper().split() if data.blacklist and data.blacklist[0] else [],
            "min_spread": data.min_spread,
            "min_funding_spread": data.min_funding_spread,
        }

        save_settings(data.user_id, settings_dict)
        return {"status": "success", "message": "Настройки успешно сохранены!"}
    except Exception as e:
        logging.error(f"Ошибка сохранения настроек: {e}")
        raise HTTPException(status_code=500, detail="Ошибка при сохранении настроек.")


# ==================== WEBHOOK & STARTUP ====================

@app.post(WEBHOOK_PATH)
async def telegram_webhook(request: Request):
    data = await request.json()
    update = Update.de_json(data, TELE_BOT_APP.bot)
    await TELE_BOT_APP.process_update(update)

    return {"ok": True}


@app.on_event("startup")
async def startup_event():
    global arb_thread

    create_db()

    arb_thread = ArbAnalyzer(update_interval=FETCH_INTERVAL)
    arb_thread.start()
    logging.info("Арбитражный анализатор запущен.")

    await TELE_BOT_APP.initialize()
    TELE_BOT_APP.add_handler(CommandHandler("start", cmd_start))
    TELE_BOT_APP.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), echo))

    await TELE_BOT_APP.bot.set_webhook(url=WEBHOOK_URL)
    logging.info(f"Webhook установлен на: {WEBHOOK_URL}")


@app.on_event("shutdown")
async def shutdown_event():
    await TELE_BOT_APP.bot.delete_webhook()
    logging.info("Webhook удален.")

    if arb_thread:
        # Остановка потока анализатора для чистого завершения
        arb_thread._stop_event.set()
        arb_thread.join()
        logging.info("Арбитражный анализатор остановлен.")