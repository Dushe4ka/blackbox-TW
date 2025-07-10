# auth_tg.py

import os
import logging
import json
import redis
import asyncio
import requests
from celery import shared_task
from telethon import TelegramClient
import sys
from pathlib import Path

# Получаем путь к корню проекта (папка blackbox)
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Теперь импортируем SESSION_FILE из session_path
from session_path import SESSION_FILE

# Отладочная информация
print(f"DEBUG: SESSION_FILE в auth_TG.py = {SESSION_FILE}")
print(f"DEBUG: Файл сессии существует: {os.path.exists(SESSION_FILE + '.session')}")

# --- Настройка ---
# Если у вас есть свой логгер, используйте его. Иначе будет базовый.
try:
    from logger_config import setup_logger
    log = setup_logger("auth_TG")
except ImportError:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log = logging.getLogger(__name__)

# --- Конфигурация из переменных окружения ---
API_ID = os.getenv('API_ID')
API_HASH = os.getenv('API_HASH')
PHONE_NUMBER = os.getenv('PHONE_NUMBER')
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")

# --- Клиент Redis ---
# Убедитесь, что ваш Redis запущен по этому адресу
try:
    redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    redis_client.ping() # Проверяем соединение
    log.info("✅ Успешное подключение к Redis.")
except redis.exceptions.ConnectionError as e:
    log.error(f"❌ Не удалось подключиться к Redis: {e}. Проверьте, запущен ли Redis сервер.")
    # В реальном проекте здесь можно либо завершить работу, либо перейти в аварийный режим
    redis_client = None


# --- Вспомогательные функции ---
AUTH_STATE_KEY_PREFIX = "telegram_auth_state:"

def send_admin_notification(text: str):
    """Отправляет сообщение админу через Telegram Bot API."""
    if not TELEGRAM_BOT_TOKEN or not ADMIN_CHAT_ID:
        log.warning("TELEGRAM_BOT_TOKEN или ADMIN_CHAT_ID не установлены. Уведомление не отправлено.")
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": ADMIN_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown" # Используем Markdown для красивого форматирования
    }
    try:
        response = requests.post(url, data=payload, timeout=10)
        response.raise_for_status() # Вызовет ошибку, если HTTP-статус не 2xx
        log.info(f"Уведомление успешно отправлено админу (ID: {ADMIN_CHAT_ID})")
    except requests.RequestException as e:
        log.error(f"Не удалось отправить уведомление админу: {e}")


# --- Основная логика ---

@shared_task(name="celery_app.tasks.auth_TG.periodic_telegram_auth_check")
def periodic_telegram_auth_check():
    """Задача Celery: проверяет авторизацию и инициирует процесс, если нужно."""
    log.info("🚀 Запуск периодической проверки авторизации Telegram...")
    if not redis_client:
        log.error("Проверка невозможна, так как отсутствует подключение к Redis.")
        return
    try:
        # Запускаем асинхронную функцию из синхронного контекста Celery
        asyncio.run(initiate_auth_if_needed())
    except Exception as e:
        log.error(f"Критическая ошибка в задаче periodic_telegram_auth_check: {e}", exc_info=True)


async def initiate_auth_if_needed():
    """Проверяет авторизацию и, если она нужна, сохраняет состояние для бота."""
    if not all([API_ID, API_HASH, PHONE_NUMBER]):
        log.error("Отсутствуют API_ID, API_HASH или PHONE_NUMBER. Проверка прервана.")
        return

    client = TelegramClient(SESSION_FILE, API_ID, API_HASH)
    
    # Ключ состояния для текущего пользователя
    auth_key = f"{AUTH_STATE_KEY_PREFIX}{PHONE_NUMBER}"

    if redis_client.exists(auth_key):
        log.info("Процесс авторизации уже запущен. Пропускаем эту проверку.")
        return

    try:
        await client.connect()
        if await client.is_user_authorized():
            log.info("✅ Авторизация в Telegram активна.")
            return

        log.info("❗️ Требуется авторизация в Telegram. Инициирую процесс...")
        sent_code = await client.send_code_request(PHONE_NUMBER)
        
        # Сохраняем состояние в Redis на 10 минут
        state = {
            "status": "awaiting_code",
            "phone_code_hash": sent_code.phone_code_hash
        }
        redis_client.set(auth_key, json.dumps(state), ex=600)

        message = (
            "🤖 **Требуется ваше участие!**\n\n"
            "Для работы парсера телеграм-каналов нужна авторизация. Я отправил код в ваш аккаунт Telegram.\n\n"
            "Пожалуйста, **отправьте мне этот код прямо сюда**."
        )
        send_admin_notification(message)

    except Exception as e:
        log.error(f"❌ Неизвестная ошибка при инициации авторизации: {e}", exc_info=True)
    finally:
        if client.is_connected():
            await client.disconnect()