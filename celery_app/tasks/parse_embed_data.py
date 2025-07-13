import os
from celery_app import app
from celery.utils.log import get_task_logger
from vector_store import VectorStore
from dotenv import load_dotenv
from database import get_sources, db
from parsers.rss_parser import parse_rss
from parsers.tg_parser import parse_tg_channel
import asyncio

# Загружаем переменные окружения
load_dotenv()

logger = get_task_logger(__name__)

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# Функция для получения списка админских чатов
def get_admin_chat_ids():
    """Возвращает список ID административных чатов."""
    admin_chat_ids_str = os.getenv("ADMIN_CHAT_ID", "")
    if not admin_chat_ids_str:
        return []
    return [admin_chat_id.strip() for admin_chat_id in admin_chat_ids_str.split(",")]

def parse_all_sources_sync(sources):
    parsed_count = 0
    failed_sources = []
    for source in sources:
        url = source["url"]
        category = source["category"]
        stype = source["type"]
        try:
            if stype == "rss":
                result = asyncio.run(parse_rss(url, category))
            elif stype == "telegram":
                result = asyncio.run(parse_tg_channel(url, category))
                if result is None:
                    logger.warning(f"Источник {url} пропущен из-за отсутствия авторизации.")
                    continue
            else:
                failed_sources.append(url)
                continue
            # Если парсер возвращает число/True — считаем как успех
            if result:
                parsed_count += 1
            else:
                failed_sources.append(url)
        except Exception as e:
            failed_sources.append(url)
            logger.error(f"Ошибка при парсинге {url}: {e}")
    return parsed_count, failed_sources

def send_admin_notification(text: str, specific_chat_id=None):
    """Отправляет сообщение админу через Telegram Bot API."""
    if not TELEGRAM_BOT_TOKEN:
        logger.warning("TELEGRAM_BOT_TOKEN не установлен. Уведомление не отправлено.")
        return
    
    # Если указан конкретный chat_id, отправляем только туда
    if specific_chat_id:
        chat_ids = [str(specific_chat_id)]
    else:
        # Иначе отправляем во все админские чаты
        chat_ids = get_admin_chat_ids()
        if not chat_ids:
            logger.warning("ADMIN_CHAT_ID не установлен. Уведомление не отправлено.")
            return
    
    import requests
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    
    # Отправляем сообщение в указанные чаты
    for chat_id in chat_ids:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown" # Используем Markdown для красивого форматирования
        }
        try:
            response = requests.post(url, data=payload, timeout=10)
            response.raise_for_status() # Вызовет ошибку, если HTTP-статус не 2xx
            logger.info(f"Уведомление успешно отправлено админу (ID: {chat_id})")
        except requests.RequestException as e:
            logger.error(f"Не удалось отправить уведомление админу {chat_id}: {e}")

@app.task(bind=True, max_retries=3, name='celery_app.tasks.parse_embed_data.parse_and_vectorize_sources')
def parse_and_vectorize_sources(self, chat_id=None):
    sources = get_sources()
    total_sources = len(sources)

    # Синхронно парсим все источники и считаем успехи/ошибки
    parsed_count, failed_sources = parse_all_sources_sync(sources)

    # Выборка только не векторизованных записей
    not_vectorized = list(db.parsed_data.find({"vectorized": False}))

    # Векторизация
    vector_store = VectorStore()
    success = vector_store.add_materials(not_vectorized)
    vectorized_count = 0
    if success:
        vectorized_count = len(not_vectorized)
        db.parsed_data.update_many(
            {"_id": {"$in": [x["_id"] for x in not_vectorized]}},
            {"$set": {"vectorized": True}}
        )

    # Формируем отчет
    result_message = (
        f"✅ Задача завершена\n"
        f"• Источников: {total_sources}\n"
        f"• Спарсилось: {parsed_count}\n"
        f"• Векторизовано: {vectorized_count}\n"
        f"• Ошибки: {len(failed_sources)}\n"
        f"• Не спарсились: {failed_sources}"
    )

    # Отправляем результат в зависимости от типа запуска
    if chat_id:
        # Если указан chat_id - отправляем конкретному пользователю (принудительный запуск)
        send_admin_notification(result_message, chat_id)
    else:
        # Если chat_id не указан - отправляем всем админам (периодический запуск)
        send_admin_notification(result_message)