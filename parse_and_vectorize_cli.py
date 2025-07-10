import os
import logging
from dotenv import load_dotenv
from vector_store import VectorStore
from database import get_sources, db
from parsers.rss_parser import parse_rss
from parsers.tg_parser import parse_tg_channel
import asyncio

# Загружаем переменные окружения
load_dotenv()

from logger_config import setup_logger

# Настраиваем логгер
logger = setup_logger("parse_and_vectorize")

ADMIN_CHAT_ID = os.getenv("ADMIN_CHAT_ID")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

async def parse_all_sources(sources):
    for source in sources:
        url = source["url"]
        category = source["category"]
        stype = source["type"]
        if stype == "rss":
            await parse_rss(url, category)
        elif stype == "telegram":
            await parse_tg_channel(url, category)

def parse_all_sources_sync(sources):
    for source in sources:
        url = source["url"]
        category = source["category"]
        stype = source["type"]
        if stype == "rss":
            asyncio.run(parse_rss(url, category))
        elif stype == "telegram":
            asyncio.run(parse_tg_channel(url, category))

def start_parsing_after_csv_upload(new_sources=None):
    """Запускает парсинг после загрузки CSV файла и возвращает статусы по каждому источнику"""
    try:
        if new_sources:
            sources = new_sources
        else:
            sources = get_sources()
    
        results = []
        for source in sources:
            url = source["url"]
            category = source["category"]
            stype = source["type"]
            try:
                if stype == "rss":
                    parse_rss(url, category)
                elif stype == "telegram":
                    parse_tg_channel(url, category)
                else:
                    results.append({"url": url, "status": "❌ Неизвестный тип источника"})
                    continue
                results.append({"url": url, "status": "✅ Успешно"})
            except Exception as e:
                results.append({"url": url, "status": f"❌ Ошибка: {str(e)}"})
        logger.info("Парсинг завершен")
        return results
    except Exception as e:
        logger.error(f"Ошибка при парсинге: {str(e)}")
        raise

def main():
    logger.info("--- Запуск CLI-парсинга и векторизации источников ---")
    sources = get_sources()
    total_sources = len(sources)
    parsed_count = 0
    vectorized_count = 0
    failed_sources = []

    # Синхронно парсим все источники
    parse_all_sources_sync(sources)

    # Теперь данные гарантированно сохранены, можно векторизовать
    not_vectorized = list(db.parsed_data.find({"vectorized": False}))

    vector_store = VectorStore()
    success = vector_store.add_materials(not_vectorized)
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
    logger.info(result_message)

    asyncio.run(parse_all_sources(sources))

if __name__ == "__main__":
    main() 