import csv
from database import save_sources_db, is_source_exists_db
from logger_config import setup_logger

# Настраиваем логгер
logger = setup_logger("csv_reader_source")

async def process_csv(file_path, message):
    """Обрабатывает CSV файл и сохраняет только новые источники. Возвращает отчет по загрузке."""
    added = 0
    skipped = 0
    errors = 0
    new_sources = []
    try:
        with open(file_path, 'r', encoding='utf-8-sig') as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    url = row['url'].strip()
                    source_type = row['type'].strip()
                    category = row['category'].strip()
                except Exception as e:
                    logger.warning(f"Ошибка в строке CSV: {str(e)}")
                    errors += 1
                    continue
                # Проверяем на дубликаты
                if is_source_exists_db(url):
                    logger.warning(f"Пропущен дубликат: {url}")
                    skipped += 1
                    continue
                # Сохраняем источник
                source = {
                    "url": url,
                    "category": category,
                    "type": source_type
                }
                save_sources_db(source)
                new_sources.append(source)
                logger.info(f"Добавлен источник: {url} (категория: {category}, тип: {source_type})")
                added += 1
                if added % 10 == 0:
                    await message.answer(f"✅ Обработано {added} источников...")
        logger.info(f"Обработка CSV завершена. Добавлено: {added}, пропущено: {skipped}, ошибок: {errors}")
    except Exception as e:
        logger.error(f"Ошибка при сохранении источников: {str(e)}")
        errors += 1
    return {"added": added, "skipped": skipped, "errors": errors}