from hashlib import md5
import asyncio
from logger_config import setup_logger

# Настраиваем логгер
log = setup_logger("utils")

def is_duplicate(url):
    """Проверяет, есть ли запись с таким url в БД"""
    from database import db
    is_dup = db.parsed_data.count_documents({"url": url}) > 0
    if is_dup:
        log.warning(f"Дубликат обнаружен по url: {url}")
    else:
        log.info(f"Успешно загружено: {url}")
    return is_dup

async def retry_on_failure(func, *args, max_retries=3, delay=1, **kwargs):
    """
    Выполняет функцию с повторными попытками при сбоях
    
    Args:
        func: Асинхронная функция для выполнения
        *args: Аргументы функции
        max_retries: Максимальное количество попыток
        delay: Задержка между попытками в секундах
        **kwargs: Именованные аргументы функции
    
    Returns:
        Результат выполнения функции
        
    Raises:
        Exception: Если все попытки не удались
    """
    last_error = None
    for attempt in range(max_retries):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            last_error = e
            if attempt < max_retries - 1:
                log.warning(f"Попытка {attempt + 1} не удалась: {str(e)}. Повторная попытка через {delay} сек...")
                await asyncio.sleep(delay)
            else:
                log.error(f"Все {max_retries} попыток не удались. Последняя ошибка: {str(e)}")
                raise last_error