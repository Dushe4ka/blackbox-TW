from telethon import TelegramClient
from database import save_source
from logger_config import setup_logger
import os
from dotenv import load_dotenv
import re
import asyncio
from session_path import SESSION_FILE
import sys

load_dotenv()


# Настраиваем логгер
log = setup_logger("csv_reader")

print("API_ID:", os.getenv('API_ID'))
print("API_HASH:", os.getenv('API_HASH'))

def extract_channel_username(url):
    """Извлекает username канала из URL"""
    # Убираем https://t.me/ или @ если они есть
    username = re.sub(r'^https?://t\.me/', '', url)
    username = re.sub(r'^@', '', username)
    return username

async def ensure_authorized():
    """Проверяет и обеспечивает авторизацию в Telegram. Если требуется авторизация, возвращает None."""
    if not os.getenv('API_ID') or not os.getenv('API_HASH'):
        log.error("❌ Отсутствуют API_ID или API_HASH в .env файле")
        return None
        
    if not os.getenv('PHONE_NUMBER'):
        log.error("❌ Отсутствует PHONE_NUMBER в .env файле")
        return None

    client = TelegramClient(SESSION_FILE, os.getenv('API_ID'), os.getenv('API_HASH'))
    try:
        await client.connect()
        
        # Проверяем существование файла сессии
        session_file = f"{SESSION_FILE}.session"
        if os.path.exists(session_file):
            log.info(f"Найдена сохраненная сессия Telegram: {session_file}")
            log.info(f"Размер файла сессии: {os.path.getsize(session_file)} байт")
        else:
            log.warning(f"Файл сессии не найден: {session_file}")
        
        # Проверяем авторизацию
        is_authorized = await client.is_user_authorized()
        log.info(f"Статус авторизации: {is_authorized}")
        
        if not is_authorized:
            log.error("❌ Требуется авторизация в Telegram. Пропускаем источник.")
            await client.disconnect()
            return None
        else:
            log.info("✅ Уже авторизован в Telegram (используется сохраненная сессия)")
            
            # Дополнительная проверка - попробуем получить информацию о себе
            try:
                me = await client.get_me()
                log.info(f"✅ Подключен как: {me.first_name} {me.last_name or ''} (@{me.username or 'без username'})")
            except Exception as e:
                log.warning(f"Не удалось получить информацию о пользователе: {e}")
                
        return client
    except Exception as e:
        log.error(f"❌ Ошибка при авторизации: {str(e)}")
        await client.disconnect()
        return None

async def parse_tg_channel(channel, category):
    log.info(f"Начало парсинга Telegram-канала: {channel}")
    try:
        # Извлекаем username канала
        channel_username = extract_channel_username(channel)
        log.info(f"Извлечен username канала: {channel_username}")

        # Получаем авторизованный клиент
        client = await ensure_authorized()
        if not client:
            log.error("❌ Пропускаем парсинг этого канала из-за отсутствия авторизации.")
            return None

        if await client.is_bot():
            log.error("❌ Боты не могут парсить каналы. Используйте обычный аккаунт Telegram.")
            await client.disconnect()
            return None

        count = 0
        try:
            async for message in client.iter_messages(channel_username, limit=50):
                if message.text:
                    # Декодируем данные, если они в байтах
                    def decode_if_bytes(value):
                        if isinstance(value, bytes):
                            try:
                                return value.decode('utf-8')
                            except UnicodeDecodeError:
                                return value.decode('cp1251')
                        return value

                    text = decode_if_bytes(message.text)
                    data = {
                        "url": f"https://t.me/{channel_username}/{message.id}",
                        "title": text[:100] if text else "",
                        "description": text,
                        "content": text,
                        "date": message.date,
                        "category": category,
                        "source_type": "telegram"
                    }
                    save_source(data)
                    print(f"📄 Спарсено: {text[:100]}... | https://t.me/{channel_username}/{message.id}")
                    count += 1
            log.info(f"✅ Успешно спаршено {count} постов из {channel_username}")
            return count
        except Exception as e:
            log.error(f"❌ Ошибка при получении сообщений: {str(e)}")
            return None
        finally:
            await client.disconnect()
    except Exception as e:
        log.error(f"❌ Ошибка при парсинге Telegram ({channel}): {str(e)}")
        return None

async def test_tg_parser():
    """Функция для тестирования Telegram парсера через консоль"""
    print("=== Тестирование Telegram парсера ===")
    channel = input("Введите username канала (например, @channelname или https://t.me/channelname): ")
    category = input("Введите категорию: ")
    print("\nРезультаты парсинга:")
    result = await parse_tg_channel(channel, category)
    if result:
        print(f"\n✅ Успешно спаршено {result} постов")
    else:
        print("❌ Парсинг завершился с ошибкой")

if __name__ == "__main__":
    asyncio.run(test_tg_parser())
