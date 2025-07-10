import aiohttp
import asyncio
import feedparser
from database import save_source
from logger_config import setup_logger
from parsers.utils import retry_on_failure

# Настраиваем логгер
log = setup_logger("rss_parsers")


async def fetch_rss_content(url, headers):
    """Получает содержимое RSS-ленты"""
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as response:
            return await response.text()

async def parse_rss(url, category, verbose=False):
    log.info(f"Начало парсинга RSS: {url}")
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/rss+xml, text/xml;q=0.9, */*;q=0.8'
        }
        
        # Используем механизм повторных запросов
        content = await retry_on_failure(fetch_rss_content, url, headers)
        feed = feedparser.parse(content)
        
        if verbose:
            print("\n=== Отладочная информация ===")
            print(f"Фактический URL: {getattr(feed, 'href', url)}")
            print(f"Статус: {getattr(feed, 'status', 'N/A')}")
            if hasattr(feed, 'bozo_exception') and feed.bozo_exception:
                print(f"Ошибка парсера: {type(feed.bozo_exception).__name__}: {feed.bozo_exception}")
            print(f"Content-Type: {feed.headers.get('content-type', 'N/A')}")
            print(f"Версия: {getattr(feed, 'version', 'N/A')}")
            print(f"Кодировка: {getattr(feed, 'encoding', 'N/A')}")
            print(f"Заголовок канала: {feed.feed.get('title', 'N/A')}")
            print(f"Описание канала: {feed.feed.get('description', 'N/A')}")
            print(f"Всего записей: {len(feed.entries)}")
            if len(feed.entries) > 0:
                print("\nПример первой записи:")
                first_entry = feed.entries[0]
                print(f"Заголовок: {first_entry.get('title', 'N/A')}")
                print(f"Ссылка: {first_entry.get('link', 'N/A')}")
                print(f"Дата публикации: {first_entry.get('published', 'N/A')}")
                print(f"Доступные поля: {list(first_entry.keys())}")
            print("===========================\n")
            
        if feed.bozo and isinstance(feed.bozo_exception, feedparser.NonXMLContentType):
            log.error(f"Сервер вернул не XML (RSS) контент: {feed.headers.get('content-type', 'N/A')}")
            if verbose:
                print("Сырой ответ сервера:")
                print(f"Первые 200 символов:\n{content[:200]}")
            return None
            
        if not feed.entries:
            log.warning("RSS-лента не содержит записей")
            return None
            
        for entry in feed.entries:
            # Декодируем данные, если они в байтах
            def decode_if_bytes(value):
                if isinstance(value, bytes):
                    try:
                        return value.decode('utf-8')
                    except UnicodeDecodeError:
                        return value.decode('cp1251')
                return value

            data = {
                "url": decode_if_bytes(entry.link),
                "title": decode_if_bytes(entry.get('title', '')),
                "description": decode_if_bytes(entry.get('description', '')),
                "content": decode_if_bytes(entry.get('content', [{}])[0].get('value', '')) if hasattr(entry, 'content') else '',
                "date": entry.get('published', ''),
                "category": category,
                "source_type": "rss"
            }
            save_source(data)
            print(f"Спарсено: {data['title'][:100]}... | {data['url']}")
            
        log.info(f"Успешно спаршено {len(feed.entries)} записей из {url}")
        return feed.entries
    except Exception as e:
        log.error(f"Ошибка при парсинге RSS ({url}): {str(e)}")
        return None

async def test_rss_parser():
    """Функция для тестирования RSS парсера через консоль"""
    print("=== Тестирование RSS парсера ===")
    url = input("Введите URL RSS-ленты: ").strip()
    category = input("Введите категорию: ").strip()
    verbose = input("Показать отладочную информацию? (y/n): ").lower() == 'y'
    print("\nРезультаты парсинга:")
    result = await parse_rss(url, category, verbose)
    if result:
        print(f"\nУспешно спаршено {len(result)} записей")
    else:
        print("\nПарсинг завершился с ошибкой или лента пуста")
        print("Проверьте:")
        print("- Доступность RSS-ленты (откройте в браузере)")
        print("- Корректность URL")
        print("- Наличие записей в ленте")

if __name__ == "__main__":
    asyncio.run(test_rss_parser())
