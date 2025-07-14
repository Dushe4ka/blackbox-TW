from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
import os
from dotenv import load_dotenv
from datetime import datetime
import logging
from typing import Optional, List, Dict, Any

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Загружаем переменные окружения
load_dotenv()

# Проверяем наличие необходимых переменных
required_env_vars = ['MONGODB_URI', 'MONGODB_DB']
missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    raise ValueError(f"Отсутствуют необходимые переменные окружения: {', '.join(missing_vars)}")

# Подключение к MongoDB
try:
    client = MongoClient(
        os.getenv('MONGODB_URI', 'mongodb://localhost:27017/'),
        serverSelectionTimeoutMS=5000
    )
    client.admin.command('ping')
    logger.info("✅ MongoDB подключена")
except ConnectionFailure as e:
    logger.error(f"❌ Ошибка подключения: {e}")
    raise

db = client[os.getenv('MONGODB_DB')]

# Создаем индексы для таблицы parsed_data
db.parsed_data.create_index("url", unique=True)
db.parsed_data.create_index([("category", 1), ("date", -1)])

# Создаем индексы для подписок
try:
    db.subscriptions.create_index([("subscription_id", 1), ("subscription_type", 1)], unique=True)
except Exception as e:
    logger.warning(f"Индекс подписок не создан: {e}")

# Создаем индексы для таблицы sources
db.sources.create_index([("url", 1), ("type", 1)], unique=True)

# Создаем индексы для таблицы daily_news
db.daily_news.create_index([("category", 1), ("date", 1)], unique=True)

def save_source(source: Dict[str, Any]) -> bool:
    """
    Сохраняет данные в базу данных
    
    Args:
        source: словарь с данными для сохранения
            {
                "url": str,
                "title": str,
                "description": str,
                "content": str,
                "date": str,
                "category": str,
                "source_type": str
            }
    """
    try:
        # Проверяем, есть ли уже запись с таким url
        if db.parsed_data.count_documents({"url": source['url']}, limit=1) > 0:
            # Уже есть — пропускаем
            return False
        # Если нет — добавляем новую запись
        source['created_at'] = datetime.utcnow()
        source['vectorized'] = False
        db.parsed_data.insert_one(source)
        logger.info(f"Данные сохранены: {source['title'][:50]}...")
        return True
    except Exception as e:
        logger.error(f"Ошибка при сохранении данных: {str(e)}")
        return False

def is_source_exists(url: str) -> bool:
    """
    Проверяет, существует ли запись с таким URL
    
    Args:
        url: URL для проверки
        
    Returns:
        bool: True если запись существует, False если нет
    """
    return db.parsed_data.count_documents({"url": url}) > 0

def get_all_sources() -> List[Dict[str, Any]]:
    """
    Получает все источники из MongoDB
    
    Returns:
        List[Dict[str, Any]]: Список источников
    """
    try:
        sources = list(db.parsed_data.find({}, {
            "_id": 0,  # Исключаем поле _id
            "url": 1,
            "title": 1,
            "description": 1,
            "content": 1,
            "date": 1,
            "category": 1,
            "source_type": 1
        }))
        return sources
    except Exception as e:
        logger.error(f"Ошибка при получении источников: {str(e)}")
        return []

def get_data_by_category(category: str) -> List[Dict[str, Any]]:
    """
    Получает все записи по категории
    
    Args:
        category: категория для поиска
        
    Returns:
        list: список записей
    """
    try:
        return list(db.parsed_data.find(
            {"category": category},
            {"_id": 0}
        ).sort("date", -1))
    except Exception as e:
        logger.error(f"Ошибка при получении данных по категории {category}: {str(e)}")
        return []

def get_categories() -> List[str]:
    """
    Получает список всех категорий
    
    Returns:
        list: список уникальных категорий
    """
    try:
        return db.parsed_data.distinct("category")
    except Exception as e:
        logger.error(f"Ошибка при получении категорий: {str(e)}")
        return []

def get_user_subscription(subscription_id, subscription_type):
    """Получить подписку по id и типу ('user' или 'group')"""
    sub = db.subscriptions.find_one({"subscription_id": subscription_id, "subscription_type": subscription_type})
    if not sub:
        return {"categories": []}
    return sub

def update_user_subscription(subscription_id, subscription_type, categories):
    """Обновить подписку по id и типу"""
    db.subscriptions.update_one(
        {"subscription_id": subscription_id, "subscription_type": subscription_type},
        {"$set": {"categories": categories}},
        upsert=True
    )

def toggle_subscription(user_id: str) -> bool:
    """
    Переключает состояние подписки пользователя
    
    Args:
        user_id: ID пользователя
        
    Returns:
        bool: Новое состояние подписки
    """
    try:
        subscription = get_user_subscription(user_id)
        subscription['enabled'] = not subscription.get('enabled', False)
        
        db.subscriptions.update_one(
            {"user_id": user_id},
            {"$set": {
                "enabled": subscription['enabled'],
                "updated_at": datetime.utcnow()
            }}
        )
        return subscription['enabled']
    except Exception as e:
        logger.error(f"Ошибка при переключении подписки пользователя {user_id}: {str(e)}")
        return False

def get_subscribed_users():
    """Получить все подписки (и пользователей, и групп)"""
    return list(db.subscriptions.find())

def create_subscription(subscription_id, subscription_type, categories):
    """Создать новую подписку по id и типу"""
    try:
        db.subscriptions.insert_one({
            "subscription_id": subscription_id,
            "subscription_type": subscription_type,
            "categories": categories
        })
        return True
    except Exception as e:
        logger.error(f"Ошибка при создании подписки: {str(e)}")
        return False 

#---------------------------------------------------------------------------
# Функционал для работы с таблицей sources
#---------------------------------------------------------------------------

def save_sources_db(source):
    """Сохраняет источник в базу данных"""
    try:
        source['created_at'] = datetime.utcnow()
        db.sources.update_one(
            {"url": source['url'], "type": source['type']},
            {"$set": source},
            upsert=True
        )
        logger.info(f"Источник сохранен: {source['url']} ({source['type']})")
        return True
    except Exception as e:
        logger.error(f"Ошибка при сохранении источника: {str(e)}")
        return False
    
def get_sources():
    """Получает список всех источников"""
    try:
        return list(db.sources.find())
    except Exception as e:
        logger.error(f"Ошибка при получении источников: {str(e)}")
        return []
    
def is_source_exists_db(url):
    """Проверяет, существует ли источник с таким URL"""
    return db.sources.count_documents({"url": url}) > 0

def delete_source(url):
    """Удаляет источник из базы данных"""
    try:
        db.sources.delete_one({"url": url})
        logger.info(f"Источник удален: {url}")
        return True
    except Exception as e:
        logger.error(f"Ошибка при удалении источника: {str(e)}")
        return False

def save_daily_news_digest(category: str, date: str, digest_text: str) -> bool:
    """
    Сохраняет дайджест новостей по категории и дате в коллекцию daily_news
    """
    try:
        db.daily_news.update_one(
            {"category": category, "date": date},
            {"$set": {"category": category, "date": date, "digest": digest_text, "updated_at": datetime.utcnow()}},
            upsert=True
        )
        logger.info(f"Дайджест сохранён: {category} {date}")
        return True
    except Exception as e:
        logger.error(f"Ошибка при сохранении дайджеста: {str(e)}")
        return False

def get_daily_news_digest(category: str, date: str) -> str:
    """
    Получает дайджест новостей по категории и дате из коллекции daily_news
    """
    try:
        doc = db.daily_news.find_one({"category": category, "date": date})
        if doc:
            return doc.get("digest", "")
        return ""
    except Exception as e:
        logger.error(f"Ошибка при получении дайджеста: {str(e)}")
        return ""