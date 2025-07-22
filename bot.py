import os
import logging
import asyncio
import json
from typing import Dict, Any, List, Set
from datetime import datetime, time
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import FSInputFile, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from logger_config import setup_logger
from vector_store import VectorStore
from text_processor import TextProcessor
from llm_client import get_llm_client
from usecases.daily_news import analyze_trend
from database import (
    get_user_subscription,
    update_user_subscription,
    toggle_subscription,
    get_subscribed_users,
    create_subscription,
    save_sources_db,
    is_source_exists_db,
    get_sources,
    delete_source,
    get_categories
)
from celery_app.tasks.csv_processing_tasks import process_csv as process_csv_task
from celery_app.tasks.trend_analysis_tasks import analyze_trend_task
from celery_app.tasks.news_tasks import analyze_news_task
from csv_sources_reader import process_csv as process_csv_sources
import redis
import json
import urllib.parse
import hashlib

from celery_app.tasks.parse_embed_data import parse_and_vectorize_sources
from celery_app.tasks.auth_TG import periodic_telegram_auth_check, check_telegram_auth_status, process_auth_code
from session_path import SESSION_FILE
from celery_app.tasks.weekly_news_tasks import analyze_weekly_news_task
from aiogram3_calendar import SimpleCalendar, simple_cal_callback
import tempfile
from utils.admin_utils import is_admin, is_admin_chat

# Настройка логирования
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Загружаем переменные окружения
load_dotenv()

# Настраиваем логгер
logger = setup_logger("bot")

# Инициализация бота
bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# Инициализация компонентов аналитики
vector_store = VectorStore()
text_processor = TextProcessor()
llm_client = get_llm_client()

# Состояния FSM
class CSVUpload(StatesGroup):
    waiting_for_file = State()

class SubscriptionStates(StatesGroup):
    waiting_for_category = State()

class RSSUpload(StatesGroup):
    waiting_for_category = State()
    waiting_for_rss = State()
    waiting_for_more_rss = State()

class TGUpload(StatesGroup):
    waiting_for_category = State()
    waiting_for_tg = State()
    waiting_for_more_tg = State()

class CustomStates(StatesGroup):
    analysis_query_category = State()
    analysis_query_input = State()
    analysis_daily_category = State()
    analysis_daily_date = State()
    analysis_weekly_category = State()
    analysis_weekly_date = State()

class SourcesManageStates(StatesGroup):
    viewing_sources = State()

def clean_source_data(source: Dict[str, Any]) -> Dict[str, Any]:
    """Очистка и валидация данных источника"""
    cleaned = source.copy()
    
    # Преобразуем NaN в пустые строки
    for key in cleaned:
        if pd.isna(cleaned[key]):
            cleaned[key] = ''
        elif isinstance(cleaned[key], (float, np.float64)):
            cleaned[key] = str(cleaned[key])
    
    # Объединяем описание и контент, если контент пустой
    if not cleaned.get('content') and cleaned.get('description'):
        cleaned['content'] = cleaned['description']
    
    return cleaned

def print_source_info(source: Dict[str, Any], index: int):
    """Вывод информации об источнике"""
    logger.info(f"\n{'='*50}")
    logger.info(f"Источник #{index + 1}:")
    logger.info(f"URL: {source.get('url', 'N/A')}")
    logger.info(f"Заголовок: {source.get('title', 'N/A')}")
    logger.info(f"Описание: {source.get('description', 'N/A')}")
    logger.info(f"Категория: {source.get('category', 'N/A')}")
    logger.info(f"Дата: {source.get('date', 'N/A')}")
    logger.info(f"Тип источника: {source.get('source_type', 'N/A')}")
    
    # Проверяем контент
    content = source.get('content', '')
    logger.info(f"Длина контента: {len(content) if content else 0} символов")
    if content:
        logger.info(f"Первые 200 символов контента: {content[:200]}")
    logger.info(f"{'='*50}\n")

def get_main_menu_keyboard(is_admin=False):
    buttons = [
        [InlineKeyboardButton(text="Источники", callback_data="menu_sources")],
        [InlineKeyboardButton(text="Анализ", callback_data="menu_analysis")],
        [InlineKeyboardButton(text="Подписка на дайджест", callback_data="menu_subscription")],
    ]
    if is_admin:
        buttons.append([InlineKeyboardButton(text="Telegram авторизация", callback_data="tg_auth_request_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def send_welcome_message(chat_id: int, is_admin=False):
    """Отправка приветственного сообщения и меню"""
    welcome_text = (
        "👋 Привет! Я бот для анализа трендов в различных категориях.\n\n"
        "📊 Что я умею:\n"
        "• Формирую ежедневные и еженедельные дайджесты новостей по выбранным категориям\n"
        "• Анализирую тренды по вашему запросу\n"
        "• Помогаю отслеживать важные изменения в интересующих вас сферах\n\n"
        "Выберите нужный раздел ниже."
    )
    keyboard = get_main_menu_keyboard(is_admin=is_admin)
    await bot.send_message(chat_id=chat_id, text=welcome_text, reply_markup=keyboard)

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    admin_status = is_admin(message.from_user.id)
    await send_welcome_message(message.chat.id, is_admin=admin_status)

@dp.message(Command("main_menu"))
async def cmd_main_menu(message: types.Message):
    admin_status = is_admin(message.from_user.id)
    keyboard = get_main_menu_keyboard(is_admin=admin_status)
    await message.answer("Выберите задачу:", reply_markup=keyboard)

@dp.message(CSVUpload.waiting_for_file)
async def process_csv_file(message: types.Message, state: FSMContext):
    # Если пользователь отправил не файл, а текст "Назад" (например, с мобильной клавиатуры)
    if message.text and message.text.strip() == "← Назад":
        # Возврат к меню загрузки источников
        await message.answer("Вы вернулись в меню загрузки источников.")
        await state.clear()
        return
    if not message.document or not message.document.file_name.endswith('.csv'):
        await message.answer(
            "❌ Пожалуйста, отправьте файл в формате CSV.",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[
                    [InlineKeyboardButton(text="← Назад", callback_data="sources_upload")]
                ]
            )
        )
        return

    file = await bot.get_file(message.document.file_id)
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as tmp:
        await bot.download_file(file.file_path, tmp.name)
        temp_file = tmp.name

    try:
        report = await process_csv_sources(temp_file, message)
        await message.answer(
            f"✅ Загрузка CSV завершена!\n"
            f"Добавлено: {report['added']}\n"
            f"Дубликатов: {report['skipped']}\n"
            f"Ошибок: {report['errors']}"
        )
        # Предложить парсинг источников
        await message.answer(
            "Хотите запустить парсинг новых источников?",
            reply_markup=get_parse_sources_keyboard()
        )
    except Exception as e:
        await message.answer(f"❌ Ошибка при обработке файла: {str(e)}")
    finally:
        os.remove(temp_file)
    await state.clear()

@dp.my_chat_member()
async def on_bot_added_to_group(event: types.ChatMemberUpdated):
    if event.new_chat_member.status in ("member", "administrator") and event.old_chat_member.status == "left":
        chat_id = event.chat.id
        text = (
            "👋 Привет! Я бот для анализа трендов в различных категориях.\n\n"
            "Теперь я в этой группе и могу присылать ежедневные новости по подписке.\n"
            "\n📊 Что я умею:\n"
            "• Формирую ежедневные и еженедельные дайджесты новостей по выбранным категориям\n"
            "• Анализирую тренды по вашему запросу\n"
            "• Помогаю отслеживать важные изменения в интересующих вас сферах\n\n"
            "Чтобы подписаться на новости для всей группы, используйте меню подписки.\n"
            "\n❗️Ежедневные новости по подписке приходят в 14:00!❗️"
        )
        await bot.send_message(chat_id, text)

def get_subscription_id_and_type(obj):
    # Для callback_query
    if hasattr(obj, 'from_user'):
        user_id = obj.from_user.id
    elif hasattr(obj, 'from_user_id'):
        user_id = obj.from_user_id
    else:
        user_id = None
    # Для групп
    if hasattr(obj, 'chat') and getattr(obj.chat, 'type', None) in ["group", "supergroup"]:
        return obj.chat.id, "group"
    # Для личных чатов (private)
    elif hasattr(obj, 'chat') and getattr(obj.chat, 'type', None) == "private":
        return user_id, "user"
    # Fallback (например, если только user_id)
    elif user_id is not None:
        return user_id, "user"
    else:
        raise Exception("Не удалось определить user_id для подписки")


@dp.message(SubscriptionStates.waiting_for_category)
async def process_subscription_category(message: types.Message, state: FSMContext):
    """Обработчик выбора категории для подписки"""
    category = message.text
    
    # Проверяем существующую подписку
    subscription_id, subscription_type = get_subscription_id_and_type(message)
    subscription = get_user_subscription(subscription_id, subscription_type)
    
    if subscription:
        # Обновляем существующую подписку
        categories = subscription.get('categories', [])
        if category in categories:
            categories.remove(category)
            await message.answer(f"✅ Отписались от категории: {category}")
        else:
            categories.append(category)
            await message.answer(f"✅ Подписались на категорию: {category}")
        
        update_user_subscription(subscription_id, subscription_type, categories)
    else:
        # Создаем новую подписку с начальной категорией
        if create_subscription(subscription_id, subscription_type, [category]):
            await message.answer(f"✅ Подписались на категорию: {category}")
        else:
            await message.answer("❌ Произошла ошибка при создании подписки")
            logger.error("Ошибка при создании подписки")
    
    await state.clear()
    await message.answer(
        "Теперь вы будете получать ежедневные новости по выбранным категориям.",
        reply_markup=types.ReplyKeyboardRemove()
    )

@dp.callback_query(lambda c: c.data == "menu_sources")
async def menu_sources_callback(callback_query: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Загрузка", callback_data="sources_upload")],
            [InlineKeyboardButton(text="Просмотр и удаление", callback_data="sources_manage")],
            [InlineKeyboardButton(text="Парсинг источников", callback_data="parse_sources_menu")],
            [InlineKeyboardButton(text="← Назад", callback_data="main_menu")],
        ]
    )
    await callback_query.message.edit_text(
        "📚 Источники:\nВыберите действие:",
        reply_markup=keyboard
    )

@dp.callback_query(lambda c: c.data == "main_menu")
async def main_menu_callback(callback_query: types.CallbackQuery):
    admin_status = is_admin(callback_query.from_user.id)
    keyboard = get_main_menu_keyboard(is_admin=admin_status)
    await callback_query.message.edit_text(
        "👋 Привет! Я бот для анализа трендов.\n\nВыберите действие:",
        reply_markup=keyboard
    )

@dp.callback_query(lambda c: c.data == "sources_upload")
async def sources_upload_callback(callback_query: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="CSV-файл (массовая загрузка)", callback_data="upload_csv")],
            [InlineKeyboardButton(text="RSS-лента (одна ссылка)", callback_data="upload_rss")],
            [InlineKeyboardButton(text="Telegram-канал (один канал)", callback_data="upload_tg")],
            [InlineKeyboardButton(text="← Назад", callback_data="menu_sources")],
        ]
    )
    await callback_query.message.edit_text(
        "📥 Загрузка источников:\n❗️Добавленные категории будут видны для подписки/анализа только после успешного парсинга данных❗️\nВыберите тип:",
        reply_markup=keyboard
    )

def category_to_callback(category: str) -> str:
    if category == "all":
        return "all"
    return hashlib.md5(category.encode('utf-8')).hexdigest()[:16]

def callback_to_category(callback: str, all_categories: list) -> str:
    if callback == "all":
        return "all"
    for cat in all_categories:
        if category_to_callback(cat) == callback:
            return cat
    return None

# --- Везде, где формируется callback_data для категорий ---
@dp.callback_query(lambda c: c.data == "sources_manage")
async def sources_manage_callback(callback_query: types.CallbackQuery):
    sources = get_sources()
    categories_set = set(src.get('category', 'Без категории') for src in sources)
    category_buttons = [
        [InlineKeyboardButton(text=cat_name, callback_data=f"sources_manage_category_{category_to_callback(cat_name)}")]
        for cat_name in sorted(categories_set)
    ]
    category_buttons.append([InlineKeyboardButton(text="Все", callback_data="sources_manage_category_all")])
    category_buttons.append([InlineKeyboardButton(text="← Назад", callback_data="menu_sources")])
    await callback_query.message.edit_text(
        "Выберите категорию источников для просмотра:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=category_buttons)
    )

@dp.callback_query(lambda c: c.data.startswith("sources_manage_category_"))
async def sources_manage_category_callback(callback_query: types.CallbackQuery):
    category_hash = callback_query.data.replace("sources_manage_category_", "")
    sources = get_sources()
    categories_set = set(src.get('category', 'Без категории') for src in sources)
    category_filter = callback_to_category(category_hash, list(categories_set))
    filtered_sources = sources if category_filter == "all" else [src for src in sources if src.get('category') == category_filter]
    keyboard = create_sources_pagination_keyboard(filtered_sources, category_filter, page=0)
    total_sources = len(filtered_sources)
    if total_sources == 0:
        text = f"🗂 Активные источники (категория: {category_filter}):\n\n❌ Источники не найдены"
    else:
        text = f"🗂 Активные источники (категория: {category_filter}):\n\n📊 Всего источников: {total_sources}\n📄 Страница 1"
    await callback_query.message.edit_text(text, reply_markup=keyboard)

@dp.callback_query(lambda c: c.data.startswith("delete_source_"))
async def delete_source_callback(callback_query: types.CallbackQuery):
    # delete_source_{category_hash}_{source_idx}_{page}
    parts = callback_query.data.replace("delete_source_", "").split("_", 2)
    if len(parts) < 3:
        await callback_query.answer("❌ Ошибка при удалении", show_alert=True)
        return
    try:
        category_hash = parts[0]
        source_idx = int(parts[1])
        page = int(parts[2])
    except Exception:
        await callback_query.answer("❌ Ошибка при удалении", show_alert=True)
        return
    sources = get_sources()
    categories_set = set(src.get('category', 'Без категории') for src in sources)
    category_filter = callback_to_category(category_hash, list(categories_set))
    filtered_sources = sources if category_filter == "all" else [src for src in sources if src.get('category') == category_filter]
    if source_idx >= len(filtered_sources):
        await callback_query.answer("❌ Источник не найден", show_alert=True)
        return
    url = filtered_sources[source_idx].get('url')
    if delete_source(url):
        await callback_query.answer("✅ Источник удалён", show_alert=False)
    else:
        await callback_query.answer("❌ Ошибка при удалении", show_alert=True)
        return
    # Обновляем список, оставаясь на той же странице
    try:
        sources = get_sources()
        categories_set = set(src.get('category', 'Без категории') for src in sources)
        category_filter = callback_to_category(category_hash, list(categories_set))
        filtered_sources = sources if category_filter == "all" else [src for src in sources if src.get('category') == category_filter]
        total_sources = len(filtered_sources)
        sources_per_page = 10
        total_pages = (total_sources + sources_per_page - 1) // sources_per_page
        if page >= total_pages and total_pages > 0:
            page = total_pages - 1
        keyboard = create_sources_pagination_keyboard(filtered_sources, category_filter, page=page)
        if total_sources == 0:
            text = f"🗂 Активные источники (категория: {category_filter}):\n\n❌ Источники не найдены"
        else:
            text = f"🗂 Активные источники (категория: {category_filter}):\n\n📊 Всего источников: {total_sources}\n📄 Страница {page+1} из {total_pages}"
        await callback_query.message.edit_text(text, reply_markup=keyboard)
    except Exception as e:
        await sources_manage_category_callback(callback_query)

@dp.callback_query(lambda c: c.data.startswith("sources_page_"))
async def sources_page_callback(callback_query: types.CallbackQuery):
    # sources_page_{category_hash}_{page}
    try:
        parts = callback_query.data.replace("sources_page_", "").split("_", 1)
        category_hash = parts[0]
        page = int(parts[1])
        sources = get_sources()
        categories_set = set(src.get('category', 'Без категории') for src in sources)
        category_filter = callback_to_category(category_hash, list(categories_set))
        filtered_sources = sources if category_filter == "all" else [src for src in sources if src.get('category') == category_filter]
        keyboard = create_sources_pagination_keyboard(filtered_sources, category_filter, page=page)
        total_sources = len(filtered_sources)
        sources_per_page = 10
        total_pages = (total_sources + sources_per_page - 1) // sources_per_page
        if total_sources == 0:
            text = f"🗂 Активные источники (категория: {category_filter}):\n\n❌ Источники не найдены"
        else:
            text = f"🗂 Активные источники (категория: {category_filter}):\n\n📊 Всего источников: {total_sources}\n📄 Страница {page+1} из {total_pages}"
        await callback_query.message.edit_text(text, reply_markup=keyboard)
    except Exception:
        await callback_query.answer("❌ Ошибка при переходе на страницу")

@dp.callback_query(lambda c: c.data == "sources_manage_category_all")
async def sources_manage_all_category_callback(callback_query: types.CallbackQuery):
    sources = get_sources()
    keyboard = create_sources_pagination_keyboard(sources, category_filter="all", page=0)
    total_sources = len(sources)
    if total_sources == 0:
        text = "🗂 Активные источники:\n\n❌ Источники не найдены"
    else:
        text = f"🗂 Активные источники:\n\n📊 Всего источников: {total_sources}\n📄 Страница 1"
    await callback_query.message.edit_text(text, reply_markup=keyboard)

@dp.callback_query(lambda c: c.data.startswith("noop_"))
async def noop_callback(callback_query: types.CallbackQuery):
    """Обработчик для кнопок без действия (noop)"""
    # Просто игнорируем нажатие
    await callback_query.answer()

@dp.callback_query(lambda c: c.data == "menu_analysis")
async def menu_analysis_callback(callback_query: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Анализ по запросу", callback_data="analysis_query")],
            [InlineKeyboardButton(text="Дайджест новостей", callback_data="analysis_digest_menu")],
            [InlineKeyboardButton(text="← Назад", callback_data="main_menu")],
        ]
    )
    await callback_query.message.edit_text(
        "📊 Анализ:\nВыберите действие:",
        reply_markup=keyboard
    )

@dp.callback_query(lambda c: c.data == "menu_subscription")
async def menu_subscription_callback(callback_query: types.CallbackQuery):
    subscription_id, subscription_type = get_subscription_id_and_type(callback_query)
    # Получаем все категории динамически
    categories = get_categories()
    # Получаем подписки пользователя из БД
    user_subs = get_user_subscription(subscription_id, subscription_type)["categories"]
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text=(cat + (" ✅" if cat in user_subs else "")),
                callback_data=f"toggle_sub_{cat}")]
            for cat in categories
        ] + [[InlineKeyboardButton(text="← Назад", callback_data="main_menu")]]
    )
    await callback_query.message.edit_text(
        "🔔 Подписка на ежедневный дайджест:\n❗️Отправка дайджеста в промежутке с 14:00 до 15:00❗️\n\n✅ - категории, на которые вы подписаны\n\nВыберите категории:",
        reply_markup=keyboard
    )

@dp.callback_query(lambda c: c.data.startswith("toggle_sub_"))
async def toggle_subscription_callback(callback_query: types.CallbackQuery):
    subscription_id, subscription_type = get_subscription_id_and_type(callback_query)
    cat = callback_query.data.replace("toggle_sub_", "")
    # Получаем текущие категории пользователя
    user_subs = get_user_subscription(subscription_id, subscription_type)["categories"]
    # Добавляем или убираем категорию
    if cat in user_subs:
        user_subs.remove(cat)
    else:
        user_subs.append(cat)
    # Обновляем в БД
    update_user_subscription(subscription_id, subscription_type, user_subs)
    # Получаем все категории динамически
    categories = get_categories()
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(
                text=(categ + (" ✅" if categ in user_subs else "")),
                callback_data=f"toggle_sub_{categ}")]
            for categ in categories
        ] + [[InlineKeyboardButton(text="← Назад", callback_data="main_menu")]]
    )
    await callback_query.message.edit_text(
        "🔔 Подписка на ежедневный дайджест:\n❗️Отправка дайджеста в промежутке с 14:00 до 15:00❗️\n\n✅ - категории, на которые вы подписаны\n\nВыберите категории:",
        reply_markup=keyboard
    )

@dp.callback_query(lambda c: c.data == "upload_csv")
async def upload_csv_callback(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.message.edit_text(
        "📤 Пожалуйста, отправьте CSV-файл для массовой загрузки источников.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="← Назад", callback_data="sources_upload")]
            ]
        )
    )
    await state.set_state(CSVUpload.waiting_for_file)

@dp.callback_query(lambda c: c.data == "upload_rss")
async def upload_rss_callback(callback_query: types.CallbackQuery, state: FSMContext):
    categories = vector_store.get_categories()
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=cat, callback_data=f"rss_cat_{urllib.parse.quote(cat)}")] for cat in categories
        ] + [
            [InlineKeyboardButton(text="Своя категория", callback_data="rss_cat_custom")],
            [InlineKeyboardButton(text="← Назад", callback_data="sources_upload")]
        ]
    )
    await callback_query.message.edit_text(
        "Выберите категорию для RSS-источника:", reply_markup=keyboard)
    await state.set_state(RSSUpload.waiting_for_category)

@dp.callback_query(lambda c: c.data.startswith("rss_cat_"))
async def rss_category_chosen(callback_query: types.CallbackQuery, state: FSMContext):
    cat = callback_query.data.replace("rss_cat_", "")
    cat = urllib.parse.unquote(cat)
    if cat == "custom":
        await callback_query.message.edit_text(
            "Введите свою категорию:",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data="upload_rss")]]
            )
        )
        await state.set_state(RSSUpload.waiting_for_category)
        await state.update_data(rss_custom=True)
    else:
        await state.update_data(category=cat, rss_custom=False)
        await callback_query.message.edit_text(
            f"Вы выбрали категорию: {cat}\nТеперь введите RSS-ссылку:",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data="upload_rss")]]
            )
        )
        await state.set_state(RSSUpload.waiting_for_rss)

@dp.message(RSSUpload.waiting_for_category)
async def rss_custom_category(message: types.Message, state: FSMContext):
    data = await state.get_data()
    if not data.get("rss_custom"):
        return
    await state.update_data(category=message.text.strip(), rss_custom=False)
    await message.answer(
        f"Вы выбрали категорию: {message.text.strip()}\nТеперь введите RSS-ссылку:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data="upload_rss")]]
        )
    )
    await state.set_state(RSSUpload.waiting_for_rss)

@dp.message(RSSUpload.waiting_for_rss)
async def process_rss_link(message: types.Message, state: FSMContext):
    url = message.text.strip()
    data = await state.get_data()
    category = data.get("category", "Общее")
    if not url.startswith("http"):
        await message.answer("❌ Пожалуйста, введите корректную ссылку на RSS-ленту.")
        return
    if is_source_exists_db(url):
        await message.answer("⚠️ Такой источник уже существует (дубликат).")
    else:
        source = {"url": url, "type": "rss", "category": category}
        if save_sources_db(source):
            await message.answer(f"✅ RSS-источник добавлен: {url}")
            await message.answer(
                "Хотите добавить еще один RSS-источник или завершить?",
                reply_markup=get_add_more_sources_keyboard("rss")
            )
            await state.set_state(RSSUpload.waiting_for_more_rss)
        else:
            await message.answer("❌ Ошибка при сохранении RSS-источника.")
            await state.clear()

@dp.callback_query(lambda c: c.data == "upload_tg")
async def upload_tg_callback(callback_query: types.CallbackQuery, state: FSMContext):
    categories = vector_store.get_categories()
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=cat, callback_data=f"tg_cat_{urllib.parse.quote(cat)}")] for cat in categories
        ] + [
            [InlineKeyboardButton(text="Своя категория", callback_data="tg_cat_custom")],
            [InlineKeyboardButton(text="← Назад", callback_data="sources_upload")]
        ]
    )
    await callback_query.message.edit_text(
        "Выберите категорию для Telegram-канала:", reply_markup=keyboard)
    await state.set_state(TGUpload.waiting_for_category)

@dp.callback_query(lambda c: c.data.startswith("tg_cat_"))
async def tg_category_chosen(callback_query: types.CallbackQuery, state: FSMContext):
    cat = callback_query.data.replace("tg_cat_", "")
    cat = urllib.parse.unquote(cat)
    if cat == "custom":
        await callback_query.message.edit_text(
            "Введите свою категорию:",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data="upload_tg")]]
            )
        )
        await state.set_state(TGUpload.waiting_for_category)
        await state.update_data(tg_custom=True)
    else:
        await state.update_data(category=cat, tg_custom=False)
        await callback_query.message.edit_text(
            f"Вы выбрали категорию: {cat}\nТеперь введите username или ссылку на Telegram-канал:",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data="upload_tg")]]
            )
        )
        await state.set_state(TGUpload.waiting_for_tg)

@dp.message(TGUpload.waiting_for_category)
async def tg_custom_category(message: types.Message, state: FSMContext):
    data = await state.get_data()
    if not data.get("tg_custom"):
        return
    await state.update_data(category=message.text.strip(), tg_custom=False)
    await message.answer(
        f"Вы выбрали категорию: {message.text.strip()}\nТеперь введите username или ссылку на Telegram-канал:",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data="upload_tg")]]
        )
    )
    await state.set_state(TGUpload.waiting_for_tg)

@dp.message(TGUpload.waiting_for_tg)
async def process_tg_channel(message: types.Message, state: FSMContext):
    tg_id = message.text.strip()
    data = await state.get_data()
    category = data.get("category", "Общее")
    # Привести к username
    if tg_id.startswith("https://t.me/"):
        tg_id = tg_id.replace("https://t.me/", "").replace("/", "")
    tg_id = tg_id.lstrip("@")
    if not tg_id:
        await message.answer("❌ Пожалуйста, введите корректный username или ссылку на Telegram-канал.")
        return
    url = f"https://t.me/{tg_id}"
    if is_source_exists_db(url):
        await message.answer("⚠️ Такой источник уже существует (дубликат).")
    else:
        source = {"url": url, "type": "telegram", "category": category}
        if save_sources_db(source):
            await message.answer(f"✅ Telegram-канал добавлен: {url}")
            await message.answer(
                "Хотите добавить еще один Telegram-канал или завершить?",
                reply_markup=get_add_more_sources_keyboard("tg")
            )
            await state.set_state(TGUpload.waiting_for_more_tg)
        else:
            await message.answer("❌ Ошибка при сохранении Telegram-канала.")
            await state.clear()

@dp.message(RSSUpload.waiting_for_more_rss)
async def process_more_rss_link(message: types.Message, state: FSMContext):
    url = message.text.strip()
    data = await state.get_data()
    category = data.get("category", "Общее")
    if not url.startswith("http"):
        await message.answer("❌ Пожалуйста, введите корректную ссылку на RSS-ленту.")
        return
    if is_source_exists_db(url):
        await message.answer("⚠️ Такой источник уже существует (дубликат).")
    else:
        source = {"url": url, "type": "rss", "category": category}
        if save_sources_db(source):
            await message.answer(f"✅ RSS-источник добавлен: {url}")
            await message.answer(
                "Хотите добавить еще один RSS-источник или завершить?",
                reply_markup=get_add_more_sources_keyboard("rss")
            )
        else:
            await message.answer("❌ Ошибка при сохранении RSS-источника.")
            await state.clear()

@dp.message(TGUpload.waiting_for_more_tg)
async def process_more_tg_channel(message: types.Message, state: FSMContext):
    tg_id = message.text.strip()
    data = await state.get_data()
    category = data.get("category", "Общее")
    # Привести к username
    if tg_id.startswith("https://t.me/"):
        tg_id = tg_id.replace("https://t.me/", "").replace("/", "")
    tg_id = tg_id.lstrip("@")
    if not tg_id:
        await message.answer("❌ Пожалуйста, введите корректный username или ссылку на Telegram-канал.")
        return
    url = f"https://t.me/{tg_id}"
    if is_source_exists_db(url):
        await message.answer("⚠️ Такой источник уже существует (дубликат).")
    else:
        source = {"url": url, "type": "telegram", "category": category}
        if save_sources_db(source):
            await message.answer(f"✅ Telegram-канал добавлен: {url}")
            await message.answer(
                "Хотите добавить еще один Telegram-канал или завершить?",
                reply_markup=get_add_more_sources_keyboard("tg")
            )
        else:
            await message.answer("❌ Ошибка при сохранении Telegram-канала.")
            await state.clear()

# Кнопки назад для upload_rss и upload_tg
@dp.callback_query(lambda c: c.data == "sources_upload")
async def back_to_sources_upload(callback_query: types.CallbackQuery, state: FSMContext):
    await sources_upload_callback(callback_query, state)

@dp.callback_query(lambda c: c.data == "upload_rss")
async def back_to_upload_rss(callback_query: types.CallbackQuery, state: FSMContext):
    await upload_rss_callback(callback_query, state)

@dp.callback_query(lambda c: c.data == "upload_tg")
async def back_to_upload_tg(callback_query: types.CallbackQuery, state: FSMContext):
    await upload_tg_callback(callback_query, state)

# Обработчики для кнопок "Назад" в состояниях добавления дополнительных источников
@dp.callback_query(lambda c: c.data == "upload_rss", RSSUpload.waiting_for_more_rss)
async def back_to_upload_rss_from_more(callback_query: types.CallbackQuery, state: FSMContext):
    await upload_rss_callback(callback_query, state)

@dp.callback_query(lambda c: c.data == "upload_tg", TGUpload.waiting_for_more_tg)
async def back_to_upload_tg_from_more(callback_query: types.CallbackQuery, state: FSMContext):
    await upload_tg_callback(callback_query, state)

@dp.callback_query(lambda c: c.data == "parse_sources_menu")
async def parse_sources_menu_callback(callback_query: CallbackQuery):
    await callback_query.message.edit_text(
        "Запустить парсинг всех источников?",
        reply_markup=get_parse_sources_keyboard()
    )

@dp.callback_query(lambda c: c.data == "parse_sources_confirm")
async def parse_sources_confirm_callback(callback_query: CallbackQuery, state: FSMContext):
    parse_and_vectorize_sources.delay(callback_query.message.chat.id)
    await callback_query.message.edit_text(
        "⏳ Парсинг источников запущен! Вы получите уведомление после завершения.",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="← В главное меню", callback_data="main_menu")]]
        )
    )
    await state.clear()

@dp.callback_query(lambda c: c.data == "parse_sources_cancel")
async def parse_sources_cancel_callback(callback_query: CallbackQuery):
    await callback_query.message.edit_text("❌ Парсинг источников отменён.")

@dp.callback_query(lambda c: c.data.startswith("add_more_"))
async def add_more_sources_callback(callback_query: CallbackQuery, state: FSMContext):
    source_type = callback_query.data.replace("add_more_", "")
    data = await state.get_data()
    category = data.get("category", "Общее")
    
    if source_type == "rss":
        await callback_query.message.edit_text(
            f"Категория: {category}\nВведите ссылку на RSS-ленту:",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data="upload_rss")]]
            )
        )
        await state.set_state(RSSUpload.waiting_for_more_rss)
    elif source_type == "tg":
        await callback_query.message.edit_text(
            f"Категория: {category}\nВведите username или ссылку на Telegram-канал:",
            reply_markup=InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data="upload_tg")]]
            )
        )
        await state.set_state(TGUpload.waiting_for_more_tg)

@dp.callback_query(lambda c: c.data == "finish_adding_sources")
async def finish_adding_sources_callback(callback_query: CallbackQuery, state: FSMContext):
    await callback_query.message.edit_text(
        "✅ Добавление источников завершено!",
        reply_markup=InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="← В главное меню", callback_data="main_menu")]]
        )
    )
    await state.clear()

@dp.message(Command("tg_auth_status"))
async def cmd_tg_auth_status(message: types.Message):
    """Проверка статуса авторизации Telegram"""
    if not is_admin(message.from_user.id):
        await message.answer("⛔️ Только администратор может проверять статус авторизации Telegram.")
        return
    
    await message.answer("🔍 Запускаю проверку статуса авторизации Telegram...")
    
    # Запускаем проверку в Celery
    print(f"DEBUG: Запускаем check_telegram_auth_status.delay с chat_id: {message.chat.id}")
    check_telegram_auth_status.delay(message.chat.id)



@dp.callback_query(lambda c: c.data == "tg_auth_request_menu")
async def tg_auth_request_menu_callback(callback_query: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Запросить новый код", callback_data="tg_auth_request_confirm")],
            [InlineKeyboardButton(text="Проверить статус", callback_data="tg_auth_status_check")],
            [InlineKeyboardButton(text="← Назад", callback_data="main_menu")],
        ]
    )
    await callback_query.message.edit_text(
        "🔑 Управление авторизацией Telegram:",
        reply_markup=keyboard
    )

@dp.callback_query(lambda c: c.data == "tg_auth_request_confirm")
async def tg_auth_request_confirm_callback(callback_query: types.CallbackQuery):
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("⛔️ Только администратор может инициировать авторизацию Telegram.", show_alert=True)
        return
    
    await callback_query.message.edit_text("🔄 Запускаю запрос на отправку нового кода авторизации...")
    
    # Запускаем задачу в Celery
    periodic_telegram_auth_check.delay(callback_query.message.chat.id)

@dp.callback_query(lambda c: c.data == "tg_auth_status_check")
async def tg_auth_status_check_callback(callback_query: types.CallbackQuery):
    if not is_admin(callback_query.from_user.id):
        await callback_query.answer("⛔️ Только администратор может проверять статус авторизации Telegram.", show_alert=True)
        return
    
    await callback_query.message.edit_text("🔍 Запускаю проверку статуса авторизации Telegram...")
    
    # Запускаем проверку в Celery
    print(f"DEBUG: Запускаем check_telegram_auth_status.delay с chat_id: {callback_query.message.chat.id}")
    check_telegram_auth_status.delay(callback_query.message.chat.id)

@dp.callback_query(lambda c: c.data == "analysis_digest_menu")
async def analysis_digest_menu_callback(callback_query: types.CallbackQuery):
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Ежедневный", callback_data="analysis_daily")],
            [InlineKeyboardButton(text="Еженедельный", callback_data="analysis_weekly")],
            [InlineKeyboardButton(text="← Назад", callback_data="menu_analysis")],
        ]
    )
    await callback_query.message.edit_text(
        "📰 Дайджест новостей:\nВыберите тип:",
        reply_markup=keyboard
    )

# Анализ по запросу: выбор категории через кнопки, затем ввод запроса
@dp.callback_query(lambda c: c.data == "analysis_query")
async def analysis_query_category_callback(callback_query: types.CallbackQuery, state: FSMContext):
    categories = VectorStore().get_categories()
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=cat, callback_data=f"analysis_query_cat_{urllib.parse.quote(cat)}")] for cat in categories] +
        [[InlineKeyboardButton(text="← Назад", callback_data="menu_analysis")]]
    )
    await callback_query.message.edit_text(
        "Выберите категорию для анализа:",
        reply_markup=keyboard
    )
    await state.set_state(CustomStates.analysis_query_category)

@dp.callback_query(lambda c: c.data.startswith("analysis_query_cat_"))
async def analysis_query_input_callback(callback_query: types.CallbackQuery, state: FSMContext):
    category = callback_query.data.replace("analysis_query_cat_", "")
    category = urllib.parse.unquote(category)
    await state.update_data(category=category)
    await callback_query.message.edit_text(
        f"Вы выбрали категорию: {category}\nВведите ваш запрос:",
    )
    await state.set_state(CustomStates.analysis_query_input)

@dp.message(CustomStates.analysis_query_input)
async def analysis_query_run(message: types.Message, state: FSMContext):
    data = await state.get_data()
    category = data.get("category")
    user_query = message.text.strip()
    await message.answer("⏳ Анализируем материалы... Ожидайте результат в течении получаса.")
    analyze_trend_task.delay(category=category, user_query=user_query, chat_id=message.chat.id)
    await state.clear()

# Ежедневный дайджест: выбор категории и даты
@dp.callback_query(lambda c: c.data == "analysis_daily")
async def analysis_daily_category_callback(callback_query: types.CallbackQuery, state: FSMContext):
    categories = VectorStore().get_categories()
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=cat, callback_data=f"analysis_daily_cat_{urllib.parse.quote(cat)}")] for cat in categories] +
        [[InlineKeyboardButton(text="← Назад", callback_data="analysis_digest_menu")]]
    )
    await callback_query.message.edit_text(
        "Выберите категорию для ежедневного дайджеста:",
        reply_markup=keyboard
    )
    await state.set_state(CustomStates.analysis_daily_category)

@dp.callback_query(lambda c: c.data.startswith("analysis_daily_cat_"))
async def analysis_daily_date_input_callback(callback_query: types.CallbackQuery, state: FSMContext):
    category = callback_query.data.replace("analysis_daily_cat_", "")
    category = urllib.parse.unquote(category)
    await state.update_data(category=category)
    await callback_query.message.edit_text(
        f"Вы выбрали категорию: {category}\nВыберите дату:",
        reply_markup=await SimpleCalendar().start_calendar()
    )
    await state.set_state(CustomStates.analysis_daily_date)

@dp.callback_query(simple_cal_callback.filter(), CustomStates.analysis_daily_date)
async def process_daily_calendar(callback_query: types.CallbackQuery, callback_data: dict, state: FSMContext):
    selected, date = await SimpleCalendar().process_selection(callback_query, callback_data)
    if selected:
        data = await state.get_data()
        category = data.get("category")
        date_str = date.strftime("%Y-%m-%d")
        await callback_query.message.answer("⏳ Формируем ежедневный дайджест... Ожидайте результат в течении получаса.")
        analyze_news_task.delay(category=category, analysis_date=date_str, chat_id=callback_query.message.chat.id)
        await state.clear()

@dp.callback_query(lambda c: c.data == "analysis_weekly")
async def analysis_weekly_category_callback(callback_query: types.CallbackQuery, state: FSMContext):
    categories = VectorStore().get_categories()
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text=cat, callback_data=f"analysis_weekly_cat_{urllib.parse.quote(cat)}")] for cat in categories] +
        [[InlineKeyboardButton(text="← Назад", callback_data="analysis_digest_menu")]]
    )
    await callback_query.message.edit_text(
        "Выберите категорию для еженедельного дайджеста:",
        reply_markup=keyboard
    )
    await state.set_state(CustomStates.analysis_weekly_category)

@dp.callback_query(lambda c: c.data.startswith("analysis_weekly_cat_"))
async def analysis_weekly_date_input_callback(callback_query: types.CallbackQuery, state: FSMContext):
    category = callback_query.data.replace("analysis_weekly_cat_", "")
    category = urllib.parse.unquote(category)
    await state.update_data(category=category)
    await callback_query.message.edit_text(
        f"Вы выбрали категорию: {category}\nВыберите начальную дату недели:",
        reply_markup=await SimpleCalendar().start_calendar()
    )
    await state.set_state(CustomStates.analysis_weekly_date)

@dp.callback_query(simple_cal_callback.filter(), CustomStates.analysis_weekly_date)
async def process_weekly_calendar(callback_query: types.CallbackQuery, callback_data: dict, state: FSMContext):
    selected, date = await SimpleCalendar().process_selection(callback_query, callback_data)
    if selected:
        data = await state.get_data()
        category = data.get("category")
        date_str = date.strftime("%Y-%m-%d")
        await callback_query.message.answer("⏳ Формируем еженедельный дайджест... Ожидайте результат в течении часа.")
        analyze_weekly_news_task.delay(category=category, analysis_start_date=date_str, chat_id=callback_query.message.chat.id)
        await state.clear()

async def set_commands():
    """Установка команд бота"""
    commands = [
        types.BotCommand(command="start", description="Запустить бота"),
        types.BotCommand(command="main_menu", description="Функционал бота"),
        types.BotCommand(command="tg_auth_status", description="Проверить статус авторизации Telegram"),
    ]
    await bot.set_my_commands(commands)

#-----------------------------------------------------------------------
#Функционал авторизации в Telegram
#-----------------------------------------------------------------------

# Инициализация клиента Redis для проверки состояния авторизации
redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
AUTH_STATE_KEY_PREFIX = "telegram_auth_state:"
PHONE_NUMBER = os.getenv('PHONE_NUMBER')


# Обработчик сообщений от админа для авторизации
@dp.message(lambda message: message.text and is_admin_chat(message.chat.id))
async def handle_auth_messages(message: types.Message, state: FSMContext):
    """
    Перехватывает все текстовые сообщения от админа и проверяет,
    не являются ли они ответом для процесса авторизации.
    """
    # Проверяем, не находится ли пользователь в другом состоянии FSM
    current_state = await state.get_state()
    if current_state is not None:
        # Если бот уже ждет чего-то другого (файл, категорию), игнорируем
        return
        
    auth_key = f"{AUTH_STATE_KEY_PREFIX}{PHONE_NUMBER}"
    state_raw = redis_client.get(auth_key)

    if not state_raw:
        # Если процесс авторизации не запущен, ничего не делаем
        return

    # Если процесс запущен, делегируем обработку Celery
    await message.answer("⏳ Получил код, обрабатываю...")
    response_text = message.text.strip()
    
    # Запускаем Celery задачу для обработки кода
    process_auth_code.delay(response_text, message.chat.id)

# --- ВСПОМОГАТЕЛЬНАЯ КНОПКА ДЛЯ ПАРСИНГА ---
def get_parse_sources_keyboard():
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Принять", callback_data="parse_sources_confirm")],
            [InlineKeyboardButton(text="Отмена", callback_data="parse_sources_cancel")],
        ]
    )

# --- КЛАВИАТУРА ДЛЯ ПРОДОЛЖЕНИЯ ДОБАВЛЕНИЯ ИСТОЧНИКОВ ---
def get_add_more_sources_keyboard(source_type: str):
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="Загрузить еще", callback_data=f"add_more_{source_type}")],
            [InlineKeyboardButton(text="Запустить парсинг", callback_data="parse_sources_confirm")],
            [InlineKeyboardButton(text="Завершить", callback_data="finish_adding_sources")],
        ]
    )

def create_sources_pagination_keyboard(sources: List[Dict], category_filter: str = "all", page: int = 0, sources_per_page: int = 10):
    """Создает клавиатуру с пагинацией для управления источниками"""
    total_sources = len(sources)
    total_pages = (total_sources + sources_per_page - 1) // sources_per_page
    
    if total_sources == 0:
        return InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="← Назад", callback_data="menu_sources")]]
        )
    
    # Ограничиваем страницу
    page = max(0, min(page, total_pages - 1))
    
    # Получаем источники для текущей страницы
    start_idx = page * sources_per_page
    end_idx = min(start_idx + sources_per_page, total_sources)
    page_sources = sources[start_idx:end_idx]
    
    # Создаем кнопки для источников
    keyboard_rows = []
    for i, src in enumerate(page_sources):
        source_idx = start_idx + i
        source_type = src.get('type', '?')
        source_url = src.get('url', '')
        # Обрезаем URL для отображения
        display_url = source_url[:30] + "..." if len(source_url) > 30 else source_url
        
        keyboard_rows.append([
            InlineKeyboardButton(
                text=f"{source_type}: {display_url}", 
                callback_data=f"noop_{category_to_callback(category_filter)}_{source_idx}_{page}"
            ),
            InlineKeyboardButton(
                text="❌", 
                callback_data=f"delete_source_{category_to_callback(category_filter)}_{source_idx}_{page}"
            )
        ])
    
    # Добавляем навигационные кнопки
    nav_buttons = []
    
    # Кнопка "Предыдущая страница"
    if page > 0:
        nav_buttons.append(InlineKeyboardButton(text="◀️", callback_data=f"sources_page_{category_to_callback(category_filter)}_{page-1}"))
    
    # Информация о странице
    nav_buttons.append(InlineKeyboardButton(
        text=f"{page+1}/{total_pages}", 
        callback_data="noop_page_info"
    ))
    
    # Кнопка "Следующая страница"
    if page < total_pages - 1:
        nav_buttons.append(InlineKeyboardButton(text="▶️", callback_data=f"sources_page_{category_to_callback(category_filter)}_{page+1}"))
    
    if nav_buttons:
        keyboard_rows.append(nav_buttons)
    
    # Кнопка "Назад"
    keyboard_rows.append([InlineKeyboardButton(text="← Назад", callback_data="menu_sources")])
    
    return InlineKeyboardMarkup(inline_keyboard=keyboard_rows)

# Отладочная информация
print(f"DEBUG: SESSION_FILE в bot.py = {SESSION_FILE}")
print(f"DEBUG: Файл сессии существует: {os.path.exists(SESSION_FILE + '.session')}")

async def main():
    """Основная функция запуска бота"""
    try:
        # Устанавливаем команды бота
        await set_commands()
        
        # Запускаем бота
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())