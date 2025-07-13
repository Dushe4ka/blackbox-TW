# auth_tg.py

import os
import logging
import json
import redis
import asyncio
import requests
from celery import shared_task
from telethon import TelegramClient
from telethon.errors import SessionPasswordNeededError
import sys
from pathlib import Path

# Получаем путь к корню проекта (папка blackbox)
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

# Теперь импортируем SESSION_FILE из session_path
from session_path import SESSION_FILE

# Импортируем функции для работы с админскими чатами
try:
    from utils.admin_utils import is_admin_chat, get_admin_chat_ids
except ImportError:
    # Если utils недоступен, создаем простые функции
    def is_admin_chat(chat_id):
        admin_chat_ids_str = os.getenv("ADMIN_CHAT_ID", "")
        if not admin_chat_ids_str:
            return False
        chat_id_str = str(chat_id)
        admin_chat_ids = [admin_chat_id.strip() for admin_chat_id in admin_chat_ids_str.split(",")]
        return chat_id_str in admin_chat_ids
    
    def get_admin_chat_ids():
        admin_chat_ids_str = os.getenv("ADMIN_CHAT_ID", "")
        if not admin_chat_ids_str:
            return []
        return [admin_chat_id.strip() for admin_chat_id in admin_chat_ids_str.split(",")]

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

def send_admin_notification(text: str, keyboard=None, specific_chat_id=None):
    """Отправляет сообщение админу через Telegram Bot API."""
    if not TELEGRAM_BOT_TOKEN:
        log.warning("TELEGRAM_BOT_TOKEN не установлен. Уведомление не отправлено.")
        return
    
    # Если указан конкретный chat_id, отправляем только туда
    if specific_chat_id:
        if not is_admin_chat(specific_chat_id):
            log.warning(f"Chat ID {specific_chat_id} не является админским. Уведомление не отправлено.")
            return
        chat_ids = [str(specific_chat_id)]
    else:
        # Иначе отправляем ВСЕМ админам из списка
        admin_chat_ids = get_admin_chat_ids()
        if not admin_chat_ids:
            log.warning("ADMIN_CHAT_ID не установлен. Уведомление не отправлено.")
            return
        chat_ids = admin_chat_ids
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    
    # Отправляем сообщение во все указанные чаты
    for chat_id in chat_ids:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown" # Используем Markdown для красивого форматирования
        }
        
        if keyboard:
            payload["reply_markup"] = json.dumps(keyboard)
        
        try:
            response = requests.post(url, data=payload, timeout=10)
            response.raise_for_status() # Вызовет ошибку, если HTTP-статус не 2xx
            log.info(f"Уведомление успешно отправлено админу (ID: {chat_id})")
        except requests.RequestException as e:
            log.error(f"Не удалось отправить уведомление админу {chat_id}: {e}")


# --- Основная логика ---

@shared_task(name="celery_app.tasks.auth_TG.periodic_telegram_auth_check")
def periodic_telegram_auth_check(chat_id=None):
    """Задача Celery: проверяет авторизацию и инициирует процесс, если нужно."""
    log.info("🚀 Запуск периодической проверки авторизации Telegram...")
    if not redis_client:
        log.error("Проверка невозможна, так как отсутствует подключение к Redis.")
        return
    try:
        # Запускаем асинхронную функцию из синхронного контекста Celery
        asyncio.run(initiate_auth_if_needed(chat_id))
    except Exception as e:
        log.error(f"Критическая ошибка в задаче periodic_telegram_auth_check: {e}", exc_info=True)


async def initiate_auth_if_needed(chat_id=None):
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
            
            # Отправляем сообщение пользователю о том, что авторизация активна
            message = (
                "✅ **Авторизация Telegram активна!**\n\n"
                "🔐 Ваша сессия работает корректно.\n"
                "📱 Парсер Telegram-каналов готов к работе.\n\n"
                "💡 Дополнительная авторизация не требуется."
            )
            
            # Создаем клавиатуру с кнопкой "Назад"
            keyboard = {
                "inline_keyboard": [
                    [{"text": "← Назад", "callback_data": "tg_auth_request_menu"}]
                ]
            }
            
            send_admin_notification(message, keyboard, chat_id)
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
            "Пожалуйста, **отправьте мне этот код прямо сюда**.\n\n"
            "⚠️ **Важно**: После ввода кода сессия будет автоматически сохранена."
        )
        
        # Создаем клавиатуру для управления
        keyboard = {
            "inline_keyboard": [
                [{"text": "Проверить статус", "callback_data": "tg_auth_status_check"}],
                [{"text": "← В главное меню", "callback_data": "main_menu"}]
            ]
        }
        
        send_admin_notification(message, keyboard, chat_id)

    except Exception as e:
        log.error(f"❌ Неизвестная ошибка при инициации авторизации: {e}", exc_info=True)
    finally:
        if client.is_connected():
            await client.disconnect()


@shared_task(name="celery_app.tasks.auth_TG.process_auth_code")
def process_auth_code(code: str, chat_id: int):
    """Задача Celery: обрабатывает код авторизации от пользователя."""
    log.info(f"🔐 Обработка кода авторизации для чата {chat_id}...")
    try:
        # Запускаем асинхронную функцию из синхронного контекста Celery
        asyncio.run(process_auth_code_async(code, chat_id))
    except Exception as e:
        log.error(f"Критическая ошибка в задаче process_auth_code: {e}", exc_info=True)
        error_message = f"❌ Ошибка при обработке кода авторизации: {e}"
        send_message_to_chat(chat_id, error_message)


async def process_auth_code_async(code: str, chat_id: int):
    """Асинхронная функция для обработки кода авторизации."""
    if not all([API_ID, API_HASH, PHONE_NUMBER]):
        error_msg = "❌ Отсутствуют API_ID, API_HASH или PHONE_NUMBER."
        send_message_to_chat(chat_id, error_msg)
        return

    auth_key = f"{AUTH_STATE_KEY_PREFIX}{PHONE_NUMBER}"
    state_raw = redis_client.get(auth_key)

    if not state_raw:
        error_msg = "❌ Процесс авторизации не запущен. Сначала запросите код."
        send_message_to_chat(chat_id, error_msg)
        return

    auth_state = json.loads(state_raw)
    current_status = auth_state.get("status")

    if current_status not in ["awaiting_code", "awaiting_password"]:
        error_msg = f"❌ Неожиданный статус авторизации: {current_status}"
        send_message_to_chat(chat_id, error_msg)
        return

    client = TelegramClient(SESSION_FILE, API_ID, API_HASH)

    try:
        await client.connect()
        
        # Проверяем, что сессия была создана
        if client.session is None:
            log.error("Сессия не была инициализирована при создании клиента")
            send_message_to_chat(chat_id, "❌ Ошибка: сессия не была инициализирована. Попробуйте еще раз.")
            redis_client.delete(auth_key)
            return
        
        log.info(f"Клиент подключен, сессия: {type(client.session).__name__}")
        log.info(f"Метод save доступен: {hasattr(client.session, 'save')}")
        if hasattr(client.session, 'save'):
            log.info(f"Метод save асинхронный: {asyncio.iscoroutinefunction(client.session.save)}")

        # Пытаемся войти с кодом или паролем
        try:
            if current_status == "awaiting_code":
                await client.sign_in(
                    phone=PHONE_NUMBER,
                    code=code,
                    phone_code_hash=auth_state["phone_code_hash"]
                )
            elif current_status == "awaiting_password":
                await client.sign_in(password=code)
            
            # Если успешно, сохраняем сессию
            if hasattr(client.session, 'save'):
                if asyncio.iscoroutinefunction(client.session.save):
                    await client.session.save()
                else:
                    client.session.save()
                
                # Проверяем, что файл сессии создался
                session_file = f"{SESSION_FILE}.session"
                if os.path.exists(session_file):
                    session_size = os.path.getsize(session_file)
                    log.info(f"Файл сессии сохранен: {session_file}, размер: {session_size} байт")
                    send_message_to_chat(chat_id, f"✅ Авторизация успешна! Сессия сохранена ({session_size} байт)")
                else:
                    log.error("Файл сессии не был создан после сохранения")
                    send_message_to_chat(chat_id, "⚠️ Авторизация прошла, но сессия не сохранилась")
            else:
                log.error("Метод save недоступен в сессии")
                send_message_to_chat(chat_id, "⚠️ Авторизация прошла, но не удалось сохранить сессию")
            
            # Очищаем состояние из Redis
            redis_client.delete(auth_key)
            
        except SessionPasswordNeededError:
            log.info("Обнаружена двухфакторная аутентификация")
            send_message_to_chat(chat_id, "🔐 Обнаружена двухфакторная аутентификация. Отправьте мне ваш пароль.")
            auth_state["status"] = "awaiting_password"
            redis_client.set(auth_key, json.dumps(auth_state), ex=600)
        except Exception as sign_in_error:
            log.error(f"Ошибка при входе с кодом: {sign_in_error}")
            send_message_to_chat(chat_id, f"❌ Ошибка при входе: {sign_in_error}")
            redis_client.delete(auth_key)

    except Exception as e:
        log.error(f"❌ Неизвестная ошибка при обработке кода: {e}", exc_info=True)
        send_message_to_chat(chat_id, f"❌ Ошибка при обработке кода: {e}")
        redis_client.delete(auth_key)
    finally:
        if client.is_connected():
            await client.disconnect()


@shared_task(name="celery_app.tasks.auth_TG.check_telegram_auth_status")
def check_telegram_auth_status(chat_id: int):
    """Задача Celery: проверяет статус авторизации Telegram и отправляет результат в чат."""
    log.info(f"🔍 Запуск проверки статуса авторизации Telegram для чата {chat_id}...")
    log.info(f"DEBUG: chat_id тип: {type(chat_id)}, значение: {chat_id}")
    log.info(f"DEBUG: ADMIN_CHAT_ID: {ADMIN_CHAT_ID}")
    
    try:
        # Запускаем асинхронную функцию из синхронного контекста Celery
        asyncio.run(check_auth_status_async(chat_id))
    except Exception as e:
        log.error(f"Критическая ошибка в задаче check_telegram_auth_status: {e}", exc_info=True)
        # Отправляем сообщение об ошибке
        error_message = f"❌ Ошибка при проверке статуса авторизации: {e}"
        send_message_to_chat(chat_id, error_message)


async def check_auth_status_async(chat_id: int):
    """Асинхронная функция для проверки статуса авторизации."""
    if not all([API_ID, API_HASH, PHONE_NUMBER]):
        error_msg = "❌ Отсутствуют API_ID, API_HASH или PHONE_NUMBER."
        send_message_to_chat(chat_id, error_msg)
        return

    client = TelegramClient(SESSION_FILE, API_ID, API_HASH)
    
    try:
        await client.connect()
        
        session_file = f"{SESSION_FILE}.session"
        session_exists = os.path.exists(session_file)
        session_size = os.path.getsize(session_file) if session_exists else 0
        is_authorized = await client.is_user_authorized()
        
        # Дополнительная диагностика
        session_type = type(client.session).__name__ if client.session else "None"
        log.info(f"Тип сессии: {session_type}")
        log.info(f"Файл сессии существует: {session_exists}")
        log.info(f"Размер файла сессии: {session_size}")
        log.info(f"Авторизация активна: {is_authorized}")
        
        status_text = f"📊 **Статус авторизации Telegram:**\n\n"
        status_text += f"🔑 Файл сессии: {'✅ Найден' if session_exists else '❌ Не найден'}\n"
        if session_exists:
            status_text += f"📁 Размер файла: {session_size} байт\n"
        status_text += f"🔐 Авторизация: {'✅ Активна' if is_authorized else '❌ Не активна'}\n"
        status_text += f"🔧 Тип сессии: {session_type}\n"
        
        if is_authorized:
            try:
                me = await client.get_me()
                status_text += f"👤 Пользователь: {me.first_name} {me.last_name or ''} (@{me.username or 'без username'})\n"
            except Exception as e:
                status_text += f"⚠️ Не удалось получить информацию о пользователе: {e}\n"
        
        # Формируем клавиатуру в зависимости от статуса авторизации
        if is_authorized:
            keyboard = {
                "inline_keyboard": [
                    [{"text": "← Назад", "callback_data": "tg_auth_request_menu"}]
                ]
            }
        else:
            keyboard = {
                "inline_keyboard": [
                    [{"text": "Запросить новый код", "callback_data": "tg_auth_request_confirm"}],
                    [{"text": "← Назад", "callback_data": "tg_auth_request_menu"}]
                ]
            }
        
        # Отправляем результат в чат с клавиатурой
        send_message_to_chat(chat_id, status_text, keyboard)
        
    except Exception as e:
        error_msg = f"❌ Ошибка при проверке статуса: {e}"
        log.error(f"Ошибка при проверке статуса авторизации: {e}", exc_info=True)
        send_message_to_chat(chat_id, error_msg)
    finally:
        if client.is_connected():
            await client.disconnect()


def send_message_to_chat(chat_id: int, text: str, keyboard=None):
    """Отправляет сообщение в указанный чат через Telegram Bot API."""
    if not TELEGRAM_BOT_TOKEN:
        log.warning("TELEGRAM_BOT_TOKEN не установлен. Сообщение не отправлено.")
        return
    
    # Проверяем, что chat_id является числом и положительным
    if not isinstance(chat_id, int) or chat_id <= 0:
        log.error(f"Некорректный chat_id: {chat_id} (тип: {type(chat_id)})")
        return
    
    # Проверяем, что chat_id является админским чатом для безопасности
    if not is_admin_chat(chat_id):
        log.warning(f"Попытка отправить сообщение в чат {chat_id}, но он не является админским. Сообщение не отправлено.")
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "Markdown"
    }
    
    if keyboard:
        payload["reply_markup"] = json.dumps(keyboard)
    
    try:
        response = requests.post(url, data=payload, timeout=10)
        response.raise_for_status()
        log.info(f"Сообщение успешно отправлено в чат {chat_id}")
    except requests.exceptions.HTTPError as e:
        if response.status_code == 400:
            log.error(f"400 Bad Request при отправке в чат {chat_id}. Ответ API: {response.text}")
            # Попробуем отправить без parse_mode
            try:
                payload.pop("parse_mode", None)
                response2 = requests.post(url, data=payload, timeout=10)
                response2.raise_for_status()
                log.info(f"Сообщение успешно отправлено в чат {chat_id} без parse_mode")
            except requests.exceptions.RequestException as e2:
                log.error(f"Не удалось отправить сообщение в чат {chat_id} даже без parse_mode: {e2}")
        else:
            log.error(f"HTTP ошибка при отправке в чат {chat_id}: {e}")
    except requests.RequestException as e:
        log.error(f"Не удалось отправить сообщение в чат {chat_id}: {e}")


def send_message_to_all_admins(text: str, keyboard=None):
    """Отправляет сообщение всем админам из списка."""
    if not TELEGRAM_BOT_TOKEN:
        log.warning("TELEGRAM_BOT_TOKEN не установлен. Сообщение не отправлено.")
        return
    
    admin_chat_ids = get_admin_chat_ids()
    if not admin_chat_ids:
        log.warning("ADMIN_CHAT_ID не установлен. Сообщение не отправлено.")
        return
    
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    
    # Отправляем сообщение всем админам
    for chat_id in admin_chat_ids:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown"
        }
        
        if keyboard:
            payload["reply_markup"] = json.dumps(keyboard)
        
        try:
            response = requests.post(url, data=payload, timeout=10)
            response.raise_for_status()
            log.info(f"Сообщение успешно отправлено всем админам (ID: {chat_id})")
        except requests.exceptions.HTTPError as e:
            if response.status_code == 400:
                log.error(f"400 Bad Request при отправке в чат {chat_id}. Ответ API: {response.text}")
                # Попробуем отправить без parse_mode
                try:
                    payload.pop("parse_mode", None)
                    response2 = requests.post(url, data=payload, timeout=10)
                    response2.raise_for_status()
                    log.info(f"Сообщение успешно отправлено в чат {chat_id} без parse_mode")
                except requests.exceptions.RequestException as e2:
                    log.error(f"Не удалось отправить сообщение в чат {chat_id} даже без parse_mode: {e2}")
            else:
                log.error(f"HTTP ошибка при отправке в чат {chat_id}: {e}")
        except requests.RequestException as e:
            log.error(f"Не удалось отправить сообщение в чат {chat_id}: {e}")