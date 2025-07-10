# TrendWatching Bot

TrendWatching Blackbox — система анализа трендов на основе искусственного интеллекта для Telegram, с поддержкой LLM, векторных баз, автоматическим парсингом источников (CSV, RSS, Telegram) и подпиской на дайджесты.

## Особенности

- 🤖 Telegram-бот для анализа трендов и новостей
- 🔍 Семантический поиск по векторной базе (Qdrant)
- 📊 Анализ трендов с использованием LLM (DeepSeek, OpenAI, Gemini)
- 📁 Загрузка и обработка данных из CSV, RSS, Telegram-каналов
- 🔄 Автоматическая векторизация и индексация
- 📝 Генерация структурированных отчётов
- 🔔 Подписка на ежедневные и еженедельные дайджесты
- ⏩ Параллельное выполнение задач через Celery
- 🛡️ Авторизация Telegram для парсинга закрытых каналов

## Установка

1. Клонируйте репозиторий:
```bash
git clone https://github.com/yourusername/TrendWatching.git
cd TrendWatching/blackbox
```

2. Создайте виртуальное окружение и установите зависимости:
```bash
python -m venv venv
venv\Scripts\activate  # для Windows
source venv/bin/activate  # для Linux/Mac
pip install -r requirements.txt
```

3. Создайте файл `.env` в директории `blackbox/` и добавьте переменные:
```env
MONGODB_URI=your_mongodb_uri
MONGODB_DB=your_database_name
TELEGRAM_BOT_TOKEN=your_bot_token
ADMIN_ID=your_telegram_id
ADMIN_CHAT_ID=your_telegram_chat_id
API_ID=your_tg_api_id
API_HASH=your_tg_api_hash
PHONE_NUMBER=your_tg_phone
DEEPSEEK_API_KEY=your_deepseek_api_key
OPENAI_API_KEY=your_openai_api_key
GEMINI_API_KEY=your_gemini_key
LLM_PROVIDER=deepseek  # или openai, или gemini
```

## Запуск

```bash
python bot.py
```

## Основные команды бота

- `/start` — Приветствие и главное меню
- `/main_menu` — Главное меню
- `/upload` — Загрузка CSV-файла
- `/subscribe` — Управление подпиской на дайджесты
- `/tg_auth_request` — Запрос авторизации Telegram (только для администратора)

Остальные действия доступны через кнопки меню: загрузка CSV-файла, RSS/Telegram, парсинг источников, анализ по запросу, дайджесты, управление подпиской на дайджесты, запрос авторизации Telegram (только для администратора).

## Функционал

- **Загрузка источников:** CSV, RSS, Telegram-каналы (через меню)
- **Парсинг источников:** автоматический и по кнопке (RSS, Telegram)
- **Анализ трендов:** по категории и пользовательскому запросу
- **Дайджесты:** ежедневные и еженедельные, по категориям и дате
- **Подписка:** гибкое управление категориями подписки
- **Авторизация Telegram:** для доступа к закрытым каналам (через ADMIN)

## Структура проекта

```
blackbox/
├── bot.py                # Основной файл Telegram-бота
├── celery_app/           # Celery: задачи и конфиг
│   ├── celery_config.py
│   └── tasks/
│       ├── csv_processing_tasks.py
│       ├── news_tasks.py
│       ├── trend_analysis_tasks.py
│       ├── weekly_news_tasks.py
│       ├── parse_embed_data.py
│       ├── auth_TG.py
│       └── vectorization_tasks.py
├── database.py           # Работа с MongoDB
├── llm_client.py         # Работа с LLM
├── text_processor.py     # Обработка текста
├── vector_store.py       # Векторное хранилище
├── logger_config.py      # Логирование
├── csv_sources_reader.py # Импорт CSV
├── session_path.py       # Путь к сессии Telegram
├── config.py             # Конфиг LLM и API ключей
├── scripts/              # Скрипты для обслуживания
├── usecases/             # Бизнес-логика (analysis, daily_news, weekly_news)
├── parsers/              # Парсеры RSS и Telegram
├── tests/                # Тесты (test_analysis.py, test_embeddings.py, test_search.py и др.)
├── logs/                 # Логи по датам
├── requirements.txt      # Зависимости
└── README.md
```

## Celery и задачи
- Все задачи асинхронного анализа, парсинга, дайджестов и авторизации вынесены в `celery_app/tasks/`.
- Запуск воркера: `celery -A celery_app.celery_config worker --loglevel=info`
- Планировщик (beat): `celery -A celery_app.celery_config beat --loglevel=info`

## Логирование
- Все логи сохраняются в папке `logs/` с датой в имени файла.

## Тестирование
- Тесты лежат в папке `tests/` (например, `test_analysis.py`, `test_embeddings.py`, `test_search.py` и др.).
- Запуск: `pytest tests/`

## Пример .env
```
MONGODB_URI=mongodb://localhost:27017
MONGODB_DB=trendwatching
TELEGRAM_BOT_TOKEN=xxx
ADMIN_ID=123456789
ADMIN_CHAT_ID=123456789
API_ID=your_tg_api_id
API_HASH=your_tg_api_hash
PHONE_NUMBER=+79991234567
DEEPSEEK_API_KEY=xxx
OPENAI_API_KEY=xxx
GEMINI_API_KEY=xxx
LLM_PROVIDER=deepseek
```

## Примечания
- Для работы с Telegram-каналами требуется авторизация через Telethon (см. меню администратора).
- Для парсинга RSS и Telegram-источников используйте соответствующие пункты меню.
- Все зависимости указаны в `requirements.txt` (Python 3.8+).