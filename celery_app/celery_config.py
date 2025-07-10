from celery.schedules import crontab
import platform

# Настройки брокера и бэкенда
broker_url = 'redis://localhost:6379/0'
result_backend = 'redis://localhost:6379/0'

# Настройки задач
task_serializer = 'json'
result_serializer = 'json'
accept_content = ['json']
timezone = 'Europe/Moscow'
enable_utc = True

# Настройки для Windows
if platform.system() == 'Windows':
    worker_pool = 'solo'  # Используем solo пул для Windows
else:
    worker_pool = 'prefork'  # Используем prefork пул для Linux/Mac

worker_concurrency = 8  # Количество воркеров
worker_prefetch_multiplier = 1  # Каждый воркер берет по одной задаче
worker_max_tasks_per_child = 100  # Перезапуск воркера после 100 задач
worker_max_memory_per_child = 200000  # Перезапуск воркера при превышении памяти (в КБ)

# Настройки логирования
worker_log_format = '[%(asctime)s: %(levelname)s/%(processName)s] %(message)s'
worker_task_log_format = '[%(asctime)s: %(levelname)s/%(processName)s][%(task_name)s(%(task_id)s)] %(message)s'

# Настройки расписания
beat_schedule = {
    'send-daily-news': {
        'task': 'celery_app.tasks.news_tasks.send_daily_news',
        'schedule': crontab(hour=14, minute=0),  # Каждую день в 14:00
    },
    'periodic-telegram-auth-check': {
        'task': 'celery_app.tasks.auth_TG.periodic_telegram_auth_check',
        'schedule': crontab(minute=0, hour='*'),  # Каждый час
    },
    'parse-and-vectorize-sources-every-3h': {
        'task': 'celery_app.tasks.parse_embed_data.parse_and_vectorize_sources',
        'schedule': crontab(minute=0, hour='*/3'),  # Каждые 3 часа
    },
}

# Настройки beat
beat_max_loop_interval = 60  # Максимальный интервал проверки расписания
beat_sync_every = 60  # Синхронизация расписания каждые 60 секунд

# Включаем автодискавери задач
imports = (
    'celery_app.tasks.csv_processing_tasks',
    'celery_app.tasks.trend_analysis_tasks',
    'celery_app.tasks.news_tasks',
    'celery_app.tasks.vectorization_tasks',
    'celery_app.tasks.auth_TG',
    'celery_app.tasks.parse_embed_data',
)

# Дополнительные настройки для отладки
task_track_started = True
task_time_limit = 3600  # Максимальное время выполнения задачи (1 час)
task_soft_time_limit = 3000  # Мягкое ограничение времени (50 минут)
task_acks_late = True  # Подтверждение задачи только после выполнения
task_reject_on_worker_lost = True  # Отклонение задачи при потере воркера 