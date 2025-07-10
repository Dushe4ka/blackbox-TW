#!/bin/bash

# Активация виртуального окружения (если используется)
# source /path/to/your/venv/bin/activate

# Переход в директорию проекта
cd "$(dirname "$0")"

# Запуск Celery worker
celery -A celery_app worker --pool=solo --concurrency=5 -l info 