#!/bin/bash

# Активация виртуального окружения (если используется)
# source /path/to/your/venv/bin/activate

# Переход в директорию проекта
cd "$(dirname "$0")"

# Запуск Celery Beat
celery -A celery_app beat -l info 