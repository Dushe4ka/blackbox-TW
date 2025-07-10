import os
from celery import Celery

# Установка переменной окружения для Windows
os.environ.setdefault('FORKED_BY_MULTIPROCESSING', '1')

def create_celery_app():
    celery_app = Celery('future2')
    
    # Загрузка конфигурации
    celery_app.config_from_object('celery_app.celery_config')
    
    # Автоматическое обнаружение задач
    celery_app.autodiscover_tasks(['celery_app.tasks'], force=True)
    
    return celery_app

app = create_celery_app() 