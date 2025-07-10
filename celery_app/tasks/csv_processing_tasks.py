import os
import pandas as pd
from celery_app import app
from celery.utils.log import get_task_logger
from vector_store import VectorStore
from aiogram import Bot
import asyncio
from dotenv import load_dotenv
import time
from celery import current_task
import multiprocessing

# Загружаем переменные окружения
load_dotenv()

logger = get_task_logger(__name__)

def clean_source_data(source):
    """
    Очистка данных источника
    """
    # TODO: Перенести сюда логику очистки данных из вашего существующего кода
    return source

@app.task(bind=True, max_retries=3)
def process_csv(self, file_path: str, chat_id: int = None):
    """
    Задача для обработки CSV файла и загрузки данных в векторное хранилище
    
    Args:
        file_path (str): Путь к CSV файлу
        chat_id (int): ID чата для отправки результата
    """
    start_time = time.time()
    worker_id = current_task.request.hostname
    process_id = os.getpid()
    worker_num = process_id % 5 + 1  # Номер воркера (1-5)
    
    logger.info(f"=== Воркер {worker_num} (PID: {process_id}) начал обработку CSV файла ===")
    logger.info(f"Файл: {file_path}")
    
    try:
        logger.info(f"Начало обработки CSV файла: {file_path}")
        
        # Проверяем существование файла
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Файл не найден: {file_path}")
        
        # Проверяем расширение файла
        if not file_path.endswith('.csv'):
            raise ValueError("Файл должен быть в формате CSV")
        
        # Читаем CSV
        df = pd.read_csv(file_path)
        logger.info(f"Воркер {worker_num}: Прочитано {len(df)} строк из CSV")
        
        # Проверяем наличие необходимых колонок
        required_columns = ['url', 'title', 'description', 'content', 'date', 'category', 'source_type']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            raise ValueError(f"В файле отсутствуют следующие колонки: {', '.join(missing_columns)}")
        
        # Обрабатываем данные
        processed_data = []
        
        for _, row in df.iterrows():
            source = {
                'url': row['url'],
                'title': row['title'],
                'description': row['description'],
                'content': row['content'],
                'date': row['date'],
                'category': row['category'],
                'source_type': row['source_type']
            }
            processed_source = clean_source_data(source)
            processed_data.append(processed_source)
        
        logger.info(f"Воркер {worker_num}: Обработано {len(processed_data)} записей")
        
        # Загружаем данные в векторное хранилище
        vector_store = VectorStore()
        vector_store.add_materials(processed_data)
        
        result_message = (
            f"✅ Данные успешно загружены!\n"
            f"• Загружено записей: {len(processed_data)}\n"
            f"• Категории: {', '.join(df['category'].unique())}"
        )
        
        logger.info(f"Воркер {worker_num}: Данные успешно загружены в векторное хранилище")
        
        # Отправляем результат пользователю
        bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
        asyncio.run(bot.send_message(chat_id=chat_id, text=result_message))
        asyncio.run(bot.session.close())
        
        execution_time = time.time() - start_time
        logger.info(f"=== Воркер {worker_num} (PID: {process_id}) завершил обработку за {execution_time:.2f} секунд ===")
        
        return {
            "status": "success",
            "message": "Данные успешно загружены",
            "records_count": len(processed_data),
            "categories": df['category'].unique().tolist(),
            "chat_id": chat_id,
            "result_message": result_message
        }
        
    except Exception as e:
        error_message = f"❌ Произошла ошибка при обработке файла: {str(e)}"
        logger.error(error_message)
        
        # Отправляем ошибку пользователю
        try:
            bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
            asyncio.run(bot.send_message(chat_id=chat_id, text=error_message))
            asyncio.run(bot.session.close())
        except Exception as bot_error:
            logger.error(f"Ошибка при отправке сообщения пользователю: {str(bot_error)}")
        
        execution_time = time.time() - start_time
        logger.error(f"=== Воркер {worker_num} (PID: {process_id}) завершил обработку с ошибкой за {execution_time:.2f} секунд ===")
        
        return {
            "status": "error",
            "message": error_message,
            "chat_id": chat_id
        }
        
    finally:
        # Удаляем временный файл
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Воркер {worker_num}: Временный файл удален")
