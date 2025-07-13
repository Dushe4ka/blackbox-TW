from celery import shared_task
from celery_app import app
from usecases.analysis import analyze_trend
import logging
from aiogram import Bot
import os
from dotenv import load_dotenv
import asyncio
import time
from celery import current_task
import multiprocessing

# Загружаем переменные окружения
load_dotenv()

logger = logging.getLogger(__name__)

@app.task(bind=True, name='celery_app.tasks.trend_analysis_tasks.analyze_trend_task')
def analyze_trend_task(self, category: str, user_query: str, chat_id: int = None) -> dict:
    """
    Отложенная задача для анализа тренда
    
    Args:
        category: Категория для анализа
        user_query: Запрос пользователя
        chat_id: ID чата для отправки результата
        
    Returns:
        dict: Результат анализа с информацией для отправки пользователю
    """
    start_time = time.time()
    worker_id = current_task.request.hostname
    process_id = os.getpid()
    worker_num = process_id % 5 + 1  # Номер воркера (1-5)
    
    logger.info(f"=== Воркер {worker_num} (PID: {process_id}) начал выполнение задачи анализа тренда ===")
    logger.info(f"Категория: {category}")
    logger.info(f"Запрос: {user_query}")
    
    try:
        # Выполняем анализ тренда
        result = analyze_trend(category, user_query)
        
        if result['status'] == 'success':
            # Формируем сообщение об успешном результате
            result_message = (
                f"✅ Анализ тренда по запросу завершен!\n\n"
                f"📊 Проанализировано материалов: {result['materials_count']}\n\n"
                f"📝 Результаты анализа:\n{result['analysis']}"
            )
            
            # Отправляем результат пользователю
            bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
            asyncio.run(bot.send_message(chat_id=chat_id, text=result_message))
            asyncio.run(bot.session.close())
            
            execution_time = time.time() - start_time
            logger.info(f"=== Воркер {worker_num} (PID: {process_id}) завершил задачу за {execution_time:.2f} секунд ===")
            
            return {
                'status': 'success',
                'chat_id': chat_id,
                'result_message': result_message
            }
        else:
            # Формируем сообщение об ошибке
            error_message = f"❌ Ошибка при анализе тренда: {result['message']}"
            
            # Отправляем ошибку пользователю
            bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
            asyncio.run(bot.send_message(chat_id=chat_id, text=error_message))
            asyncio.run(bot.session.close())
            
            execution_time = time.time() - start_time
            logger.error(f"=== Воркер {worker_num} (PID: {process_id}) завершил задачу с ошибкой за {execution_time:.2f} секунд ===")
            
            return {
                'status': 'error',
                'chat_id': chat_id,
                'message': error_message
            }
            
    except Exception as e:
        error_message = f"❌ Произошла ошибка при анализе тренда: {str(e)}"
        logger.error(error_message)
        
        # Отправляем ошибку пользователю
        try:
            bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
            asyncio.run(bot.send_message(chat_id=chat_id, text=error_message))
            asyncio.run(bot.session.close())
        except Exception as bot_error:
            logger.error(f"Ошибка при отправке сообщения пользователю: {str(bot_error)}")
        
        execution_time = time.time() - start_time
        logger.error(f"=== Воркер {worker_num} (PID: {process_id}) завершил задачу с исключением за {execution_time:.2f} секунд ===")
        
        return {
            'status': 'error',
            'chat_id': chat_id,
            'message': error_message
        }
