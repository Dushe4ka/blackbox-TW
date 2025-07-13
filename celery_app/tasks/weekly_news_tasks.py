from celery_app import app
from usecases.weekly_news import analyze_trend
import logging
from aiogram import Bot
import os
from dotenv import load_dotenv
import asyncio
import time
from celery import current_task
from utils.message_utils import split_analysis_message, format_message_part

# Загружаем переменные окружения
load_dotenv()

logger = logging.getLogger(__name__)

async def send_all_messages(bot, chat_id, message_parts):
    for i, part in enumerate(message_parts, 1):
        await bot.send_message(chat_id=chat_id, text=part)
        if i < len(message_parts):
            await asyncio.sleep(0.5)
    await bot.session.close()

@app.task(bind=True, name='celery_app.tasks.weekly_news_tasks.analyze_weekly_news_task')
def analyze_weekly_news_task(self, category: str, analysis_start_date: str, chat_id: int = None) -> dict:
    """
    Отложенная задача для анализа новостей за неделю
    
    Args:
        category: Категория для анализа
        analysis_start_date: Начальная дата недели (формат: YYYY-MM-DD)
        chat_id: ID чата для отправки результата
    """
    start_time = time.time()
    worker_id = current_task.request.hostname
    process_id = os.getpid()
    worker_num = process_id % 5 + 1  # Номер воркера (1-5)
    
    logger.info(f"=== Воркер {worker_num} (PID: {process_id}) начал недельный анализ новостей ===")
    logger.info(f"Категория: {category}")
    logger.info(f"Начальная дата недели: {analysis_start_date}")
    
    try:
        # Выполняем анализ новостей за неделю
        result = analyze_trend(
            category=category,
            analysis_start_date=analysis_start_date
        )
        
        if result['status'] == 'success':
            # Разбиваем сообщение на части, если оно слишком длинное
            message_parts = split_analysis_message(
                analysis_text=result['analysis'],
                materials_count=result['materials_count'],
                category=category,
                date=analysis_start_date,
                analysis_type='weekly'
            )
            
            # Отправляем все части сообщения
            bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
            asyncio.run(send_all_messages(bot, chat_id, [format_message_part(part, i+1, len(message_parts)) for i, part in enumerate(message_parts)]))
            
            execution_time = time.time() - start_time
            logger.info(f"=== Воркер {worker_num} (PID: {process_id}) завершил недельный анализ новостей за {execution_time:.2f} секунд ===")
            
            return {
                'status': 'success',
                'chat_id': chat_id,
                'parts_count': len(message_parts),
                'message': 'Недельный анализ успешно выполнен и отправлен'
            }
        else:
            # Формируем сообщение об ошибке
            error_message = f"❌ Ошибка при недельном анализе новостей: {result['message']}"
            
            # Отправляем ошибку пользователю
            bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
            asyncio.run(send_all_messages(bot, chat_id, [error_message]))
            
            execution_time = time.time() - start_time
            logger.error(f"=== Воркер {worker_num} (PID: {process_id}) завершил недельный анализ новостей с ошибкой за {execution_time:.2f} секунд ===")
            
            return {
                'status': 'error',
                'chat_id': chat_id,
                'message': error_message
            }
            
    except Exception as e:
        error_message = f"❌ Произошла ошибка при недельном анализе новостей: {str(e)}"
        logger.error(error_message)
        
        # Отправляем ошибку пользователю
        try:
            bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
            asyncio.run(send_all_messages(bot, chat_id, [error_message]))
        except Exception as bot_error:
            logger.error(f"Ошибка при отправке сообщения пользователю: {str(bot_error)}")
        
        execution_time = time.time() - start_time
        logger.error(f"=== Воркер {worker_num} (PID: {process_id}) завершил недельный анализ новостей с исключением за {execution_time:.2f} секунд ===")
        
        return {
            'status': 'error',
            'chat_id': chat_id,
            'message': error_message
        }
