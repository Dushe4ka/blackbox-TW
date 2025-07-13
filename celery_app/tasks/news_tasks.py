from celery_app import app
from usecases.daily_news import analyze_trend
import logging
from aiogram import Bot
import os
from dotenv import load_dotenv
import asyncio
import time
from celery import current_task
from datetime import datetime
from database import get_subscribed_users
from utils.message_utils import split_analysis_message, split_digest_message, format_message_part

# Загружаем переменные окружения
load_dotenv()

logger = logging.getLogger(__name__)

async def send_all_messages(bot, chat_id, message_parts):
    for i, part in enumerate(message_parts, 1):
        await bot.send_message(chat_id=chat_id, text=part)
        if i < len(message_parts):
            await asyncio.sleep(0.5)
    await bot.session.close()

@app.task(bind=True, name='celery_app.tasks.news_tasks.analyze_news_task')
def analyze_news_task(self, category: str, analysis_date: str, chat_id: int = None) -> dict:
    """
    Отложенная задача для анализа новостей
    
    Args:
        category: Категория для анализа
        analysis_date: Дата для анализа в формате YYYY-MM-DD
        chat_id: ID чата для отправки результата
    """
    start_time = time.time()
    worker_id = current_task.request.hostname
    process_id = os.getpid()
    worker_num = process_id % 5 + 1  # Номер воркера (1-5)
    
    logger.info(f"=== Воркер {worker_num} (PID: {process_id}) начал анализ новостей ===")
    logger.info(f"Категория: {category}")
    logger.info(f"Дата: {analysis_date}")
    
    try:
        # Выполняем анализ новостей
        result = analyze_trend(
            category=category,
            analysis_date=analysis_date
        )
        
        if result['status'] == 'success':
            message_parts = split_analysis_message(
                analysis_text=result['analysis'],
                materials_count=result['materials_count'],
                category=category,
                date=analysis_date,
                analysis_type='single_day'
            )
            bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
            asyncio.run(send_all_messages(bot, chat_id, [format_message_part(part, i+1, len(message_parts)) for i, part in enumerate(message_parts)]))
            execution_time = time.time() - start_time
            logger.info(f"=== Воркер {worker_num} (PID: {process_id}) завершил анализ новостей за {execution_time:.2f} секунд ===")
            
            return {
                'status': 'success',
                'chat_id': chat_id,
                'parts_count': len(message_parts),
                'message': 'Анализ успешно выполнен и отправлен'
            }
        else:
            error_message = f"❌ Ошибка при анализе новостей: {result['message']}"
            bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
            asyncio.run(send_all_messages(bot, chat_id, [error_message]))
            execution_time = time.time() - start_time
            logger.error(f"=== Воркер {worker_num} (PID: {process_id}) завершил анализ новостей с ошибкой за {execution_time:.2f} секунд ===")
            
            return {
                'status': 'error',
                'chat_id': chat_id,
                'message': error_message
            }
            
    except Exception as e:
        error_message = f"❌ Произошла ошибка при анализе новостей: {str(e)}"
        logger.error(error_message)
        
        # Отправляем ошибку пользователю
        try:
            bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
            asyncio.run(send_all_messages(bot, chat_id, [error_message]))
        except Exception as bot_error:
            logger.error(f"Ошибка при отправке сообщения пользователю: {str(bot_error)}")
        
        execution_time = time.time() - start_time
        logger.error(f"=== Воркер {worker_num} (PID: {process_id}) завершил анализ новостей с исключением за {execution_time:.2f} секунд ===")
        
        return {
            'status': 'error',
            'chat_id': chat_id,
            'message': error_message
        }

@app.task(bind=True, name='celery_app.tasks.news_tasks.send_daily_news')
def send_daily_news(self):
    """
    Задача для отправки ежедневных новостей всем подписчикам
    """
    logger.info("=== Начало выполнения задачи send_daily_news ===")
    start_time = time.time()
    worker_id = current_task.request.hostname
    process_id = os.getpid()
    worker_num = process_id % 5 + 1  # Номер воркера (1-5)
    
    logger.info(f"=== Воркер {worker_num} (PID: {process_id}) начал отправку ежедневных новостей ===")
    logger.info(f"Worker ID: {worker_id}")
    logger.info(f"Task ID: {self.request.id}")
    
    try:
        # Получаем текущую дату
        current_date = datetime.now().strftime("%Y-%m-%d")
        logger.info(f"Текущая дата: {current_date}")
        
        # Получаем всех подписчиков
        subscribers = get_subscribed_users()
        logger.info(f"Найдено подписчиков: {len(subscribers) if subscribers else 0}")
        
        if not subscribers:
            logger.info("Нет подписчиков для отправки новостей")
            return {
                'status': 'success',
                'message': 'No subscribers found'
            }
        
        # Отправляем новости каждому подписчику
        for subscriber in subscribers:
            chat_id = subscriber['user_id']
            categories = subscriber.get('categories', [])
            
            if not categories:
                logger.info(f"У подписчика {chat_id} нет выбранных категорий")
                continue
                
            logger.info(f"Обработка подписчика {chat_id} с категориями: {categories}")
            
            # Собираем результаты анализа для всех категорий
            all_results = []
            total_materials = 0
            
            for category in categories:
                logger.info(f"Анализ категории {category}")
                try:
                    result = analyze_trend(
                        category=category,
                        analysis_date=current_date
                    )
                    
                    if result['status'] == 'success':
                        all_results.append({
                            'category': category,
                            'materials_count': result['materials_count'],
                            'analysis': result['analysis']
                        })
                        total_materials += result['materials_count']
                except Exception as e:
                    logger.error(f"Ошибка при анализе категории {category}: {str(e)}")
                    all_results.append({
                        'category': category,
                        'error': str(e)
                    })
            
            if not all_results:
                logger.info(f"Нет результатов анализа для подписчика {chat_id}")
                continue
            
            # Формируем общий дайджест
            digest_text = ""
            for result in all_results:
                digest_text += f"📌 Категория: {result['category']}\n"
                if 'error' in result:
                    digest_text += f"❌ Ошибка: {result['error']}\n\n"
                else:
                    digest_text += (
                        f"📊 Материалов: {result['materials_count']}\n"
                        f"📝 Анализ:\n{result['analysis']}\n\n"
                    )
            
            # Разбиваем дайджест на части, если он слишком длинный
            message_parts = split_digest_message(
                digest_text=digest_text,
                date=current_date,
                total_materials=total_materials
            )
            
            # Отправляем все части дайджеста пользователю
            try:
                bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
                asyncio.run(send_all_messages(bot, chat_id, [format_message_part(part, i+1, len(message_parts)) for i, part in enumerate(message_parts)]))
                logger.info(f"Дайджест успешно отправлен подписчику {chat_id} ({len(message_parts)} частей)")
            except Exception as e:
                logger.error(f"Ошибка при отправке дайджеста подписчику {chat_id}: {str(e)}")
        
        execution_time = time.time() - start_time
        logger.info(f"=== Воркер {worker_num} (PID: {process_id}) завершил отправку ежедневных новостей за {execution_time:.2f} секунд ===")
        
        return {
            'status': 'success',
            'subscribers_count': len(subscribers)
        }
            
    except Exception as e:
        error_message = f"❌ Произошла ошибка при отправке ежедневных новостей: {str(e)}"
        logger.error(error_message)
        logger.exception("Полный стек ошибки:")
        
        execution_time = time.time() - start_time
        logger.error(f"=== Воркер {worker_num} (PID: {process_id}) завершил отправку ежедневных новостей с ошибкой за {execution_time:.2f} секунд ===")
        
        return {
            'status': 'error',
            'message': error_message
        }
