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

# Загружаем переменные окружения
load_dotenv()

logger = logging.getLogger(__name__)

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
            # Формируем сообщение об успешном результате
            result_message = (
                f"✅ Анализ новостей за сутки завершен!\n\n"
                f"📊 Проанализировано материалов: {result['materials_count']}\n\n"
                f"📝 Результаты анализа:\n{result['analysis']}"
            )
            
            # Отправляем результат пользователю
            bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
            asyncio.run(bot.send_message(chat_id=chat_id, text=result_message))
            asyncio.run(bot.session.close())
            
            execution_time = time.time() - start_time
            logger.info(f"=== Воркер {worker_num} (PID: {process_id}) завершил анализ новостей за {execution_time:.2f} секунд ===")
            
            return {
                'status': 'success',
                'chat_id': chat_id,
                'result_message': result_message
            }
        else:
            # Формируем сообщение об ошибке
            error_message = f"❌ Ошибка при анализе новостей: {result['message']}"
            
            # Отправляем ошибку пользователю
            bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
            asyncio.run(bot.send_message(chat_id=chat_id, text=error_message))
            asyncio.run(bot.session.close())
            
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
            asyncio.run(bot.send_message(chat_id=chat_id, text=error_message))
            asyncio.run(bot.session.close())
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
            digest_message = (
                f"📰 Ежедневный дайджест новостей за {current_date}\n\n"
                f"📊 Всего проанализировано материалов: {total_materials}\n\n"
            )
            
            for result in all_results:
                digest_message += f"📌 Категория: {result['category']}\n"
                if 'error' in result:
                    digest_message += f"❌ Ошибка: {result['error']}\n\n"
                else:
                    digest_message += (
                        f"📊 Материалов: {result['materials_count']}\n"
                        f"📝 Анализ:\n{result['analysis']}\n\n"
                    )
            
            # Отправляем дайджест пользователю
            try:
                bot = Bot(token=os.getenv("TELEGRAM_BOT_TOKEN"))
                asyncio.run(bot.send_message(chat_id=chat_id, text=digest_message))
                asyncio.run(bot.session.close())
                logger.info(f"Дайджест успешно отправлен подписчику {chat_id}")
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
