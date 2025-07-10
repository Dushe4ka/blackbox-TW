import asyncio
import logging
from aiogram import Bot, Dispatcher
from bot import dp, bot, set_commands
from logger_config import setup_logger

# Настраиваем логгер
logger = setup_logger("main")

async def main():
    """Запуск бота"""
    try:
        # Устанавливаем команды бота
        await set_commands()
        
        # Запускаем бота
        logger.info("Starting bot...")
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {str(e)}")
        raise

if __name__ == "__main__":
    asyncio.run(main())