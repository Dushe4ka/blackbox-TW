import pandas as pd
import logging
from typing import Dict, Any
from database import save_source, is_source_exists
from logger_config import setup_logger

# Настраиваем логгер
logger = setup_logger("csv_reader")

def process_csv_file(file_path: str) -> Dict[str, Any]:
    """
    Обрабатывает CSV файл и сохраняет данные в MongoDB
    
    Args:
        file_path: Путь к CSV файлу
        
    Returns:
        Dict[str, Any]: Статистика обработки
    """
    stats = {
        "total": 0,
        "added": 0,
        "skipped": 0,
        "errors": 0
    }
    
    try:
        logger.info(f"Начало обработки файла: {file_path}")
        
        # Читаем CSV файл
        logger.info("Чтение CSV файла...")
        df = pd.read_csv(file_path)
        logger.info(f"Прочитано {len(df)} строк")
        
        # Проверяем наличие необходимых колонок
        required_columns = ['url', 'title', 'description', 'content', 'date', 'category', 'source_type']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            error_msg = f"Отсутствуют обязательные колонки: {', '.join(missing_columns)}"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Обрабатываем каждую строку
        for index, row in df.iterrows():
            stats["total"] += 1
            logger.info(f"Обработка записи {index + 1}/{len(df)}")
            
            try:
                # Проверяем URL
                url = str(row['url']).strip()
                if not url or url == 'nan':
                    logger.warning(f"Пропуск записи {index + 1}: пустой URL")
                    stats["skipped"] += 1
                    continue
                
                # Проверяем на дубликаты
                if is_source_exists(url):
                    logger.info(f"Пропуск записи {index + 1}: URL уже существует")
                    stats["skipped"] += 1
                    continue
                
                # Подготавливаем данные
                source_data = {
                    "url": url,
                    "title": str(row['title']).strip(),
                    "description": str(row['description']).strip(),
                    "content": str(row['content']).strip(),
                    "date": str(row['date']).strip(),
                    "category": str(row['category']).strip(),
                    "source_type": str(row['source_type']).strip()
                }
                
                # Логируем информацию о записи
                logger.info(f"URL: {source_data['url']}")
                logger.info(f"Заголовок: {source_data['title']}")
                logger.info(f"Категория: {source_data['category']}")
                logger.info(f"Дата: {source_data['date']}")
                
                # Сохраняем в MongoDB
                if save_source(source_data):
                    logger.info(f"Запись {index + 1} успешно сохранена")
                    stats["added"] += 1
                else:
                    logger.error(f"Ошибка при сохранении записи {index + 1}")
                    stats["errors"] += 1
                    
            except Exception as e:
                logger.error(f"Ошибка при обработке записи {index + 1}: {str(e)}")
                stats["errors"] += 1
                continue
        
        logger.info("Обработка файла завершена")
        logger.info(f"Статистика: всего={stats['total']}, добавлено={stats['added']}, "
                   f"пропущено={stats['skipped']}, ошибок={stats['errors']}")
        
        return stats
        
    except Exception as e:
        logger.error(f"Критическая ошибка при обработке файла: {str(e)}")
        stats["errors"] += 1
        return stats
