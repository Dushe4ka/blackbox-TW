import re
from typing import List, Tuple
import logging

logger = logging.getLogger(__name__)

# Константы для Telegram
MAX_MESSAGE_LENGTH = 4096
MAX_CAPTION_LENGTH = 1024

def split_message(text: str, max_length: int = MAX_MESSAGE_LENGTH) -> List[str]:
    """
    Разбивает длинное сообщение на части, сохраняя целостность предложений
    
    Args:
        text: Текст для разбиения
        max_length: Максимальная длина одной части (по умолчанию 4096 для Telegram)
    
    Returns:
        Список частей сообщения
    """
    if len(text) <= max_length:
        return [text]
    
    parts = []
    current_part = ""
    
    # Разбиваем текст на предложения
    sentences = re.split(r'(?<=[.!?])\s+', text)
    
    # Если нет предложений или только одно предложение, разбиваем по словам
    if len(sentences) <= 1:
        words = text.split()
        
        # Если нет слов (например, только символы без пробелов), разбиваем по символам
        if len(words) <= 1:
            # Разбиваем текст на части по max_length символов
            for i in range(0, len(text), max_length):
                part = text[i:i + max_length]
                if part:
                    parts.append(part)
            return parts
        
        # Разбиваем по словам
        current_part = ""
        
        for word in words:
            if len(current_part + " " + word) <= max_length:
                current_part += (" " + word) if current_part else word
            else:
                if current_part:
                    parts.append(current_part.strip())
                current_part = word
        
        if current_part:
            parts.append(current_part.strip())
        
        return parts
    
    # Обрабатываем предложения
    for sentence in sentences:
        # Если предложение само по себе длиннее лимита
        if len(sentence) > max_length:
            # Если у нас есть накопленный текст, сохраняем его
            if current_part:
                parts.append(current_part.strip())
                current_part = ""
            
            # Разбиваем длинное предложение по словам
            words = sentence.split()
            temp_part = ""
            
            for word in words:
                if len(temp_part + " " + word) <= max_length:
                    temp_part += (" " + word) if temp_part else word
                else:
                    if temp_part:
                        parts.append(temp_part.strip())
                    temp_part = word
            
            if temp_part:
                current_part = temp_part
        else:
            # Проверяем, поместится ли предложение в текущую часть
            if len(current_part + " " + sentence) <= max_length:
                current_part += (" " + sentence) if current_part else sentence
            else:
                # Сохраняем текущую часть и начинаем новую
                if current_part:
                    parts.append(current_part.strip())
                current_part = sentence
    
    # Добавляем последнюю часть
    if current_part:
        parts.append(current_part.strip())
    
    return parts

def split_analysis_message(
    analysis_text: str,
    materials_count: int,
    category: str = None,
    date: str = None,
    analysis_type: str = None
) -> List[str]:
    """
    Специальная функция для разбиения сообщений анализа новостей
    
    Args:
        analysis_text: Текст анализа
        materials_count: Количество проанализированных материалов
        category: Категория анализа (опционально)
        date: Дата анализа (опционально)
        analysis_type: Тип анализа ("daily", "weekly", "trend_query", "single_day")
    
    Returns:
        Список частей сообщения
    """
    # Формируем заголовок в зависимости от типа анализа
    if analysis_type == "weekly":
        header = "✅ Анализ новостей за неделю завершен!\n\n"
    elif analysis_type == "single_day":
        header = "✅ Анализ новостей за сутки завершен!\n\n"
    elif analysis_type == "trend_query":
        header = "✅ Анализ тренда по запросу завершен!\n\n"
    else:
        header = "✅ Анализ новостей завершен!\n\n"
    if category:
        header += f"📂 Категория: {category}\n"
    if date:
        header += f"📅 Дата: {date}\n"
    header += f"📊 Проанализировано материалов: {materials_count}\n\n"
    
    # Если весь текст помещается в одно сообщение
    full_message = header + f"📝 Результаты анализа:\n{analysis_text}"
    if len(full_message) <= MAX_MESSAGE_LENGTH:
        return [full_message]
    
    # Разбиваем на части
    parts = []
    
    # Первая часть с заголовком
    first_part = header + "📝 Результаты анализа:\n"
    remaining_length = MAX_MESSAGE_LENGTH - len(first_part)
    
    # Находим подходящее место для разрыва в анализе
    analysis_parts = split_message(analysis_text, remaining_length)
    
    if analysis_parts:
        first_part += analysis_parts[0]
        parts.append(first_part)
        
        # Добавляем остальные части анализа
        for part in analysis_parts[1:]:
            parts.append(f"📝 Продолжение анализа:\n{part}")
    
    return parts

def split_digest_message(digest_text: str, date: str, total_materials: int) -> List[str]:
    """
    Специальная функция для разбиения дайджеста новостей
    
    Args:
        digest_text: Текст дайджеста
        date: Дата дайджеста
        total_materials: Общее количество материалов
    
    Returns:
        Список частей сообщения
    """
    # Формируем заголовок
    header = f"📰 Ежедневный дайджест новостей за {date}\n\n"
    header += f"📊 Всего проанализировано материалов: {total_materials}\n\n"
    
    # Если весь текст помещается в одно сообщение
    full_message = header + digest_text
    if len(full_message) <= MAX_MESSAGE_LENGTH:
        return [full_message]
    
    # Разбиваем на части
    parts = []
    
    # Первая часть с заголовком
    first_part = header
    remaining_length = MAX_MESSAGE_LENGTH - len(first_part)
    
    # Находим подходящее место для разрыва в дайджесте
    digest_parts = split_message(digest_text, remaining_length)
    
    if digest_parts:
        first_part += digest_parts[0]
        parts.append(first_part)
        
        # Добавляем остальные части дайджеста
        for i, part in enumerate(digest_parts[1:], 2):
            parts.append(f"📰 Продолжение дайджеста (часть {i}):\n{part}")
    
    return parts

def format_message_part(part: str, part_number: int = None, total_parts: int = None) -> str:
    """
    Форматирует часть сообщения с указанием номера части
    
    Args:
        part: Текст части сообщения
        part_number: Номер части (начиная с 1)
        total_parts: Общее количество частей
    
    Returns:
        Отформатированная часть сообщения
    """
    if part_number and total_parts and total_parts > 1:
        return f"{part}\n\n--- Часть {part_number} из {total_parts} ---"
    return part 