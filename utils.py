import os
from typing import Union


def is_admin(user_id: Union[int, str]) -> bool:
    """
    Проверяет, является ли пользователь администратором.
    
    Args:
        user_id: ID пользователя (int или str)
    
    Returns:
        bool: True если пользователь является админом, False иначе
    """
    admin_ids_str = os.getenv("ADMIN_ID", "")
    
    if not admin_ids_str:
        return False
    
    # Преобразуем user_id в строку для сравнения
    user_id_str = str(user_id)
    
    # Разбиваем строку админов по запятой и убираем пробелы
    admin_ids = [admin_id.strip() for admin_id in admin_ids_str.split(",")]
    
    return user_id_str in admin_ids


def get_admin_ids() -> list:
    """
    Возвращает список ID администраторов.
    
    Returns:
        list: Список ID администраторов
    """
    admin_ids_str = os.getenv("ADMIN_ID", "")
    
    if not admin_ids_str:
        return []
    
    return [admin_id.strip() for admin_id in admin_ids_str.split(",")]


def is_admin_chat(chat_id: Union[int, str]) -> bool:
    """
    Проверяет, является ли чат административным.
    
    Args:
        chat_id: ID чата (int или str)
    
    Returns:
        bool: True если чат является административным, False иначе
    """
    admin_chat_ids_str = os.getenv("ADMIN_CHAT_ID", "")
    
    if not admin_chat_ids_str:
        return False
    
    # Преобразуем chat_id в строку для сравнения
    chat_id_str = str(chat_id)
    
    # Разбиваем строку админских чатов по запятой и убираем пробелы
    admin_chat_ids = [admin_chat_id.strip() for admin_chat_id in admin_chat_ids_str.split(",")]
    
    return chat_id_str in admin_chat_ids


def get_admin_chat_ids() -> list:
    """
    Возвращает список ID административных чатов.
    
    Returns:
        list: Список ID административных чатов
    """
    admin_chat_ids_str = os.getenv("ADMIN_CHAT_ID", "")
    
    if not admin_chat_ids_str:
        return []
    
    return [admin_chat_id.strip() for admin_chat_id in admin_chat_ids_str.split(",")] 