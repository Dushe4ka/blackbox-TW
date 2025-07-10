import os
from typing import Dict, Any
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

# Конфигурация моделей для разных провайдеров
LLM_CONFIG = {
    "deepseek": {
        "model": "deepseek-chat",
        "temperature": 0.7,
        "max_tokens": None,
        "timeout": None,
        "max_retries": 2
    },
    "openai": {
        "model": "gpt-3.5-turbo",
        "temperature": 0.7,
        "max_tokens": None,
        "timeout": None,
        "max_retries": 2
    },
    "gemini": {
        "model": "gemini-pro",
        "temperature": 0.7,
        "max_output_tokens": None,
        "max_retries": 2
    }
}

# Получаем текущего провайдера из переменных окружения
CURRENT_PROVIDER = os.getenv("LLM_PROVIDER", "deepseek").lower()

# Проверяем наличие API ключей
API_KEYS = {
    "deepseek": os.getenv("DEEPSEEK_API_KEY"),
    "openai": os.getenv("OPENAI_API_KEY"),
    "gemini": os.getenv("GEMINI_API_KEY")
}

def get_provider_config(provider: str = None) -> Dict[str, Any]:
    """
    Получение конфигурации для указанного провайдера
    
    Args:
        provider: Название провайдера (deepseek, openai, gemini)
        
    Returns:
        Dict[str, Any]: Конфигурация провайдера
    """
    provider = provider or CURRENT_PROVIDER
    if provider not in LLM_CONFIG:
        raise ValueError(f"Неподдерживаемый провайдер: {provider}")
    return LLM_CONFIG[provider]

def get_api_key(provider: str = None) -> str:
    """
    Получение API ключа для указанного провайдера
    
    Args:
        provider: Название провайдера (deepseek, openai, gemini)
        
    Returns:
        str: API ключ
    """
    provider = provider or CURRENT_PROVIDER
    if provider not in API_KEYS:
        raise ValueError(f"Неподдерживаемый провайдер: {provider}")
    
    api_key = API_KEYS[provider]
    if not api_key:
        raise ValueError(f"API ключ для {provider} не найден в переменных окружения")
    
    return api_key 