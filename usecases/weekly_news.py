# Тестировка функионала анализа
# model_embed = OpenAI API - text-embedding-3-small | model_LLM = DeepSeek

import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

from typing import Dict, Any, List
from llm_client import get_llm_client
from vector_store import VectorStore
from text_processor import TextProcessor
from logger_config import setup_logger
import tiktoken
from datetime import datetime, timedelta

# Настраиваем логгер
logger = setup_logger("test_analysis")

# Конфигурация для анализа
ANALYSIS_START_DATE = "2025-06-03"  # Начальная дата недели в формате YYYY-MM-DD
ANALYSIS_CATEGORY = "Видеоигры"  # Категория для анализа

def count_tokens(text: str, model: str = "gpt-3.5-turbo") -> int:
    """
    Подсчет количества токенов в тексте с учетом модели
    
    Args:
        text: Текст для подсчета токенов
        model: Модель для определения токенизатора
        
    Returns:
        int: Количество токенов
    """
    try:
        encoding = tiktoken.encoding_for_model(model)
        return len(encoding.encode(text))
    except Exception as e:
        logger.error(f"Ошибка при подсчете токенов: {str(e)}")
        # Если не удалось использовать tiktoken, используем приблизительный подсчет
        return len(text.split()) * 1.3  # Примерный коэффициент для слов

def calculate_chunk_size(materials: List[Dict[str, Any]], max_context_size: int) -> int:
    """
    Расчет оптимального размера чанка с учетом промптов и системных сообщений
    
    Args:
        materials: Список материалов
        max_context_size: Максимальный размер контекста модели
        
    Returns:
        int: Оптимальный размер чанка
    """
    # Оставляем 30% контекста для промптов и системных сообщений (увеличили с 20% до 30%)
    available_tokens = int(max_context_size * 0.7)
    
    # Подсчитываем средний размер материала
    total_tokens = sum(count_tokens(material['text']) for material in materials)
    avg_tokens_per_material = total_tokens / len(materials)
    
    # Добавляем запас в 20% к среднему размеру для учета вариации
    safe_avg_tokens = avg_tokens_per_material * 1.2
    
    # Рассчитываем количество материалов, которые поместятся в доступное пространство
    return max(1, int(available_tokens / safe_avg_tokens))

def _create_context_aware_chunks(materials: List[Dict[str, Any]], max_context_size: int) -> List[List[Dict[str, Any]]]:
    """
    Создание чанков с учетом контекстного окна модели
    
    Args:
        materials: Список материалов
        max_context_size: Максимальный размер контекста модели
        
    Returns:
        List[List[Dict[str, Any]]]: Список чанков
    """
    chunks = []
    current_chunk = []
    current_size = 0
    
    # Рассчитываем оптимальный размер чанка
    chunk_size = calculate_chunk_size(materials, max_context_size)
    logger.info(f"Оптимальный размер чанка: {chunk_size} материалов")
    
    for material in materials:
        material_tokens = count_tokens(material['text'])
        
        if current_size + material_tokens > max_context_size * 0.8:  # Оставляем 20% для промптов
            if current_chunk:
                chunks.append(current_chunk)
            current_chunk = [material]
            current_size = material_tokens
        else:
            current_chunk.append(material)
            current_size += material_tokens
            
        # Если достигли оптимального размера чанка, создаем новый
        if len(current_chunk) >= chunk_size:
            chunks.append(current_chunk)
            current_chunk = []
            current_size = 0
    
    if current_chunk:
        chunks.append(current_chunk)
    
    return chunks

def test_embeddings():
    """
    Тестирование размерности эмбеддингов
    """
    try:
        text_processor = TextProcessor(
            embedding_type="openai",
            openai_model="text-embedding-3-small"
        )
        test_text = "This is a test sentence for embedding dimension check"
        embedding = text_processor.create_embeddings([test_text])[0]
        logger.info(f"Тестовая размерность эмбеддинга: {len(embedding)}")
        logger.info(f"Первые 5 значений: {embedding[:5]}")
        return len(embedding)
    except Exception as e:
        logger.error(f"Ошибка при тестировании эмбеддингов: {str(e)}")
        return None

def analyze_trend(
    category: str,
    analysis_start_date: str,
    embedding_type: str = "openai",
    openai_model: str = "text-embedding-3-small"
) -> Dict[str, Any]:
    """
    Анализ трендов и формирование сводки новостей за неделю
    
    Args:
        category: Категория для анализа
        analysis_start_date: Начальная дата недели (строка)
        embedding_type: Тип эмбеддингов ("ollama" или "openai")
        openai_model: Название модели для OpenAI
        
    Returns:
        Dict[str, Any]: Результаты анализа
    """
    try:
        # Инициализация компонентов
        llm_client = get_llm_client()
        vector_store = VectorStore(
            embedding_type=embedding_type,
            openai_model=openai_model
        )
        text_processor = TextProcessor(
            embedding_type=embedding_type,
            openai_model=openai_model
        )
        
        # 1. Получаем все материалы за неделю
        logger.info(f"Получаем материалы за неделю начиная с {analysis_start_date} для категории: {category}")
        try:
            # Преобразуем строку даты в datetime
            start_date = datetime.strptime(analysis_start_date, "%Y-%m-%d")
            end_date = start_date + timedelta(days=6)
            
            # Получаем материалы за диапазон дат
            recent_materials = vector_store.search_by_category_and_date_range(
                category=category,
                start_date=start_date,
                end_date=end_date
            )
            
            if not recent_materials:
                logger.warning(f"Не найдено материалов за период {analysis_start_date} - {end_date.strftime('%Y-%m-%d')}")
                return {
                    'status': 'error',
                    'message': f"Не найдено материалов за период {analysis_start_date} - {end_date.strftime('%Y-%m-%d')}"
                }
                
            logger.info(f"Найдено {len(recent_materials)} материалов за период {analysis_start_date} - {end_date.strftime('%Y-%m-%d')}")
            
        except Exception as e:
            logger.error(f"Ошибка при получении материалов: {str(e)}")
            raise

        # 2. Проверяем общее количество токенов и максимальный размер контекста
        total_tokens = sum(count_tokens(material['text']) for material in recent_materials)
        max_context_size = llm_client.get_max_context_size()
        logger.info(f"Общее количество токенов: {total_tokens}")
        logger.info(f"Максимальный размер контекста модели: {max_context_size}")
        
        # 3. Разбиваем на чанки и анализируем
        if total_tokens <= max_context_size * 0.8:
            # Если общее количество токенов не превышает контекстное окно
            logger.info("Количество токенов в пределах контекстного окна, анализируем все материалы")
            
            # Первый этап - выделение главных новостей
            main_news_prompt = f"""
            Проанализируй следующие материалы за последние сутки в категории {category}:
            
            {[material['text'] for material in recent_materials]}
            {[material['url'] for material in recent_materials]}

            Выдели все самые важные новости, которые:
            1. Имеют наибольшее влияние на индустрию
            2. Вызвали наибольший резонанс в сообществе
            3. Могут повлиять на будущие тренды

            Для каждой новости укажи:
            - Заголовок
            - Краткое описание
            - Влияние на индустрию
            - Реакцию сообщества
            - Ссылку на источник

            Верни только выделенные новости в структурированном виде.
            """
            
            main_news_analysis = llm_client.analyze_text(
                prompt=main_news_prompt,
                query="\n".join([material['text'] for material in recent_materials])
            )
            chunk_analyses = [main_news_analysis.get('analysis', '')]
            
        else:
            # Если превышает, делим материалы на чанки
            logger.info(f"Количество токенов превышает контекстное окно, разбиваем на чанки")
            chunks = _create_context_aware_chunks(recent_materials, max_context_size)
            logger.info(f"Материалы разбиты на {len(chunks)} чанков")
            
            # Анализируем каждый чанк
            chunk_analyses = []
            for i, chunk in enumerate(chunks):
                logger.info(f"Анализ чанка {i+1}/{len(chunks)}")
                chunk_prompt = f"""
                Проанализируй следующие материалы за последние сутки в категории {category}:
                
                {[material['text'] for material in chunk]}
                {[material['url'] for material in chunk]}

                Выдели самые важные новости из этого чанка, которые:
                1. Имеют наибольшее влияние на индустрию
                2. Вызвали наибольший резонанс в сообществе
                3. Могут повлиять на будущие тренды

                Для каждой новости укажи:
                - Заголовок
                - Краткое описание
                - Влияние на индустрию
                - Реакцию сообщества
                - Ссылку на источник

                Верни только выделенные новости в структурированном виде.
                """
                
                chunk_analysis = llm_client.analyze_text(
                    prompt=chunk_prompt,
                    query="\n".join([material['text'] for material in chunk])
                )
                chunk_analyses.append(chunk_analysis.get('analysis', ''))
        
        # 4. Генерируем финальную сводку новостей
        final_prompt = f"""
        На основе следующих анализов отдельных частей материалов, сформируй единую сводку новостей за последние сутки в категории {category}.
        
        Анализы частей:
        {chunk_analyses}
        
        Сформируй ответ в следующем формате:

        📆 Сводка новостей — {category} ({analysis_start_date} - {end_date})

        🎮 Главные события:
        [Для каждой главной новости]
        - 📌 Заголовок
        - 📝 Описание
        - 💡 Влияние на индустрию
        - 👥 Реакция сообщества
        - 🔗 Ссылка на источник

        📊 Общие тренды:
        [Список основных трендов, выявленных за сутки]

        🧠 Анализ:
        [Общий анализ ситуации, включая:
        - Количество уникальных новостей
        - Индекс цитируемости
        - Наиболее активные источники
        - Ключевые выводы для категориального менеджера]

        🔮 Прогноз:
        [Краткий прогноз развития ситуации]

        На каждый раздел отвечай подробно как профессиональный аналитик.
        """
        
        final_analysis = llm_client.analyze_text(
            prompt=final_prompt,
            query="\n".join(chunk_analyses)
        )
        
        return {
            'status': 'success',
            'analysis': final_analysis.get('analysis', ''),
            'materials_count': len(recent_materials),
            'chunks_count': len(chunks) if total_tokens > max_context_size * 0.8 else 1
        }
        
    except Exception as e:
        logger.error(f"Ошибка при анализе тренда: {str(e)}")
        return {
            'status': 'error',
            'message': str(e)
        }

if __name__ == "__main__":
    # Сначала проверяем размерность эмбеддингов
    embedding_size = test_embeddings()
    if embedding_size:
        logger.info(f"Размерность эмбеддингов: {embedding_size}")
        if embedding_size != 1024:
            logger.warning(f"Размерность эмбеддингов ({embedding_size}) отличается от ожидаемой (1024)")
            logger.warning("Необходимо обновить размерность в VectorStore и пересоздать коллекцию")
    
    # Пример использования
    result = analyze_trend(ANALYSIS_CATEGORY, ANALYSIS_START_DATE)
    
    if result['status'] == 'success':
        print("\nРезультаты анализа:")
        print(f"Проанализировано материалов: {result['materials_count']}")
        print("\nСводка новостей:")
        print(result['analysis'])
    else:
        print(f"Ошибка: {result['message']}") 