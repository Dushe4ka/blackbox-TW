# Тестировка функионала анализа
# model_embed = OpenAI API - text-embedding-3-small | model_LLM = DeepSeek

import logging
from typing import Dict, Any, List
from llm_client import get_llm_client
from vector_store import VectorStore
from text_processor import TextProcessor
from logger_config import setup_logger
import tiktoken

# Настраиваем логгер
logger = setup_logger("test_analysis")

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
    # Оставляем 20% контекста для промптов и системных сообщений
    available_tokens = int(max_context_size * 0.8)
    
    # Подсчитываем средний размер материала
    total_tokens = sum(count_tokens(material['text']) for material in materials)
    avg_tokens_per_material = total_tokens / len(materials)
    
    # Рассчитываем количество материалов, которые поместятся в доступное пространство
    return max(1, int(available_tokens / avg_tokens_per_material))

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
    user_query: str,
    embedding_type: str = "openai",
    openai_model: str = "text-embedding-3-small"
) -> Dict[str, Any]:
    """
    Анализ тренда на основе категории и запроса пользователя
    
    Args:
        category: Категория для анализа
        user_query: Запрос пользователя
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
        
        # 1. Получаем основную тематику из запроса пользователя через LLM
        theme_prompt = f"""
        Проанализируй запрос пользователя и напиши короткое предложение, которое будет использоваться для поиска релевантных материалов.
        Тематика должна быть максимально конкретной и отражать суть запроса.
        
        Запрос: {user_query}
        
        Верни только предложение, без дополнительных пояснений.
        """
        
        theme_response = llm_client.analyze_text(theme_prompt, user_query)
        theme = theme_response.get('analysis', '').strip()
        logger.info(f"Выделенная тематика: {theme}")
        
        # 2. Создаем эмбеддинг только для тематики (theme)
        search_text = theme
        logger.info(f"Текст для векторизации: {search_text}")
        
        try:
            search_embedding = text_processor.create_embeddings([search_text])[0]
            logger.info(f"Размерность созданного эмбеддинга: {len(search_embedding)}")
            logger.info(f"Первые 5 значений эмбеддинга: {search_embedding[:5]}")
        except Exception as e:
            logger.error(f"Ошибка при создании эмбеддинга: {str(e)}")
            raise
        
        # 3. Ищем релевантные материалы по тематике и категории
        logger.info(f"Начинаем поиск материалов для категории: {category}")
        logger.info(f"Порог релевантности: 0.35")
        
        try:
            relevant_materials = vector_store.search_vectors(
                query_vector=search_embedding,
                category=category,
                score_threshold=0.35
            )
            
            if relevant_materials:
                logger.info(f"Найдено {len(relevant_materials)} релевантных материалов")
                # Логируем первые 3 результата для проверки
                for i, material in enumerate(relevant_materials[:3]):
                    logger.info(f"Результат {i+1}:")
                    logger.info(f"Заголовок: {material.get('title', 'N/A')}")
                    logger.info(f"Категория: {material.get('category', 'N/A')}")
                    logger.info(f"Релевантность: {material.get('score', 'N/A')}")
            else:
                logger.warning("Не найдено релевантных материалов")
                return {
                    'status': 'error',
                    'message': 'Не найдено релевантных материалов'
                }
                
        except Exception as e:
            logger.error(f"Ошибка при поиске материалов: {str(e)}")
            raise

        # 4. Проверяем общее количество токенов и максимальный размер контекста
        total_tokens = sum(count_tokens(material['text']) for material in relevant_materials)
        max_context_size = llm_client.get_max_context_size()
        logger.info(f"Общее количество токенов: {total_tokens}")
        logger.info(f"Максимальный размер контекста модели: {max_context_size}")
        
        # 5. Разбиваем на чанки и анализируем
        if total_tokens <= max_context_size * 0.8:  # Оставляем 20% для промптов
            # Если общее количество токенов не превышает контекстное окно, сначала фильтруем материалы
            logger.info("Количество токенов в пределах контекстного окна, фильтруем материалы перед анализом")
            
            # Первый этап - фильтрация материалов
            filter_prompt = f"""
            Проанализируй следующие материалы по запросу: {user_query}
            
            Основная тематика: {theme}
            
            Материалы для анализа:
            {[material['text'] for material in relevant_materials]}
            {[material['url'] for material in relevant_materials]}

            Отбери и оставь только те данные (text, url) которые связаны с запросом пользователя, с игрой или направлением, а остальное удали.
            Верни только отфильтрованные материалы в формате списка, где каждый элемент содержит text и url.
            """
            
            filtered_materials_response = llm_client.analyze_text(filter_prompt, user_query)
            filtered_materials = filtered_materials_response.get('analysis', '')
            logger.info("Материалы отфильтрованы, переходим к анализу")
            
            # Второй этап - анализ отфильтрованных материалов
            analysis_prompt = f"""
            Проанализируй следующие материалы по запросу: {user_query}
            
            Основная тематика: {theme}
            
            Материалы для анализа:
            {filtered_materials}

            Важно чтобы результаты были релевантными и соответствовали запросу пользователя.
            Сделай анализ и предоставь ответ в формате:

            Пример:
            Результат
            • Событие: Релиз кейса «Dragonfire Case» 15 октября 2024
            • Ссылка: тут должна быть ссылка на материал если ее нет, то напиши что ссылка отсутствует
            • Влияние:
            ◦ Продано 1.2 млн копий кейса за первые 24 часа (+40% к среднему показателю за
            последний год)
            ◦ Рост онлайна CS2 на 25% (пик — 1.8 млн игроков)
            ◦ Цена ножа «Dragonclaw» достигла $2000 на Steam Market (+300% за неделю)
            ◦ 45% обсуждений в Reddit содержат жалобы на «слишком низкий шанс выпадения
            ножа»
            • Рекомендации:
            b. Для инвесторов/трейдеров:
            ▪ Скупить скины из кейса в первые 2 недели (исторически цены растут через
            месяц после релиза)
            ▪ Мониторить активность стримеров, массовые открытия кейсов на Twitch могут
            вызвать скачки цен.

            Используй только текст, без форматирования. Ответ должен касаться только того что было сказано в запросе пользователя.
            """
            
            final_analysis = llm_client.analyze_text(analysis_prompt, user_query)
            chunk_analyses = [final_analysis.get('analysis', '')]
            
        else:
            # Если превышает, делим материалы на чанки
            logger.info(f"Количество токенов превышает контекстное окно, разбиваем на чанки")
            chunks = _create_context_aware_chunks(relevant_materials, max_context_size)
            logger.info(f"Материалы разбиты на {len(chunks)} чанков")
            
            # Анализируем каждый чанк
            chunk_analyses = []
            for i, chunk in enumerate(chunks):
                logger.info(f"Анализ чанка {i+1}/{len(chunks)}")
                analysis_prompt = f"""
                Проанализируй следующие материалы по запросу: {user_query}
                
                Основная тематика: {theme}
                
                Материалы для анализа:
                {[material['text'] for material in chunk]}
                {[material['url'] for material in chunk]}

                Отбери и оставь только те данные (text, url) которые связаны с запросом пользователя, с игрой или направлением, а остальное удали.
                
                Важно чтобы результаты были релевантными и соответствовали запросу пользователя.

                """
                
                chunk_analysis = llm_client.analyze_text(analysis_prompt, user_query)
                chunk_analyses.append(chunk_analysis.get('analysis', ''))
        
        # 7. Генерируем финальный отчет на основе всех чанков
        final_prompt = f"""
        На основе следующих анализов отдельных частей материалов, сформируй единый отчет по запросу: {user_query}
        
        Анализы частей:
        {chunk_analyses}
        
        Важно чтобы результаты были релевантными и соответствовали запросу пользователя.
        Сделай анализ и предоставь ответ в формате:

        Результат
        • Событие: Релиз кейса «Dragonfire Case» 15 октября 2024
        • Ссылка: тут должна быть ссылка на материал если ее нет, то напиши что ссылка отсутствует
        • Влияние:
        ◦ Продано 1.2 млн копий кейса за первые 24 часа (+40% к среднему показателю за
        последний год)
        ◦ Рост онлайна CS2 на 25% (пик — 1.8 млн игроков)
        ◦ Цена ножа «Dragonclaw» достигла $2000 на Steam Market (+300% за неделю)
        ◦ 45% обсуждений в Reddit содержат жалобы на «слишком низкий шанс выпадения
        ножа»
        • Рекомендации:
        b. Для инвесторов/трейдеров:
        ▪ Скупить скины из кейса в первые 2 недели (исторически цены растут через
        месяц после релиза)
        ▪ Мониторить активность стримеров, массовые открытия кейсов на Twitch могут
        вызвать скачки цен.

        таких событий может быть несколько, их нужно анализировать по очереди, и выводить в виде списка.
        

        Используй только текст, без форматирования. Ответ должен касаться только того что было сказано в запросе пользователя.
        После перечисления напиши подробные выводы которые были бы полезны для категориального менеджерс в сфере видеоигр.
        """
        
        final_analysis = llm_client.analyze_text(final_prompt, user_query)
        
        return {
            'status': 'success',
            'theme': theme,
            'materials_count': len(relevant_materials),
            'analysis': final_analysis.get('analysis', ''),
            'materials': relevant_materials
        }
        
    except Exception as e:
        error_message = f"Ошибка при анализе тренда: {str(e)}"
        logger.error(error_message)
        return {
            'status': 'error',
            'message': error_message
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
    category = "Видеоигры"
    user_query = "Проанализируй CS GO, выяви ключевые факторы успеха/провала, спрогнозируй долгосрочную вовлеченность игроков, предложи возможные решения по развитию направления"
    
    result = analyze_trend(category, user_query)
    
    if result['status'] == 'success':
        print("\nРезультаты анализа:")
        print(f"Тематика: {result['theme']}")
        print(f"Проанализировано материалов: {result['materials_count']}")
        print("\nАнализ:")
        print(result['analysis'])
    else:
        print(f"Ошибка: {result['message']}") 

