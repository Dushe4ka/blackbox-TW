import logging
from typing import List, Dict, Any, Optional
from datetime import datetime
import uuid
from qdrant_client import QdrantClient
from qdrant_client.http import models
from qdrant_client.http.models import Distance, VectorParams, Filter, FieldCondition, Range, Payload
from logger_config import setup_logger
from text_processor import TextProcessor

# Настраиваем логгер
logger = setup_logger("vector_store")

class VectorStore:
    def __init__(
        self,
        collection_name: str = "trends",
        embedding_type: str = "openai",  # "ollama" или "openai"
        openai_model: str = "text-embedding-3-small",  # для openai
        host: str = "localhost",
        port: int = 6333
    ):
        """
        Инициализация векторного хранилища
        
        Args:
            collection_name: Название коллекции
            embedding_type: Тип эмбеддингов ("ollama" или "openai")
            openai_model: Название модели для OpenAI
            host: Хост Qdrant
            port: Порт Qdrant
        """
        self.collection_name = collection_name
        self.embedding_type = embedding_type
        self.client = QdrantClient(host=host, port=port)
        self.text_processor = TextProcessor(
            embedding_type=embedding_type,
            openai_model=openai_model
        )
        
        # Определяем размерность векторов в зависимости от типа эмбеддингов
        if embedding_type == "ollama":
            self.vector_size = 3072
        elif embedding_type == "openai":
            if openai_model == "text-embedding-3-small":
                self.vector_size = 1536
            elif openai_model == "text-embedding-3-large":
                self.vector_size = 3072
            elif openai_model == "text-embedding-ada-002":
                self.vector_size = 1536
            else:
                raise ValueError(f"Неподдерживаемая модель OpenAI: {openai_model}")
        else:
            raise ValueError(f"Неподдерживаемый тип эмбеддингов: {embedding_type}")
        
        # Создаем коллекцию, если она не существует
        self._create_collection_if_not_exists()

    def _create_collection_if_not_exists(self):
        """Создает коллекцию в Qdrant, если она не существует"""
        try:
            collections = self.client.get_collections().collections
            collection_names = [collection.name for collection in collections]
            
            if self.collection_name not in collection_names:
                logger.info(f"Создание коллекции {self.collection_name}")
                
                # Создаем коллекцию с нужной размерностью векторов
                self.client.create_collection(
                    collection_name=self.collection_name,
                    vectors_config=VectorParams(
                        size=self.vector_size,
                        distance=Distance.COSINE
                    )
                )
                
                logger.info(f"Коллекция {self.collection_name} создана успешно")
            else:
                logger.info(f"Коллекция {self.collection_name} уже существует")
                
        except Exception as e:
            logger.error(f"Ошибка при создании коллекции: {str(e)}")
            raise

    def _parse_date(self, date_str: str) -> str:
        """
        Преобразует дату из различных форматов в стандартный формат '%Y-%m-%d'
        
        Args:
            date_str: Строка с датой в любом формате
            
        Returns:
            str: Дата в формате '%Y-%m-%d'
        """
        try:
            # Если это уже datetime, просто форматируем
            if isinstance(date_str, datetime):
                return date_str.strftime('%Y-%m-%d')
            # Если это не строка, пробуем привести к строке
            if not isinstance(date_str, str):
                date_str = str(date_str)
            # Список форматов для парсинга
            date_formats = [
                '%Y-%m-%d',  # Стандартный формат
                '%d.%m.%Y',  # Формат dd.mm.yyyy
                '%a, %d %b %Y %H:%M:%S %Z',  # RFC 2822 формат
                '%Y-%m-%dT%H:%M:%S.%fZ',  # ISO формат с микросекундами
                '%Y-%m-%dT%H:%M:%SZ',  # ISO формат без микросекунд
                '%Y-%m-%d %H:%M:%S',  # Формат с временем
                '%d-%m-%Y',  # Формат dd-mm-yyyy
                '%m/%d/%Y',  # Формат mm/dd/yyyy
                '%d/%m/%Y'   # Формат dd/mm/yyyy
            ]
            
            # Пробуем каждый формат
            for date_format in date_formats:
                try:
                    parsed_date = datetime.strptime(date_str, date_format)
                    return parsed_date.strftime('%Y-%m-%d')
                except ValueError:
                    continue
            
            # Если ни один формат не подошел, пробуем парсить через dateutil
            try:
                from dateutil import parser
                parsed_date = parser.parse(date_str)
                return parsed_date.strftime('%Y-%m-%d')
            except:
                pass
            
            # Если все попытки не удались, возвращаем исходную строку
            logger.warning(f"Не удалось распарсить дату: {date_str}")
            return date_str
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге даты {date_str}: {str(e)}")
            return date_str

    def store_vectors(
        self,
        vectors: List[List[float]],
        texts: List[str],
        metadata: List[Dict[str, Any]]
    ) -> bool:
        """
        Сохраняет векторы в коллекцию с разбиением на батчи
        
        Args:
            vectors: Список векторов
            texts: Список текстов
            metadata: Список метаданных
            
        Returns:
            bool: True если сохранение прошло успешно, False в противном случае
        """
        try:
            if not vectors or not texts or not metadata:
                logger.warning("Пустые данные для сохранения")
                return False
            
            # Максимальный размер батча (примерно 25MB для безопасности)
            MAX_BATCH_SIZE = 100
            total_points = len(vectors)
            processed_points = 0
            batch_number = 1
            
            logger.info(f"Начало сохранения {total_points} векторов")
            
            while processed_points < total_points:
                # Определяем размер текущего батча
                current_batch_size = min(MAX_BATCH_SIZE, total_points - processed_points)
                end_index = processed_points + current_batch_size
                
                logger.info(f"Обработка батча #{batch_number}: {current_batch_size} векторов")
                
                # Формируем точки для текущего батча
                points = []
                for i in range(processed_points, end_index):
                    vector = vectors[i]
                    text = texts[i]
                    meta = metadata[i]
                    
                    # Создаем UUID для точки
                    point_id = str(uuid.uuid4())
                    
                    # Преобразуем дату в нужный формат
                    date_str = meta.get("date", "")
                    formatted_date = self._parse_date(date_str)
                    
                    point = models.PointStruct(
                        id=point_id,
                        vector=vector,
                        payload={
                            "text": text,
                            "url": meta.get("url", ""),
                            "title": meta.get("title", ""),
                            "category": meta.get("category", ""),
                            "date": formatted_date,
                            "date_timestamp": datetime.strptime(formatted_date, "%Y-%m-%d").timestamp() if formatted_date else None,
                            "source_type": meta.get("source_type", ""),
                            "chunk_index": i,
                            "total_chunks": total_points,
                            "created_at": datetime.now().isoformat()
                        }
                    )
                    points.append(point)
                
                # Сохраняем текущий батч
                try:
                    self.client.upsert(
                        collection_name=self.collection_name,
                        points=points
                    )
                    
                    # Принудительно запускаем индексацию для батча
                    self.client.update_collection(
                        collection_name=self.collection_name,
                        optimizers_config=models.OptimizersConfigDiff(
                            indexing_threshold=0
                        )
                    )
                    
                    logger.info(f"Батч #{batch_number} успешно сохранен")
                    
                except Exception as e:
                    logger.error(f"Ошибка при сохранении батча #{batch_number}: {str(e)}")
                    # Если произошла ошибка из-за размера payload, уменьшаем размер батча
                    if "Payload error: JSON payload" in str(e):
                        MAX_BATCH_SIZE = max(1, MAX_BATCH_SIZE // 2)
                        logger.warning(f"Уменьшаем размер батча до {MAX_BATCH_SIZE}")
                        continue
                    return False
                
                processed_points = end_index
                batch_number += 1
                
                # Логируем прогресс
                remaining_points = total_points - processed_points
                logger.info(f"Прогресс: {processed_points}/{total_points} векторов сохранено, осталось {remaining_points}")
            
            logger.info(f"Сохранение завершено: {processed_points} векторов сохранено в {batch_number-1} батчах")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при сохранении векторов: {str(e)}")
            return False

    def search_vectors(
        self,
        query_vector: List[float],
        score_threshold: float = 0.7,
        limit: Optional[int] = None,
        category: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[dict]:
        """
        Поиск векторов в Qdrant с фильтрацией по релевантности и метаданным.
        
        Args:
            query_vector: Вектор запроса.
            score_threshold: Минимальный порог схожести (0.0 - 1.0).
            limit: Максимальное количество результатов. Если None, возвращаются все релевантные результаты.
            category: Фильтр по категории.
            start_date: Начальная дата.
            end_date: Конечная дата.
            
        Returns:
            List[dict]: Релевантные материалы.
        """
        try:
            # Подготовка фильтра для Qdrant
            filters = []
            if category:
                filters.append(
                    FieldCondition(
                        key="category",
                        match={"value": category}
                    )
                )
            if start_date and end_date:
                filters.append(
                    FieldCondition(
                        key="date",
                        range={
                            "gte": start_date.isoformat(),
                            "lte": end_date.isoformat()
                        }
                    )
                )
            
            # Поиск в Qdrant с учетом порога релевантности
            search_result = self.client.search(
                collection_name=self.collection_name,
                query_vector=query_vector,
                query_filter=Filter(must=filters) if filters else None,
                score_threshold=score_threshold,
                limit=limit if limit is not None else 1000000  # Если limit не указан, используем максимальное значение
            )
            
            # Преобразование результатов в нужный формат
            return [
                {
                    "id": hit.id,
                    "text": hit.payload.get("text", ""),
                    "title": hit.payload.get("title", ""),
                    "date": hit.payload.get("date"),
                    "category": hit.payload.get("category"),
                    "score": hit.score,  # Для отладки
                    "url": hit.payload.get("url", "")  # Добавлено поле url
                }
                for hit in search_result
            ]
        except Exception as e:
            logger.error(f"Ошибка при поиске векторов: {str(e)}")
            return []

    def delete_vectors(self, filter_conditions: Dict[str, Any]) -> bool:
        """
        Удаляет векторы по условиям фильтра
        
        Args:
            filter_conditions: Условия фильтрации
            
        Returns:
            bool: True если успешно, False в случае ошибки
        """
        try:
            logger.info(f"Удаление векторов с условиями: {filter_conditions}")
            
            self.client.delete(
                collection_name=self.collection_name,
                points_selector=models.FilterSelector(
                    filter=models.Filter(
                        must=[
                            models.FieldCondition(
                                key=key,
                                match=models.MatchValue(value=value)
                            )
                            for key, value in filter_conditions.items()
                        ]
                    )
                )
            )
            
            logger.info("Векторы успешно удалены")
            return True
        except Exception as e:
            logger.error(f"Ошибка при удалении векторов: {str(e)}")
            return False

    def get_categories(self) -> List[str]:
        """
        Получение списка уникальных категорий из коллекции
        
        Returns:
            List[str]: Список категорий
        """
        try:
            # Получаем все точки из коллекции
            points = self.client.scroll(
                collection_name=self.collection_name,
                limit=10000  # Максимальное количество точек
            )[0]
            
            # Извлекаем уникальные категории
            categories = set()
            for point in points:
                category = point.payload.get("category", "")
                if category:
                    categories.add(category)
            
            logger.info(f"Найдено {len(categories)} уникальных категорий")
            return sorted(list(categories))
            
        except Exception as e:
            logger.error(f"Ошибка при получении категорий: {str(e)}")
            return []

    def add_materials(self, materials: List[Dict[str, Any]]) -> bool:
        """
        Добавляет материалы в векторное хранилище
        
        Args:
            materials: Список материалов для добавления
            
        Returns:
            bool: True если добавление прошло успешно, False в противном случае
        """
        try:
            if not materials:
                logger.warning("Пустой список материалов для добавления")
                return False
            
            vectors = []
            texts = []
            metadata = []
            
            for material in materials:
                # Формируем текст для векторизации
                text = f"{material.get('title', '')} {material.get('description', '')} {material.get('content', '')}"
                texts.append(text)
                
                # Формируем метаданные
                meta = {
                    'url': material.get('url', ''),
                    'title': material.get('title', ''),
                    'description': material.get('description', ''),
                    'content': material.get('content', ''),
                    'date': material.get('date', ''),
                    'category': material.get('category', ''),
                    'source_type': material.get('source_type', '')
                }
                metadata.append(meta)
            
            # Получаем векторы для текстов
            vectors = self.text_processor.get_embeddings(texts)
            
            # Сохраняем векторы
            return self.store_vectors(vectors, texts, metadata)
            
        except Exception as e:
            logger.error(f"Ошибка при добавлении материалов: {str(e)}")
            return False

    def upsert_vector(
        self,
        vector: List[float],
        payload: Dict[str, Any],
        vector_id: Optional[int] = None
    ) -> bool:
        """
        Добавление или обновление вектора в хранилище.
        
        Args:
            vector: Вектор для добавления.
            payload: Метаданные (текст, заголовок, дата, категория и т.д.).
            vector_id: ID вектора (опционально).
            
        Returns:
            bool: Успешно ли выполнена операция.
        """
        try:
            self.client.upsert(
                collection_name=self.collection_name,
                points=[
                    {
                        "id": vector_id or payload.get("id"),
                        "vector": vector,
                        "payload": payload
                    }
                ]
            )
            return True
        except Exception as e:
            logger.error(f"Ошибка при добавлении вектора: {str(e)}")
            return False

    def recreate_collection(self) -> bool:
        """
        Пересоздает коллекцию с новыми параметрами
        
        Returns:
            bool: True если успешно, False в случае ошибки
        """
        try:
            # Удаляем существующую коллекцию
            self.client.delete_collection(collection_name=self.collection_name)
            logger.info(f"Коллекция {self.collection_name} удалена")
            
            # Создаем новую коллекцию
            self._create_collection_if_not_exists()
            logger.info(f"Коллекция {self.collection_name} пересоздана с размерностью {self.vector_size}")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка при пересоздании коллекции: {str(e)}")
            return False

    def search_by_category_and_date(
        self,
        category: str,
        start_date: datetime
    ) -> List[dict]:
        """
        Поиск материалов по категории и дате
        
        Args:
            category: Категория для поиска
            start_date: Дата для поиска
            
        Returns:
            List[dict]: Список найденных материалов
        """
        try:
            # Форматируем дату в строку YYYY-MM-DD
            date_str = start_date.strftime('%Y-%m-%d')
            
            # Создаем фильтр для поиска
            filter_conditions = Filter(
                must=[
                    FieldCondition(
                        key="category",
                        match=models.MatchValue(value=category)
                    ),
                    FieldCondition(
                        key="date",
                        match=models.MatchValue(value=date_str)
                    )
                ]
            )
            
            # Выполняем поиск
            search_result = self.client.scroll(
                collection_name=self.collection_name,
                scroll_filter=filter_conditions,
                with_payload=True,
                with_vectors=False,
                limit=1000000  # Устанавливаем большой лимит, чтобы получить все результаты
            )
            
            # Преобразуем результаты в нужный формат
            results = []
            for point in search_result[0]:
                results.append({
                    'text': point.payload.get('text', ''),
                    'url': point.payload.get('url', ''),
                    'title': point.payload.get('title', ''),
                    'category': point.payload.get('category', ''),
                    'date': point.payload.get('date', ''),
                    'source_type': point.payload.get('source_type', '')
                })
            
            logger.info(f"Найдено {len(results)} материалов для категории {category} за {date_str}")
            return results
            
        except Exception as e:
            logger.error(f"Ошибка при поиске по категории и дате: {str(e)}")
            return [] 

    def search_by_category_and_date_range(
        self,
        category: str,
        start_date: datetime,
        end_date: datetime
    ) -> List[dict]:
        """
        Поиск материалов по категории и диапазону дат
        
        Args:
            category: Категория для поиска
            start_date: Начальная дата диапазона
            end_date: Конечная дата диапазона
            
        Returns:
            List[dict]: Список найденных материалов
        """
        try:
            # Форматируем даты в строки YYYY-MM-DD
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
            
            # Создаем фильтр для поиска
            filter_conditions = Filter(
                must=[
                    FieldCondition(
                        key="category",
                        match=models.MatchValue(value=category)
                    ),
                    FieldCondition(
                        key="date",
                        range={
                            "gte": start_date_str,
                            "lte": end_date_str
                        }
                    )
                ]
            )
            
            # Выполняем поиск
            search_result = self.client.scroll(
                collection_name=self.collection_name,
                scroll_filter=filter_conditions,
                with_payload=True,
                with_vectors=False,
                limit=1000000  # Устанавливаем большой лимит, чтобы получить все результаты
            )
            
            # Преобразуем результаты в нужный формат
            results = []
            for point in search_result[0]:
                results.append({
                    'text': point.payload.get('text', ''),
                    'url': point.payload.get('url', ''),
                    'title': point.payload.get('title', ''),
                    'category': point.payload.get('category', ''),
                    'date': point.payload.get('date', ''),
                    'source_type': point.payload.get('source_type', '')
                })
            
            logger.info(f"Найдено {len(results)} материалов для категории {category} за период {start_date_str} - {end_date_str}")
            return results
            
        except Exception as e:
            logger.error(f"Ошибка при поиске по категории и диапазону дат: {str(e)}")
            return [] 