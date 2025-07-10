import logging
from typing import List
import numpy as np
from langchain_ollama import OllamaEmbeddings
from langchain_openai import OpenAIEmbeddings
from dotenv import load_dotenv
import os

# Загружаем переменные окружения
load_dotenv()

logger = logging.getLogger(__name__)

class TextProcessor:
    def __init__(
        self,
        embedding_type: str = "ollama",  # "ollama" или "openai"
        model_name: str = "llama3.2:latest",  # для ollama
        openai_model: str = "text-embedding-3-small",  # для openai
        base_url: str = "http://localhost:11434"
    ):
        """
        Инициализация процессора текста
        
        Args:
            embedding_type: Тип эмбеддингов ("ollama" или "openai")
            model_name: Название модели для Ollama
            openai_model: Название модели для OpenAI (text-embedding-3-small, text-embedding-3-large, text-embedding-ada-002)
            base_url: URL для Ollama API
        """
        self.embedding_type = embedding_type
        self.model_name = model_name
        self.openai_model = openai_model
        self.base_url = base_url
        
        # Инициализация модели эмбеддингов
        if embedding_type == "ollama":
            try:
                self.model = OllamaEmbeddings(
                    model=model_name,
                    base_url=base_url
                )
                self.vector_size = 3072  # Размерность для Ollama
                
                # Проверяем работу модели на тестовом запросе
                test_embedding = self.model.embed_query("test")
                embedding_size = len(test_embedding)
                logger.info(f"Тестовая векторизация Ollama успешна")
                logger.info(f"Размерность эмбеддингов: {embedding_size}")
                
                # Проверяем, соответствует ли размерность ожидаемой
                if embedding_size != self.vector_size:
                    logger.warning(f"Размерность эмбеддингов ({embedding_size}) отличается от ожидаемой ({self.vector_size})")
                    logger.warning("Возможно, потребуется пересоздать коллекцию в Qdrant с правильной размерностью")
                
            except Exception as e:
                logger.warning(f"Не удалось инициализировать Ollama: {str(e)}")
                logger.info("Переключаемся на OpenAI эмбеддинги")
                self._init_openai()
        elif embedding_type == "openai":
            self._init_openai()
        else:
            raise ValueError(f"Неподдерживаемый тип эмбеддингов: {embedding_type}")
        
        logger.info(f"Инициализирован TextProcessor с моделью {self.model_name} (тип: {self.embedding_type})")
    
    def _init_openai(self):
        """Инициализация OpenAI эмбеддингов"""
        if not os.getenv("OPENAI_API_KEY"):
            raise ValueError("OPENAI_API_KEY не найден в переменных окружения")
        
        self.model = OpenAIEmbeddings(
            model=self.openai_model,
            openai_api_key=os.getenv("OPENAI_API_KEY")
        )
        self.embedding_type = "openai"
        self.model_name = self.openai_model
        
        # Размерность зависит от модели OpenAI
        if self.openai_model == "text-embedding-3-small":
            self.vector_size = 1536
        elif self.openai_model == "text-embedding-3-large":
            self.vector_size = 3072
        elif self.openai_model == "text-embedding-ada-002":
            self.vector_size = 1536
        else:
            raise ValueError(f"Неподдерживаемая модель OpenAI: {self.openai_model}")
        
        # Проверяем работу модели на тестовом запросе
        try:
            test_embedding = self.model.embed_query("test")
            embedding_size = len(test_embedding)
            logger.info(f"Тестовая векторизация OpenAI успешна")
            logger.info(f"Размерность эмбеддингов: {embedding_size}")
            
            # Проверяем, соответствует ли размерность ожидаемой
            if embedding_size != self.vector_size:
                logger.warning(f"Размерность эмбеддингов ({embedding_size}) отличается от ожидаемой ({self.vector_size})")
                logger.warning("Возможно, потребуется пересоздать коллекцию в Qdrant с правильной размерностью")
            
        except Exception as e:
            raise RuntimeError(f"Ошибка при тестовой векторизации OpenAI: {str(e)}")
    
    def _count_tokens(self, text: str) -> int:
        """
        Подсчитывает количество токенов в тексте в зависимости от типа эмбеддингов
        
        Args:
            text: Текст для подсчета токенов
            
        Returns:
            int: Количество токенов
        """
        try:
            if self.embedding_type == "openai":
                # Для OpenAI используем tiktoken
                import tiktoken
                encoding = tiktoken.encoding_for_model("text-embedding-3-small")
                return len(encoding.encode(text))
            else:
                # Для Ollama используем приблизительный подсчет (4 символа ~ 1 токен)
                return len(text) // 4
        except Exception as e:
            logger.warning(f"Ошибка при подсчете токенов: {str(e)}")
            # В случае ошибки возвращаем приблизительное значение
            return len(text) // 4

    def create_embeddings(self, texts: List[str]) -> List[np.ndarray]:
        """
        Создает эмбеддинги для списка текстов с разбиением на батчи по токенам
        
        Args:
            texts: Список текстов
            
        Returns:
            List[np.ndarray]: Список эмбеддингов
        """
        try:
            if not texts:
                return []
            
            total_texts = len(texts)
            logger.info(f"Начало векторизации {total_texts} текстов")
            
            # Максимальное количество токенов в одном запросе
            MAX_TOKENS_PER_REQUEST = 300000
            
            all_embeddings = []
            current_batch = []
            current_batch_tokens = 0
            processed_texts = 0
            batch_number = 1
            
            for text in texts:
                # Подсчитываем токены для текущего текста
                text_tokens = self._count_tokens(text)
                
                # Если добавление текста превысит лимит токенов, обрабатываем текущий батч
                if current_batch_tokens + text_tokens > MAX_TOKENS_PER_REQUEST:
                    if current_batch:
                        logger.info(f"Батч #{batch_number}: обработка {len(current_batch)} текстов ({current_batch_tokens} токенов)")
                        batch_embeddings = self.model.embed_documents(current_batch)
                        all_embeddings.extend(batch_embeddings)
                        processed_texts += len(current_batch)
                        remaining_texts = total_texts - processed_texts
                        logger.info(f"Прогресс: {processed_texts}/{total_texts} текстов обработано, осталось {remaining_texts}")
                        current_batch = []
                        current_batch_tokens = 0
                        batch_number += 1
                
                # Добавляем текст в текущий батч
                current_batch.append(text)
                current_batch_tokens += text_tokens
            
            # Обрабатываем оставшиеся тексты
            if current_batch:
                logger.info(f"Финальный батч #{batch_number}: обработка {len(current_batch)} текстов ({current_batch_tokens} токенов)")
                batch_embeddings = self.model.embed_documents(current_batch)
                all_embeddings.extend(batch_embeddings)
                processed_texts += len(current_batch)
                logger.info(f"Прогресс: {processed_texts}/{total_texts} текстов обработано")
            
            # Проверяем размерность первого эмбеддинга
            if all_embeddings and len(all_embeddings) > 0:
                first_embedding = all_embeddings[0]
                embedding_size = len(first_embedding)
                logger.info(f"Размерность эмбеддингов: {embedding_size}")
            
            # Преобразуем в список numpy массивов
            embeddings = [np.array(embedding) for embedding in all_embeddings]
            
            logger.info(f"Векторизация завершена: создано {len(embeddings)} эмбеддингов в {batch_number} батчах")
            return embeddings
            
        except Exception as e:
            logger.error(f"Ошибка при создании эмбеддингов: {str(e)}")
            return []
    
    def get_embeddings(self, texts: List[str]) -> List[np.ndarray]:
        """
        Алиас для метода create_embeddings для обратной совместимости
        
        Args:
            texts: Список текстов
            
        Returns:
            List[np.ndarray]: Список эмбеддингов
        """
        return self.create_embeddings(texts) 