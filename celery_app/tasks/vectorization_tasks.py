from celery_app import app
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)

@app.task(name='vectorization_tasks.update_vectors')
def update_vectors():
    """
    Задача для обновления векторов
    """
    try:
        logger.info("Начало обновления векторов")
        # TODO: Добавить логику из vector_store.py
        logger.info("Векторы успешно обновлены")
        return {"status": "success", "message": "Векторы обновлены"}
    except Exception as e:
        logger.error(f"Ошибка при обновлении векторов: {str(e)}")
        raise 