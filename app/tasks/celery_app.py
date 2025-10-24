from celery import Celery
from app.config import settings

celery_app = Celery(
    "events_analytics",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_default_retry_delay=60,
    task_max_retries=3,
    broker_connection_retry_on_startup=True,
    imports=('app.tasks.workers',)
)

celery_app.conf.beat_schedule = {
    "cleanup-hot-events": {
        "task": "app.tasks.workers.cleanup_hot_events",
        "schedule": 86400.0,
    }
}