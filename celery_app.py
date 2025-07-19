from celery import Celery

celery_app = Celery(
    'hoarder_ingest',
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/1",
    include=['app.tasks']
)

celery_app.conf.update(
    task_track_started=True,
    broker_connection_retry_on_startup=True,
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
)
