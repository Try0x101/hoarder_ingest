from celery import Celery
from celery.schedules import crontab

celery_app = Celery(
    'hoarder_ingest',
    broker="redis://localhost:6378/0",
    backend="redis://localhost:6378/1",
    include=['app.tasks']
)

celery_app.conf.update(
    task_track_started=True,
    broker_connection_retry_on_startup=True,
    task_serializer='json',
    result_serializer='json',
    accept_content=['json'],
    beat_schedule={
        'cleanup-db-every-6-hours': {
            'task': 'tasks.cleanup_db',
            'schedule': crontab(minute=0, hour='*/6'),
        },
        'monitor-system-every-15-seconds': {
            'task': 'tasks.monitor_system',
            'schedule': 15.0,
        },
    },
)
