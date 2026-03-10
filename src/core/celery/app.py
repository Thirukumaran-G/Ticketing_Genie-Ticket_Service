"""
Celery application factory.
src/core/celery/app.py

All tasks registered by importing worker modules.
Beat schedule defined here.
"""

from __future__ import annotations

from celery import Celery
from celery.schedules import crontab

from src.config.settings import settings

celery_app = Celery(
    "ticket_service",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=[
        "src.core.celery.workers.ai_worker",
        "src.core.celery.workers.email_worker",
        "src.core.celery.workers.sla_worker",
        "src.core.celery.workers.notification_worker",
    ],
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    broker_connection_retry_on_startup=True,
    task_routes={
        # AI
        "ticket.ai.classify":            {"queue": "ai_tasks"},
        "ticket.ai.draft":               {"queue": "ai_tasks"},
        "ticket.ai.auto_assign":         {"queue": "ai_tasks"},
        "ticket.ai.priority_override":   {"queue": "ai_tasks"},
        # Email
        "ticket.email.send_ack":         {"queue": "email_tasks"},
        "ticket.email.send_notification":{"queue": "email_tasks"},
        # SLA
        "ticket.sla.check_breaches":     {"queue": "sla_tasks"},
        "ticket.sla.escalate_critical":  {"queue": "sla_tasks"},
        "ticket.sla.auto_close":         {"queue": "sla_tasks"},
        # Notification
        "ticket.notification.alert_tls": {"queue": "sla_tasks"},
    },
    beat_schedule={
        "sla-breach-check-every-minute": {
            "task": "ticket.sla.check_breaches",
            "schedule": settings.SLA_CHECK_INTERVAL_SECONDS,
        },
        "escalate-critical-every-5-minutes": {
            "task": "ticket.sla.escalate_critical",
            "schedule": crontab(minute="*/5"),
        },
        "auto-close-resolved-tickets-every-hour": {
            "task": "ticket.sla.auto_close",
            "schedule": crontab(minute=0),
        },
    },
)