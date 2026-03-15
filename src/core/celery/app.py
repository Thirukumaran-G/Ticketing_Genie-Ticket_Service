"""
Celery application factory.
src/core/celery/app.py
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
        "src.core.celery.workers.email_inbound_worker",   # ← NEW
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
        "ticket.ai.classify":              {"queue": "ai_tasks"},
        "ticket.ai.draft":                 {"queue": "ai_tasks"},
        "ticket.ai.auto_assign":           {"queue": "ai_tasks"},
        "ticket.ai.priority_override":     {"queue": "ai_tasks"},
        # Outbound email
        "ticket.email.send_ack":           {"queue": "email_tasks"},
        "ticket.email.send_notification":  {"queue": "email_tasks"},
        # Inbound email  ← NEW
        "ticket.email.poll_inbox":         {"queue": "email_inbound"},
        "ticket.email.process_inbound":    {"queue": "email_inbound"},
        # SLA
        "ticket.sla.check_breaches":       {"queue": "sla_tasks"},
        "ticket.sla.escalate_critical":    {"queue": "sla_tasks"},
        "ticket.sla.auto_close":           {"queue": "sla_tasks"},
        # Notification
        "ticket.notification.alert_tls":   {"queue": "sla_tasks"},
    },
    beat_schedule={
        # Existing — untouched
        "sla-breach-check-every-minute": {
            "task":     "ticket.sla.check_breaches",
            "schedule": settings.SLA_CHECK_INTERVAL_SECONDS,
        },
        "escalate-critical-every-5-minutes": {
            "task":     "ticket.sla.escalate_critical",
            "schedule": crontab(minute="*/5"),
        },
        "auto-close-resolved-tickets-every-hour": {
            "task":     "ticket.sla.auto_close",
            "schedule": crontab(minute=0),
        },
        # New — inbound email poll every 20 s
        "poll-imap-inbox-every-20s": {
            "task":     "ticket.email.poll_inbox",
            "schedule": 20.0,
        },
    },
)