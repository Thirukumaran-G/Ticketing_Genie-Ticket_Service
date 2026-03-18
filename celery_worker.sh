#!/bin/bash
exec uv run celery -A src.core.celery.app worker \
  -Q ai_tasks,email_tasks,sla_tasks,email_inbound \
  --loglevel=info \
  --pool=prefork \
  --concurrency=4