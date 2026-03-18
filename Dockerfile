FROM python:3.11-slim

RUN pip install uv

WORKDIR /app

COPY pyproject.toml uv.lock ./

# Install CPU only — skip torch/nvidia/cuda junk
RUN uv sync --frozen --no-dev \
    --extra-index-url https://download.pytorch.org/whl/cpu

COPY . .

ENV PATH="/app/.venv/bin:$PATH"

RUN chmod +x /app/celery_worker.sh

CMD ["python", "-m", "src.main"]