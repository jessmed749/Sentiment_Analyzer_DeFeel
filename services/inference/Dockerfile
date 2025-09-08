FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential curl supervisor && rm -rf /var/lib/apt/lists/*

RUN useradd -m appuser
WORKDIR /app

ENV PIP_NO_CACHE_DIR=1 PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

# Set HF_HOME to a directory that appuser can access
ENV HF_HOME=/app/.hf_cache
ENV TRANSFORMERS_CACHE=/app/.hf_cache

# Copy requirements first for better Docker layer caching
COPY requirements.txt /app/requirements.txt

RUN python -m pip install --upgrade pip setuptools wheel && \
    python -m pip install torch==2.4.1 --index-url https://download.pytorch.org/whl/cpu && \
    python -m pip install -r /app/requirements.txt

COPY . /app/

RUN mkdir -p /app/.hf_cache && chown -R appuser:appuser /app/.hf_cache

# Pre-cache the model during build (as root, then fix permissions)
ENV MODEL_PATH=distilbert-base-uncased-finetuned-sst-2-english
RUN python - <<'PY'
from transformers import pipeline
import os
nlp = pipeline("sentiment-analysis", model=os.environ.get("MODEL_PATH"))
print("Model cached.")
PY

RUN chown -R appuser:appuser /app/.hf_cache

RUN mkdir -p /etc/supervisor/conf.d
COPY <<EOF /etc/supervisor/conf.d/supervisord.conf
[supervisord]
nodaemon=true
user=root

[program:fastapi]
command=uvicorn app.main:app --host 0.0.0.0 --port 8000
directory=/app
autostart=true
autorestart=true
user=appuser
stdout_logfile=/dev/stdout
stdout_logfile_maxbytes=0
stderr_logfile=/dev/stderr
stderr_logfile_maxbytes=0
environment=HF_HOME="/app/.hf_cache",TRANSFORMERS_CACHE="/app/.hf_cache"

[program:worker]
command=python worker/worker.py
directory=/app
autostart=true
autorestart=true
user=appuser
stdout_logfile=/dev/stdout
stdout_logfile_maxbytes=0
stderr_logfile=/dev/stderr
stderr_logfile_maxbytes=0
startretries=10
environment=HF_HOME="/app/.hf_cache",TRANSFORMERS_CACHE="/app/.hf_cache"
EOF

RUN chown -R appuser:appuser /app

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD curl -fsS http://127.0.0.1:8000/health || exit 1

# Use supervisor to run both FastAPI and Kafka worker
CMD ["/usr/bin/supervisord", "-c", "/etc/supervisor/conf.d/supervisord.conf"]