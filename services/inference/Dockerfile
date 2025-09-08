FROM python:3.12-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential curl && rm -rf /var/lib/apt/lists/*

RUN useradd -m appuser
WORKDIR /app

ENV PIP_NO_CACHE_DIR=1 PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 TRANSFORMERS_CACHE=/app/.hf_cache

# When building from inside inference/, these are local files:
COPY requirements.txt /app/requirements.txt

RUN python -m pip install --upgrade pip setuptools wheel && \
    python -m pip install torch==2.4.1 --index-url https://download.pytorch.org/whl/cpu && \
    python -m pip install -r /app/requirements.txt

COPY . /app/

ENV MODEL_PATH=distilbert-base-uncased-finetuned-sst-2-english
RUN python - <<'PY'
from transformers import pipeline
import os
nlp = pipeline("sentiment-analysis", model=os.environ.get("MODEL_PATH"))
print("Model cached.")
PY

EXPOSE 8000
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD curl -fsS http://127.0.0.1:8000/health || exit 1

USER appuser
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
