FROM python:3.12-slim

WORKDIR /app

#  torch (CPU) from the official index, then the rest
RUN python -m pip install --no-cache-dir --upgrade pip setuptools wheel && \
    python -m pip install --no-cache-dir torch==2.4.1 --index-url https://download.pytorch.org/whl/cpu && \
    python -m pip install --no-cache-dir transformers kafka-python python-dotenv

COPY inference/ /app/

# Default to run the worker
CMD ["python", "worker.py"]
