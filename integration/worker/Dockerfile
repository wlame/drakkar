FROM python:3.13-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ripgrep bc && \
    rm -rf /var/lib/apt/lists/*

RUN pip install uv

WORKDIR /app

RUN uv pip install --system --no-cache-dir \
    confluent-kafka pydantic pydantic-settings asyncpg \
    prometheus-client structlog pyyaml \
    aiosqlite fastapi 'uvicorn[standard]' jinja2 \
    motor httpx redis

COPY drakkar/ /app/drakkar/
COPY integration/worker/main.py /app/main.py
COPY integration/worker/handler.py /app/handler.py
COPY integration/worker/models.py /app/models.py
COPY integration/worker/metrics.py /app/metrics.py
COPY integration/worker/drakkar.yaml /app/drakkar.yaml
COPY integration/worker/run-rg.sh /usr/local/bin/run-rg
COPY integration/worker/entrypoint.sh /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
CMD ["python", "main.py"]
