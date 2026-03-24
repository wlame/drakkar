FROM python:3.13-slim

RUN pip install uv

WORKDIR /app

RUN uv pip install --system --no-cache-dir \
    confluent-kafka pydantic pydantic-settings asyncpg \
    prometheus-client structlog pyyaml \
    aiosqlite fastapi 'uvicorn[standard]' jinja2 \
    motor httpx redis

COPY drakkar/ /app/drakkar/
COPY integration/fast-worker/main.py /app/main.py
COPY integration/fast-worker/handler.py /app/handler.py
COPY integration/fast-worker/models.py /app/models.py
COPY integration/fast-worker/drakkar.yaml /app/drakkar.yaml
COPY integration/fast-worker/count-symbols.sh /usr/local/bin/count-symbols

RUN chmod +x /usr/local/bin/count-symbols

CMD ["python", "main.py"]
