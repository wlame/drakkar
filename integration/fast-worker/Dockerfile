# Overridable so the harness can run on a release-candidate interpreter
# (e.g. python:3.15-rc-slim); the default is the stable lane's image.
ARG PYTHON_IMAGE=python:3.13-slim
FROM ${PYTHON_IMAGE}

# Extra apt packages for source-building C extensions — empty on stable
# images (wheels exist), set by the experimental lane where cp-next wheels
# are not published yet (e.g. "build-essential librdkafka-dev").
ARG EXTRA_APT=""
RUN if [ -n "$EXTRA_APT" ]; then \
    apt-get update && \
    apt-get install -y --no-install-recommends $EXTRA_APT && \
    rm -rf /var/lib/apt/lists/*; fi

RUN pip install uv

WORKDIR /app

RUN uv pip install --system --no-cache-dir \
    confluent-kafka pydantic pydantic-settings asyncpg \
    prometheus-client structlog pyyaml \
    aiosqlite fastapi 'uvicorn[standard]' \
    pymongo httpx redis

COPY drakkar/ /app/drakkar/
COPY integration/fast-worker/main.py /app/main.py
COPY integration/fast-worker/handler.py /app/handler.py
COPY integration/fast-worker/models.py /app/models.py
COPY integration/fast-worker/drakkar.yaml /app/drakkar.yaml
COPY integration/fast-worker/count-symbols.sh /usr/local/bin/count-symbols

RUN chmod +x /usr/local/bin/count-symbols

CMD ["python", "main.py"]
