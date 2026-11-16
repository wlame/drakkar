# Overridable so the harness can run on a release-candidate interpreter
# (e.g. python:3.15-rc-slim); the default is the stable lane's image.
ARG PYTHON_IMAGE=python:3.13-slim
FROM ${PYTHON_IMAGE}

# Extra apt packages, empty on every lane today (see the fast worker's
# Dockerfile).
ARG EXTRA_APT=""
RUN if [ -n "$EXTRA_APT" ]; then \
    apt-get update && \
    apt-get install -y --no-install-recommends $EXTRA_APT && \
    rm -rf /var/lib/apt/lists/*; fi

RUN pip install uv

WORKDIR /app

# The same list as the fast worker's image, so both share this layer.
RUN uv pip install --system --no-cache-dir \
    confluent-kafka pydantic pydantic-settings asyncpg \
    prometheus-client structlog pyyaml \
    aiosqlite fastapi 'uvicorn[standard]' \
    pymongo httpx redis 'cronsim==2.7'

COPY drakkar/ /app/drakkar/
COPY integration/stream-worker/main.py /app/main.py
COPY integration/stream-worker/handler.py /app/handler.py
COPY integration/stream-worker/drakkar.yaml /app/drakkar.yaml
COPY integration/stream-worker/stream-task.sh /usr/local/bin/stream-task

RUN chmod +x /usr/local/bin/stream-task

CMD ["python", "main.py"]
