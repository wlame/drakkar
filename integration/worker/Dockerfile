# Overridable so the harness can run on a release-candidate interpreter
# (e.g. python:3.15-rc-slim); the default is the stable lane's image.
ARG PYTHON_IMAGE=python:3.13-slim
FROM ${PYTHON_IMAGE}

# Extra apt packages, empty on every lane today. The experimental lane gets
# its C-extension toolchain and librdkafka from the base image it builds on
# (integration/infra/Dockerfile.harness-base) rather than from here: an apt
# librdkafka is too old to compile confluent-kafka against.
ARG EXTRA_APT=""
RUN if [ -n "$EXTRA_APT" ]; then \
    apt-get update && \
    apt-get install -y --no-install-recommends $EXTRA_APT && \
    rm -rf /var/lib/apt/lists/*; fi

RUN apt-get update && \
    apt-get install -y --no-install-recommends ripgrep bc && \
    rm -rf /var/lib/apt/lists/*

RUN pip install uv

WORKDIR /app

RUN uv pip install --system --no-cache-dir \
    confluent-kafka pydantic pydantic-settings asyncpg \
    prometheus-client structlog pyyaml \
    aiosqlite fastapi 'uvicorn[standard]' \
    pymongo httpx redis

COPY drakkar/ /app/drakkar/
COPY integration/worker/main.py /app/main.py
COPY integration/worker/handler.py /app/handler.py
COPY integration/worker/http_handler.py /app/http_handler.py
COPY integration/worker/models.py /app/models.py
COPY integration/worker/metrics.py /app/metrics.py
COPY integration/worker/drakkar.yaml /app/drakkar.yaml
COPY integration/http-worker/drakkar.yaml /app/http-worker.yaml
COPY integration/worker/custom-renderers.js /app/custom-renderers.js
COPY integration/worker/run-rg.sh /usr/local/bin/run-rg

# run-rg searches executor.env.SEARCH_CORPUS (/tmp/search-corpus), and the
# handler's health check expects the directory to exist. Stage a corpus
# here so match counts are real instead of always zero.
COPY integration/worker/ /tmp/search-corpus/

CMD ["python", "main.py"]
