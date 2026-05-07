FROM python:3.13-slim

WORKDIR /app

# Install only ``httpx`` from a frozen requirements file. ``--no-deps`` would be
# stricter (transitive deps must be listed explicitly), but httpx pulls in
# anyio/sniffio/idna/h11/certifi which are required for the client to function.
# We rely on the pinned httpx version + base image determinism for supply-chain
# protection — no other packages are added here.
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --require-hashes=false -r /app/requirements.txt

COPY loop.py /app/loop.py

# Run unbuffered so stdout reaches ``docker logs`` immediately. Without this
# flag the per-iteration log line would be buffered until the container exits,
# which would defeat the purpose of a 10-second polling demo.
ENV PYTHONUNBUFFERED=1

CMD ["python", "loop.py"]
