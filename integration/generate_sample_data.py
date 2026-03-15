"""Generate sample text files for integration testing."""

import os
import random

SAMPLE_DIR = os.path.join(os.path.dirname(__file__), "sample_data")
NUM_FILES = 10
LINES_PER_FILE = 500

LOG_TEMPLATES = [
    "2024-01-{day:02d} {hour:02d}:{minute:02d}:{second:02d} INFO  Starting service on port {port}",
    "2024-01-{day:02d} {hour:02d}:{minute:02d}:{second:02d} DEBUG Processing request {req_id}",
    "2024-01-{day:02d} {hour:02d}:{minute:02d}:{second:02d} WARNING Connection timeout after {ms}ms",
    "2024-01-{day:02d} {hour:02d}:{minute:02d}:{second:02d} ERROR  Exception in handler: {error}",
    "2024-01-{day:02d} {hour:02d}:{minute:02d}:{second:02d} INFO  Request completed successfully in {ms}ms",
    "2024-01-{day:02d} {hour:02d}:{minute:02d}:{second:02d} DEBUG Database query returned {count} rows",
    "2024-01-{day:02d} {hour:02d}:{minute:02d}:{second:02d} INFO  User {user} logged in from {ip}",
    "2024-01-{day:02d} {hour:02d}:{minute:02d}:{second:02d} ERROR  Failed to connect to upstream: connection refused",
    "2024-01-{day:02d} {hour:02d}:{minute:02d}:{second:02d} WARNING Rate limit exceeded for client {client}",
    "2024-01-{day:02d} {hour:02d}:{minute:02d}:{second:02d} INFO  Cache hit ratio: {ratio:.1f}%",
    "class UserService:",
    "    def __init__(self, db_pool):",
    "        self.db_pool = db_pool",
    "    async def get_user(self, user_id: int):",
    "        return await self.db_pool.fetchrow('SELECT * FROM users WHERE id = $1', user_id)",
    "    def validate_token(self, token: str) -> bool:",
    "        try:",
    "            return self._decode(token) is not None",
    "        except Exception as e:",
    "            raise ValueError(f'Invalid token: {{e}}')",
    "import asyncio",
    "from dataclasses import dataclass",
    "async def process_batch(items):",
    "    results = await asyncio.gather(*[handle(item) for item in items])",
    "    return [r for r in results if r.success]",
    "response = requests.get(url, timeout=30)",
    "if response.status_code != 200:",
    "    raise ConnectionError(f'Request failed with status {{response.status_code}}')",
]

ERRORS = [
    "NullPointerException",
    "FileNotFoundError",
    "ConnectionResetError",
    "TimeoutError",
    "PermissionDenied",
    "OutOfMemoryError",
]

USERS = ["alice", "bob", "charlie", "diana", "eve"]
IPS = ["192.168.1.10", "10.0.0.42", "172.16.0.5", "203.0.113.7"]
CLIENTS = ["api-gateway", "web-frontend", "mobile-app", "batch-worker"]


def generate_line() -> str:
    template = random.choice(LOG_TEMPLATES)
    return template.format(
        day=random.randint(1, 28),
        hour=random.randint(0, 23),
        minute=random.randint(0, 59),
        second=random.randint(0, 59),
        port=random.choice([8080, 8443, 3000, 5000]),
        req_id=f"req-{random.randint(1000, 9999)}",
        ms=random.randint(1, 5000),
        error=random.choice(ERRORS),
        count=random.randint(0, 1000),
        user=random.choice(USERS),
        ip=random.choice(IPS),
        client=random.choice(CLIENTS),
        ratio=random.uniform(50, 99),
    )


def main():
    os.makedirs(SAMPLE_DIR, exist_ok=True)

    for i in range(NUM_FILES):
        filepath = os.path.join(SAMPLE_DIR, f"sample_{i}.txt")
        with open(filepath, "w") as f:
            for _ in range(LINES_PER_FILE):
                f.write(generate_line() + "\n")

    print(f"Generated {NUM_FILES} sample files in {SAMPLE_DIR}")


if __name__ == "__main__":
    main()
