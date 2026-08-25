"""Synchronous HTTP ingress (webapp) configuration."""

from pydantic import BaseModel, Field, field_validator, model_validator


class WebClientConfig(BaseModel):
    """Configuration for a single webapp client (tenant).

    Each client has a name (used in metrics labels and recorder rows), an
    optional bearer token (empty string = anonymous matching for requests
    without an Authorization header), and a per-client rpm cap enforced by
    a sliding-window rate limiter on the webapp side.

    Validation rules at the WebAppConfig level:
    - At most one client may have an empty token (anonymous slot).
    - All non-empty tokens must be unique across clients.
    - rpm must be > 0 for every client.
    """

    name: str
    token: str = Field(
        default='',
        description='Bearer token for this client; empty means the anonymous slot (no Authorization header required).',
        json_schema_extra={'drakkar_secret': True},
    )
    rpm: int = 4

    @field_validator('name')
    @classmethod
    def _validate_name_non_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError('webapp client name must be a non-empty string')
        return v

    @field_validator('token')
    @classmethod
    def _validate_token_is_ascii(cls, v: str) -> str:
        """Bearer tokens are compared with ``hmac.compare_digest``, which
        raises on non-ASCII ``str`` operands — so a non-ASCII token here
        would lock every client out at runtime. Fail at config load, where
        the operator can still fix it."""
        if not v.isascii():
            raise ValueError(
                'webapp client token must be ASCII: bearer tokens are compared byte-wise, '
                'and a non-ASCII token cannot be sent in an Authorization header'
            )
        return v


class WebAppConfig(BaseModel):
    """Configuration for the optional synchronous-HTTP webapp pipeline.

    When ``enabled=true``, Drakkar starts a FastAPI server on its own thread
    accepting POST requests and routing them through the same handler
    pipeline as Kafka messages. Defaults are tuned for a small dev
    deployment with one anonymous client; multi-tenant production
    deployments should configure named clients with non-empty tokens.

    Per-request flow:
    - Auth (token match) → rate-limit (per-client rpm) → dispatch to main
      loop → user's ``arrange_http_request`` → executor pool → user's
      ``on_http_request_complete`` → JSON response.

    See ``docs/webapp.md`` for the full feature guide.
    """

    enabled: bool = False
    host: str = '0.0.0.0'
    port: int = 8090
    path: str = '/process'
    sinks_enabled: bool = False
    request_timeout_seconds: float = 30.0
    max_concurrent: int = 64
    # Cap on a single POST body (bytes); requests beyond it get a 413
    # ``request_too_large`` envelope before the body is buffered. Same
    # key, default, and behavior as the Go backend.
    max_body_bytes: int = 10 * 1024 * 1024
    clients: list[WebClientConfig] = Field(
        default_factory=lambda: [WebClientConfig(name='anonymous', token='', rpm=4)],
        description=(
            'List of webapp clients (tenants). Defaults to a single '
            'anonymous client with empty token and rpm=4 so the webapp '
            'works out of the box for development. Production deployments '
            'should configure named clients with non-empty tokens.'
        ),
    )

    @model_validator(mode='after')
    def _validate_webapp(self) -> 'WebAppConfig':
        """Enforce webapp config invariants.

        These rules are checked at config load time so misconfigurations
        surface before any request lands. Each error message names the
        offending field/client so operators can find and fix the problem.
        """
        # path must start with '/' and not be just '/' (need a real route).
        if not self.path.startswith('/') or len(self.path) <= 1:
            raise ValueError(f"webapp.path must start with '/' and have a non-empty route, got {self.path!r}")
        # request_timeout_seconds > 0 — a zero/negative timeout would
        # cancel every request before it had a chance to start.
        if self.request_timeout_seconds <= 0:
            raise ValueError(f'webapp.request_timeout_seconds must be > 0, got {self.request_timeout_seconds}')
        # max_concurrent > 0 — semaphore with zero capacity would block all
        # requests indefinitely.
        if self.max_concurrent <= 0:
            raise ValueError(f'webapp.max_concurrent must be > 0, got {self.max_concurrent}')
        # A zero/negative body cap would reject every non-empty POST at
        # the body-read gate.
        if self.max_body_bytes <= 0:
            raise ValueError(f'webapp.max_body_bytes must be > 0, got {self.max_body_bytes}')
        # At least one client. The default factory ensures this for an
        # omitted ``clients`` block, but explicit ``clients: []`` in YAML
        # would otherwise silently give us a webapp that rejects every
        # request — fail loud instead.
        if len(self.clients) == 0:
            raise ValueError('webapp.clients must contain at least one client')
        # Per-client rpm > 0. Zero rpm means "always rate-limit", which
        # is almost certainly a typo.
        for client in self.clients:
            if client.rpm <= 0:
                raise ValueError(f'webapp client {client.name!r} has rpm={client.rpm}; rpm must be > 0')
        # At most one client with empty token (the anonymous slot).
        # Multiple empty-token clients can never be distinguished at the
        # auth layer, so we reject the ambiguity at config time.
        empty_token_clients = [c for c in self.clients if c.token == '']
        if len(empty_token_clients) > 1:
            names = ', '.join(repr(c.name) for c in empty_token_clients)
            raise ValueError(
                f'at most one webapp client may have an empty token (anonymous); '
                f'got {len(empty_token_clients)} empty-token clients: {names}'
            )
        # All non-empty tokens unique. Two clients sharing a token would
        # collide at the auth layer; the matched client_name would be
        # nondeterministic.
        seen_tokens: dict[str, str] = {}
        for client in self.clients:
            if client.token == '':
                continue
            if client.token in seen_tokens:
                raise ValueError(
                    f'webapp clients {seen_tokens[client.token]!r} and {client.name!r} '
                    f'share the same token; tokens must be unique across clients'
                )
            seen_tokens[client.token] = client.name
        return self
