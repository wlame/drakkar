"""The ``agent`` section: the read-only surface that explains the worker to an AI agent."""

from pydantic import Field, field_validator

from drakkar.config.base import ConfigModel


class AgentConfig(ConfigModel):
    """Settings of the agent routes, which are served by the API server.

    The routes only read. They answer when ``enabled`` is true, whatever
    ``ui.enabled`` says, and they need ``api.enabled``.
    """

    enabled: bool = Field(
        default=False,
        description='Serve the agent routes. They need api.enabled. Off: every agent route answers 404.',
    )
    auth_token: str = Field(
        default='',
        description=(
            'Bearer token for the agent routes. Empty (the default) means no auth: the routes show the effective '
            'config and the worker state to anyone who can reach the API port, and the worker logs a warning at '
            'start. When set, every agent route needs "Authorization: Bearer <token>"; a missing or wrong token '
            'answers 401. Leading and trailing white space is stripped on load.'
        ),
        json_schema_extra={'drakkar_secret': True},
    )
    include_payloads: bool = Field(
        default=False,
        description=(
            'Let the state answer include task output text in its recent failures. Off: the answer carries sizes '
            'and ids only.'
        ),
    )

    @field_validator('auth_token', mode='before')
    @classmethod
    def _strip_auth_token(cls, value: object) -> object:
        """Strip white space around the token, so a quoted YAML value cannot hide a space in it."""
        if isinstance(value, str):
            return value.strip()
        return value
