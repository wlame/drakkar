"""The ``periodic`` section: the election of cluster tasks.

``periodic.election`` picks the provider that decides, for each slot of a
cluster periodic task, the one worker of the cluster that runs it (the
``history.archive`` system task included).
"""

from pydantic import Field, model_validator

from drakkar.config.base import ConfigModel
from drakkar.config.resilience import ResilienceConfig, validate_resilience


class ElectionConfig(ConfigModel):
    """Which provider decides the one worker of a cluster that runs a cluster task slot."""

    provider: str = Field(
        default='filesystem',
        min_length=1,
        description=(
            'Election provider: filesystem (a lock file in shared.dir), postgres (a row in a table '
            'of the database named by periodic.election.connection), redis (a key on the Redis '
            'server named by periodic.election.connection), consul (a key of the Consul agent named '
            'by periodic.election.connection, held by a session). '
            'Every worker of a cluster must use the same provider.'
        ),
    )
    connection: str = Field(
        default='main',
        min_length=1,
        description=(
            'Name of the entry under connections.<provider> that the consul, postgres and redis '
            'providers use. The other providers ignore it.'
        ),
    )
    resilience: ResilienceConfig = Field(
        default_factory=ResilienceConfig,
        description='Time limit, retries and circuit breaker for the calls to the election provider.',
    )

    @model_validator(mode='after')
    def _validate_resilience(self) -> 'ElectionConfig':
        """Check the rules of the resilience block that span fields, naming it by its YAML path."""
        validate_resilience('periodic.election.resilience', self.resilience)
        return self


class PeriodicConfig(ConfigModel):
    """The election of cluster periodic tasks."""

    election: ElectionConfig = Field(
        default_factory=ElectionConfig,
        description='How the one worker of a cluster that runs a cluster task slot is chosen.',
    )
