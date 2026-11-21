"""The ``split`` section: messages that are cut into parts, run on many workers and put together again."""

from pydantic import Field

from drakkar.config.base import ConfigModel
from drakkar.config.units import ByteSize, WholeSeconds, seconds_text

IDLE_NOT_BELOW_JOB_MESSAGE = 'split.idle_timeout ({idle}) must be < split.job_timeout ({job})'

BOTH_TIMEOUTS_ZERO_MESSAGE = (
    'split.job_timeout and split.idle_timeout cannot both be 0: a job with a lost part would never end'
)


class SplitConfig(ConfigModel):
    """Settings of the split feature: the internal topics, the aggregate sinks and the limits of a job.

    With ``enabled`` false the section loads, is checked, and does nothing.
    """

    enabled: bool = Field(
        default=False,
        description='Split messages of the Kafka source into parts that run on every worker. Needs sources.kafka.enabled.',
    )
    connection: str = Field(
        default='main',
        description='The name of the entry under connections.kafka that the parts and results topics use.',
    )
    parts_topic: str = Field(
        default='',
        description='The internal topic of the parts. Empty: "<sources.kafka.topic>_<consumer group>_split_parts".',
    )
    results_topic: str = Field(
        default='',
        description=(
            'The internal results log. Empty: "<sources.kafka.topic>_<consumer group>_split_results". '
            'It needs the partition count of the source topic.'
        ),
    )
    parts_group: str = Field(
        default='',
        description='The consumer group of the parts. Empty: "<sources.kafka.consumer_group>_split_parts".',
    )
    results_group: str = Field(
        default='',
        description='The consumer group of the results log. Empty: "<sources.kafka.consumer_group>_split_results".',
    )
    aggregate_sinks: list[str] = Field(
        default_factory=list,
        description=(
            'Sinks of the aggregate stage, each written "<type>/<name>". Every other sink is a part sink. '
            'A sink depends only on sinks of its own stage.'
        ),
    )
    job_timeout: WholeSeconds = Field(
        default=86400,
        description=(
            'The longest time a job may run, from 1m to 30d. 0 means no limit, and then idle_timeout must be set.'
        ),
    )
    idle_timeout: WholeSeconds = Field(
        default=0,
        description=(
            'The longest time a job may go without a new part result, counted only after the split has finished, '
            'from 1m to 30d, and below job_timeout when both are set. 0 means no limit.'
        ),
    )
    max_attempts: int = Field(
        default=3,
        ge=1,
        le=10,
        description=(
            'How many attempts of one job may die during their split (no split-done and no split-cancelled) '
            'before the job goes to the DLQ, from 1 to 10.'
        ),
    )
    max_part_data: ByteSize = Field(
        default=65536,
        description='The largest data of one part, from 1KiB to 512KiB.',
    )

    def check_timeouts(self) -> None:
        """Refuse timeouts that leave a job with no end, or an idle time that is not below the job time.

        Raises:
            ValueError: both are 0, or ``idle_timeout`` is not below ``job_timeout`` while both are set.
        """
        if self.job_timeout == 0 and self.idle_timeout == 0:
            raise ValueError(BOTH_TIMEOUTS_ZERO_MESSAGE)
        if self.job_timeout > 0 and self.idle_timeout > 0 and self.idle_timeout >= self.job_timeout:
            raise ValueError(
                IDLE_NOT_BELOW_JOB_MESSAGE.format(
                    idle=seconds_text(self.idle_timeout), job=seconds_text(self.job_timeout)
                )
            )
