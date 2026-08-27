"""Transport security for the Kafka clients (consumer, sinks, DLQ).

A leaf module (no Drakkar imports) so the mapping table, the validation
rules, and the merge order can be exercised without pulling in the config
package.

Three clients speak to Kafka: the consumer (``drakkar/consumer.py``), the
Kafka sink (``drakkar/sinks/kafka.py``), and the DLQ producer
(``drakkar/sinks/dlq.py``). All three build their librdkafka config through
:func:`merge_client_config`, so security reaches every one of them by the
same path and can never be configured for one but silently missed on another.

The security surface is deliberately two-layered:

* a **typed block** (:class:`KafkaSecurityConfig`) covering the protocols
  real deployments use — it is validated, documented, and redactable;
* a **raw escape hatch** (``client_config``) merged last, so an librdkafka
  option nobody anticipated never becomes a reason to fork the framework.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal, NamedTuple

from pydantic import BaseModel, Field, SecretStr, model_validator

# Field name -> librdkafka property. Kept as data rather than a chain of
# ``if`` statements so adding a property is a row here, not a new branch in
# a builder.
# Order is the order keys appear in the built config (stable for tests and
# for eyeballing a logged dict).
SECURITY_FIELD_KEYS: tuple[tuple[str, str], ...] = (
    ('protocol', 'security.protocol'),
    ('sasl_mechanism', 'sasl.mechanism'),
    ('sasl_username', 'sasl.username'),
    ('sasl_password', 'sasl.password'),
    ('ssl_ca_location', 'ssl.ca.location'),
    ('ssl_certificate_location', 'ssl.certificate.location'),
    ('ssl_key_location', 'ssl.key.location'),
    ('ssl_key_password', 'ssl.key.password'),
    ('ssl_endpoint_identification_algorithm', 'ssl.endpoint.identification.algorithm'),
)

# librdkafka properties the escape hatch must not set, mapped to the guidance
# the error names. These are not style preferences — each one backs a
# framework invariant that silently breaks if the value changes, and a
# broken invariant here loses or duplicates messages rather than failing
# visibly. Rejecting at config load beats ignoring the key at merge time:
# an ignored override looks like it worked.
RESERVED_CLIENT_KEYS: dict[str, str] = {
    'enable.auto.commit': (
        'at-least-once delivery depends on Drakkar committing offsets on its own '
        'per-partition watermark; auto-commit would advance past unprocessed messages'
    ),
    'partition.assignment.strategy': (
        'the drain-on-revoke path assumes cooperative-sticky rebalancing; another '
        'strategy re-opens the duplicate-delivery window'
    ),
    'group.id': 'set it with kafka.consumer_group',
    'bootstrap.servers': 'set it with kafka.brokers (or the sink/DLQ brokers field)',
}

# SASL mechanisms that authenticate with a username and password. GSSAPI
# (Kerberos) takes its identity from a keytab/ticket cache and OAUTHBEARER
# from a token callback, so neither is required to carry credentials here.
_PASSWORD_MECHANISMS = frozenset({'PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512'})

# Protocols that put SASL on the wire — the ones a mechanism applies to.
_SASL_PROTOCOLS = frozenset({'SASL_PLAINTEXT', 'SASL_SSL'})

# Values librdkafka already applies on its own. Emitting them would be a
# no-op functionally but would still change the client dict of every worker
# that has configured no security at all, so they are omitted and an
# unconfigured deployment keeps a byte-identical config.
_LIBRDKAFKA_DEFAULTS: dict[str, str] = {'protocol': 'PLAINTEXT'}

SecurityProtocol = Literal['PLAINTEXT', 'SSL', 'SASL_PLAINTEXT', 'SASL_SSL']
SaslMechanism = Literal['PLAIN', 'SCRAM-SHA-256', 'SCRAM-SHA-512', 'GSSAPI', 'OAUTHBEARER']


class KafkaSecurityConfig(BaseModel):
    """Authentication and encryption settings for one Kafka client.

    The default is ``PLAINTEXT`` with everything else empty, which emits no
    librdkafka properties at all — an unconfigured worker connects exactly
    as it did before this block existed.

    Passwords are :class:`~pydantic.SecretStr`, so a stray ``repr()``,
    ``model_dump()``, or f-string renders ``**********`` instead of the
    credential. :meth:`to_client_config` is the single place the real value
    is read.
    """

    protocol: SecurityProtocol = Field(
        default='PLAINTEXT',
        description=(
            'Wire protocol. PLAINTEXT (default) is unauthenticated and unencrypted. '
            'SSL adds TLS; SASL_PLAINTEXT adds authentication without encryption; '
            'SASL_SSL adds both and is what managed clusters normally require.'
        ),
    )
    sasl_mechanism: SaslMechanism | None = Field(
        default=None,
        description=(
            'SASL mechanism. Required when protocol is SASL_PLAINTEXT or SASL_SSL, '
            'and rejected otherwise so a mechanism that would be ignored is never '
            'left in the config. PLAIN and the SCRAM mechanisms additionally '
            'require sasl_username and sasl_password.'
        ),
    )
    sasl_username: str = Field(default='', description='SASL username (PLAIN / SCRAM mechanisms).')
    sasl_password: SecretStr = Field(
        default=SecretStr(''),
        description=(
            'SASL password. Prefer the DK_KAFKA__SECURITY__SASL_PASSWORD environment '
            'override over a literal in YAML; DK_* variables are withheld from '
            'executor subprocesses by the default executor.env_inherit_deny list.'
        ),
        json_schema_extra={'drakkar_secret': True},
    )
    ssl_ca_location: str = Field(
        default='',
        description=(
            'Path to a PEM CA bundle used to verify the broker certificate. Leave empty to use the system trust store.'
        ),
    )
    ssl_certificate_location: str = Field(
        default='', description='Path to the client certificate (PEM) for mutual TLS.'
    )
    ssl_key_location: str = Field(
        default='',
        description='Path to the client private key (PEM) for mutual TLS. Requires ssl_certificate_location.',
    )
    ssl_key_password: SecretStr = Field(
        default=SecretStr(''),
        description='Passphrase for an encrypted client private key.',
        json_schema_extra={'drakkar_secret': True},
    )
    ssl_endpoint_identification_algorithm: Literal['https', 'none'] | None = Field(
        default=None,
        description=(
            'Broker hostname verification. Unset uses librdkafka\'s default ("https", '
            'verification on). Set to "none" only for internal CAs that issue '
            'certificates without matching SANs — it disables a real protection.'
        ),
    )

    @model_validator(mode='after')
    def _check_coherent(self) -> KafkaSecurityConfig:
        """Reject combinations that would connect wrongly or silently do nothing.

        Each rule exists because the alternative failure is worse than a
        startup error: librdkafka reports most of these as an opaque
        connection failure at first poll, long after the misconfiguration
        was introduced, and one of them (a mechanism on a non-SASL
        protocol) is not reported at all.
        """
        if self.protocol in _SASL_PROTOCOLS and self.sasl_mechanism is None:
            raise ValueError(
                f'kafka security protocol {self.protocol} requires sasl_mechanism to be set '
                f'(one of {", ".join(sorted(_PASSWORD_MECHANISMS | {"GSSAPI", "OAUTHBEARER"}))})'
            )

        if self.sasl_mechanism is not None and self.protocol not in _SASL_PROTOCOLS:
            raise ValueError(
                f'sasl_mechanism={self.sasl_mechanism} has no effect with protocol={self.protocol}; '
                'set protocol to SASL_SSL (or SASL_PLAINTEXT) or remove sasl_mechanism'
            )

        if self.sasl_mechanism in _PASSWORD_MECHANISMS:
            if not self.sasl_username:
                raise ValueError(f'sasl_mechanism={self.sasl_mechanism} requires sasl_username')
            if not self.sasl_password.get_secret_value():
                raise ValueError(
                    f'sasl_mechanism={self.sasl_mechanism} requires sasl_password '
                    '(set DK_KAFKA__SECURITY__SASL_PASSWORD rather than writing it into YAML)'
                )

        if self.ssl_key_location and not self.ssl_certificate_location:
            raise ValueError('ssl_key_location requires ssl_certificate_location — mutual TLS needs both halves')

        return self

    def to_client_config(self) -> dict[str, str]:
        """Render this block as librdkafka properties.

        Only non-empty values are emitted, so the ``PLAINTEXT`` default
        produces ``{}`` and every existing deployment keeps a byte-identical
        client config. This is the one place a ``SecretStr`` is unwrapped.
        """
        config: dict[str, str] = {}
        for field, key in SECURITY_FIELD_KEYS:
            value = getattr(self, field)
            if value is None:
                continue
            if isinstance(value, SecretStr):
                value = value.get_secret_value()
            if value == '' or _LIBRDKAFKA_DEFAULTS.get(field) == value:
                continue
            config[key] = str(value)
        return config

    def describe(self) -> str:
        """Credential-free one-line description for startup logging.

        Names the protocol and mechanism so an operator can confirm what the
        worker negotiated, and never touches a username, password, or key
        path.
        """
        if self.protocol == 'PLAINTEXT':
            return 'PLAINTEXT (no authentication or encryption)'
        if self.sasl_mechanism:
            return f'{self.protocol}/{self.sasl_mechanism}'
        return self.protocol


def validate_client_config(raw: Mapping[str, str]) -> dict[str, str]:
    """Return ``raw`` unchanged, or raise if it sets a reserved property.

    Called from the ``client_config`` field validators so a reserved key
    fails at config load, naming the typed field to use instead, rather
    than being dropped during the merge where nothing would report it.
    """
    for key in raw:
        guidance = RESERVED_CLIENT_KEYS.get(key.strip().lower())
        if guidance is not None:
            raise ValueError(f'client_config may not set {key!r}: {guidance}')
    return dict(raw)


class ResolvedKafkaClient(NamedTuple):
    """The brokers, security, and raw overrides one client should connect with."""

    brokers: str
    security: KafkaSecurityConfig
    client_config: dict[str, str]


def resolve_client(
    brokers: str,
    security: KafkaSecurityConfig,
    client_config: Mapping[str, str],
    *,
    fallback_brokers: str,
    fallback_security: KafkaSecurityConfig,
    fallback_client_config: Mapping[str, str],
) -> ResolvedKafkaClient:
    """Resolve a sink/DLQ client against the consumer's settings.

    Inheritance is keyed on ``brokers`` being empty — the rule the brokers
    field already followed before security existed, extended to carry
    security with it. An empty ``brokers`` means "the same cluster as the
    consumer", and the same cluster needs the same credentials; inheriting
    the address while silently dropping the credentials would turn one
    omitted field into a connection that cannot authenticate.

    Setting ``brokers`` makes the client self-contained: it uses only its
    own security block, which defaults to PLAINTEXT. That is not a silent
    downgrade — a secured cluster refuses a plaintext connect — but callers
    should surface it, which :func:`describe_mixed_security` supports.
    """
    if not brokers:
        return ResolvedKafkaClient(fallback_brokers, fallback_security, dict(fallback_client_config))
    return ResolvedKafkaClient(brokers, security, dict(client_config))


def describe_mixed_security(
    resolved: KafkaSecurityConfig,
    consumer: KafkaSecurityConfig,
) -> str:
    """Return a warning message when a client drops the consumer's security.

    Empty string when there is nothing to warn about. Fires only for the
    combination that is almost always a mistake: the consumer authenticates,
    and a client pointed at its own brokers does not.
    """
    if consumer.protocol == 'PLAINTEXT' or resolved.protocol != 'PLAINTEXT':
        return ''
    return (
        f'the consumer connects with {consumer.describe()} but this client sets its own brokers '
        'with no security block, so it will connect in plaintext — add a security block to it, '
        'or clear its brokers field to inherit the consumer cluster settings'
    )


def merge_client_config(
    base: Mapping[str, Any],
    security: KafkaSecurityConfig,
    extra: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """Compose the final librdkafka config for one client.

    Precedence, lowest to highest:

    1. ``base`` — the framework-owned keys the caller sets (brokers, group
       id, manual-commit and rebalance settings).
    2. ``security`` — the typed block.
    3. ``extra`` — the raw ``client_config`` escape hatch.

    ``extra`` deliberately outranks the typed block: an operator reaching
    for the escape hatch is overriding on purpose, and the reserved-key
    check has already refused the properties that would break a framework
    invariant. ``base`` is never mutated.
    """
    merged = dict(base)
    merged.update(security.to_client_config())
    if extra:
        merged.update(extra)
    return merged
