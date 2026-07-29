"""Kafka transport security: config model, client-dict mapping, and merge rules."""

import re

import pytest
from pydantic import ValidationError
from structlog.testing import capture_logs

from drakkar.app import DrakkarApp
from drakkar.config import (
    DrakkarConfig,
    ExecutorConfig,
    KafkaConfig,
    KafkaSinkConfig,
    LoggingConfig,
    MetricsConfig,
    SinksConfig,
)
from drakkar.handler import BaseDrakkarHandler
from drakkar.kafka_security import (
    RESERVED_CLIENT_KEYS,
    KafkaSecurityConfig,
    describe_mixed_security,
    merge_client_config,
    resolve_client,
    validate_client_config,
)
from drakkar.models import ExecutorTask

from .conftest import make_ui_config


class _SecurityProbeHandler(BaseDrakkarHandler):
    """Minimal handler — these tests never run the pipeline, only boot steps."""

    async def arrange(self, messages, pending):
        return [ExecutorTask(task_id=f't-{m.offset}', args=[], source_offsets=[m.offset]) for m in messages]


# --- mapping: security fields -> librdkafka keys ---


def test_plaintext_default_emits_no_client_keys():
    """The default must reproduce today's client dict byte-for-byte.

    Any key emitted here would change the connection behaviour of every
    existing deployment that has not configured security at all.
    """
    assert KafkaSecurityConfig().to_client_config() == {}


def test_sasl_ssl_plain_maps_every_field():
    sec = KafkaSecurityConfig(
        protocol='SASL_SSL',
        sasl_mechanism='PLAIN',
        sasl_username='MYKEY',
        sasl_password='s3cret',
        ssl_ca_location='/etc/ssl/ca.pem',
    )
    assert sec.to_client_config() == {
        'security.protocol': 'SASL_SSL',
        'sasl.mechanism': 'PLAIN',
        'sasl.username': 'MYKEY',
        'sasl.password': 's3cret',
        'ssl.ca.location': '/etc/ssl/ca.pem',
    }


def test_mutual_tls_maps_certificate_and_key():
    sec = KafkaSecurityConfig(
        protocol='SSL',
        ssl_ca_location='/etc/ssl/ca.pem',
        ssl_certificate_location='/etc/ssl/client.pem',
        ssl_key_location='/etc/ssl/client.key',
        ssl_key_password='keypass',
    )
    assert sec.to_client_config() == {
        'security.protocol': 'SSL',
        'ssl.ca.location': '/etc/ssl/ca.pem',
        'ssl.certificate.location': '/etc/ssl/client.pem',
        'ssl.key.location': '/etc/ssl/client.key',
        'ssl.key.password': 'keypass',
    }


def test_hostname_verification_override_is_emitted_only_when_disabled():
    """``https`` is librdkafka's own default, so emitting it would be noise."""
    assert 'ssl.endpoint.identification.algorithm' not in KafkaSecurityConfig(protocol='SSL').to_client_config()
    disabled = KafkaSecurityConfig(protocol='SSL', ssl_endpoint_identification_algorithm='none')
    assert disabled.to_client_config()['ssl.endpoint.identification.algorithm'] == 'none'


@pytest.mark.parametrize('mechanism', ['SCRAM-SHA-256', 'SCRAM-SHA-512'])
def test_scram_mechanisms_are_accepted(mechanism):
    sec = KafkaSecurityConfig(
        protocol='SASL_SSL',
        sasl_mechanism=mechanism,
        sasl_username='u',
        sasl_password='p',
    )
    assert sec.to_client_config()['sasl.mechanism'] == mechanism


# --- validation: incoherent combinations fail at config load ---


@pytest.mark.parametrize('protocol', ['SASL_PLAINTEXT', 'SASL_SSL'])
def test_sasl_protocol_without_mechanism_is_rejected(protocol):
    with pytest.raises(ValidationError, match='sasl_mechanism'):
        KafkaSecurityConfig(protocol=protocol)


@pytest.mark.parametrize('missing', ['sasl_username', 'sasl_password'])
def test_password_mechanisms_require_credentials(missing):
    kwargs = {
        'protocol': 'SASL_SSL',
        'sasl_mechanism': 'PLAIN',
        'sasl_username': 'u',
        'sasl_password': 'p',
    }
    kwargs[missing] = ''
    with pytest.raises(ValidationError, match=missing):
        KafkaSecurityConfig(**kwargs)


@pytest.mark.parametrize('protocol', ['PLAINTEXT', 'SSL'])
def test_mechanism_without_sasl_protocol_is_rejected(protocol):
    """Silently ignoring a configured mechanism is the trap this closes."""
    with pytest.raises(ValidationError, match='sasl_mechanism'):
        KafkaSecurityConfig(protocol=protocol, sasl_mechanism='PLAIN', sasl_username='u', sasl_password='p')


def test_key_without_certificate_is_rejected():
    with pytest.raises(ValidationError, match='ssl_certificate_location'):
        KafkaSecurityConfig(protocol='SSL', ssl_key_location='/etc/ssl/client.key')


@pytest.mark.parametrize('mechanism', ['GSSAPI', 'OAUTHBEARER'])
def test_credential_free_mechanisms_need_no_username(mechanism):
    """Kerberos and OAuth carry their credentials outside username/password."""
    sec = KafkaSecurityConfig(protocol='SASL_SSL', sasl_mechanism=mechanism)
    assert sec.to_client_config()['sasl.mechanism'] == mechanism


# --- secrets never render ---


def test_passwords_are_hidden_in_repr_and_dump():
    sec = KafkaSecurityConfig(
        protocol='SASL_SSL',
        sasl_mechanism='PLAIN',
        sasl_username='u',
        sasl_password='hunter2',
    )
    assert 'hunter2' not in repr(sec)
    assert 'hunter2' not in str(sec)
    assert 'hunter2' not in str(sec.model_dump())
    # ...but the real value still reaches the client dict.
    assert sec.to_client_config()['sasl.password'] == 'hunter2'


def test_ssl_key_password_is_hidden():
    sec = KafkaSecurityConfig(protocol='SSL', ssl_key_password='keypass')
    assert 'keypass' not in repr(sec)
    assert 'keypass' not in str(sec.model_dump())


# --- escape hatch: reserved keys and precedence ---


@pytest.mark.parametrize('key', sorted(RESERVED_CLIENT_KEYS))
def test_reserved_client_keys_are_rejected(key):
    """Framework invariants depend on these; ignoring them silently is worse."""
    with pytest.raises(ValueError, match=re.escape(key)):
        validate_client_config({key: 'whatever'})


def test_ordinary_client_keys_pass_through():
    raw = {'ssl.endpoint.identification.algorithm': 'none', 'sasl.oauthbearer.config': 'x=1'}
    assert validate_client_config(raw) == raw


def test_client_config_overrides_the_typed_block():
    sec = KafkaSecurityConfig(protocol='SSL', ssl_ca_location='/typed/ca.pem')
    merged = merge_client_config(
        {'bootstrap.servers': 'b:9092'},
        sec,
        {'ssl.ca.location': '/override/ca.pem'},
    )
    assert merged['ssl.ca.location'] == '/override/ca.pem'
    assert merged['bootstrap.servers'] == 'b:9092'
    assert merged['security.protocol'] == 'SSL'


def test_merge_leaves_base_untouched():
    base = {'bootstrap.servers': 'b:9092'}
    merge_client_config(base, KafkaSecurityConfig(protocol='SSL'), {})
    assert base == {'bootstrap.servers': 'b:9092'}


# --- config wiring ---


def test_kafka_config_rejects_reserved_client_key():
    with pytest.raises(ValidationError, match=re.escape('enable.auto.commit')):
        KafkaConfig(client_config={'enable.auto.commit': 'true'})


def test_sink_config_rejects_reserved_client_key():
    with pytest.raises(ValidationError, match=re.escape('bootstrap.servers')):
        KafkaSinkConfig(topic='t', client_config={'bootstrap.servers': 'evil:9092'})


def test_security_is_configurable_from_yaml_shape():
    cfg = KafkaConfig.model_validate(
        {
            'brokers': 'broker:9093',
            'security': {
                'protocol': 'SASL_SSL',
                'sasl_mechanism': 'SCRAM-SHA-512',
                'sasl_username': 'drakkar',
                'sasl_password': 'pw',
            },
        }
    )
    assert cfg.security.to_client_config()['sasl.mechanism'] == 'SCRAM-SHA-512'


def test_inheritance_carries_security_with_the_brokers():
    """An empty brokers field means the consumer's cluster — and its credentials."""
    consumer_sec = KafkaSecurityConfig(
        protocol='SASL_SSL', sasl_mechanism='PLAIN', sasl_username='u', sasl_password='p'
    )
    resolved = resolve_client(
        '',
        KafkaSecurityConfig(),
        {},
        fallback_brokers='shared:9093',
        fallback_security=consumer_sec,
        fallback_client_config={'a': 'b'},
    )
    assert resolved.brokers == 'shared:9093'
    assert resolved.security.protocol == 'SASL_SSL'
    assert resolved.client_config == {'a': 'b'}


def test_own_brokers_do_not_inherit_security():
    own_sec = KafkaSecurityConfig(protocol='SSL')
    resolved = resolve_client(
        'other:9093',
        own_sec,
        {},
        fallback_brokers='shared:9093',
        fallback_security=KafkaSecurityConfig(protocol='SASL_PLAINTEXT', sasl_mechanism='GSSAPI'),
        fallback_client_config={'a': 'b'},
    )
    assert resolved.brokers == 'other:9093'
    assert resolved.security.protocol == 'SSL'
    assert resolved.client_config == {}


def test_mixed_security_is_described_only_when_downgrading():
    secured = KafkaSecurityConfig(protocol='SASL_SSL', sasl_mechanism='GSSAPI')
    assert describe_mixed_security(KafkaSecurityConfig(), secured) != ''
    # consumer itself is plaintext — nothing to downgrade from
    assert describe_mixed_security(KafkaSecurityConfig(), KafkaSecurityConfig()) == ''
    # client is secured too — nothing to warn about
    assert describe_mixed_security(secured, secured) == ''


# --- the three clients actually receive it ---


def test_consumer_passes_security_to_the_client(monkeypatch):
    captured = {}

    class FakeAIOConsumer:
        def __init__(self, conf, **kwargs):
            captured.update(conf)

    monkeypatch.setattr('drakkar.consumer.AIOConsumer', FakeAIOConsumer)
    from drakkar.consumer import KafkaConsumer

    KafkaConsumer(
        KafkaConfig(
            brokers='b:9093',
            security=KafkaSecurityConfig(
                protocol='SASL_SSL', sasl_mechanism='PLAIN', sasl_username='u', sasl_password='p'
            ),
        )
    )
    assert captured['security.protocol'] == 'SASL_SSL'
    assert captured['sasl.password'] == 'p'
    # framework-owned keys survive the merge
    assert captured['enable.auto.commit'] is False
    assert captured['partition.assignment.strategy'] == 'cooperative-sticky'


def test_consumer_without_security_is_unchanged(monkeypatch):
    captured = {}

    class FakeAIOConsumer:
        def __init__(self, conf, **kwargs):
            captured.update(conf)

    monkeypatch.setattr('drakkar.consumer.AIOConsumer', FakeAIOConsumer)
    from drakkar.consumer import KafkaConsumer

    KafkaConsumer(KafkaConfig(brokers='b:9092'))
    assert not [k for k in captured if k.startswith(('security.', 'sasl.', 'ssl.'))]


async def test_kafka_sink_inherits_consumer_security(monkeypatch):
    captured = {}

    class FakeAIOProducer:
        def __init__(self, conf):
            captured.update(conf)

    monkeypatch.setattr('drakkar.sinks.kafka.AIOProducer', FakeAIOProducer)
    from drakkar.sinks.kafka import KafkaSink

    sink = KafkaSink(
        'out',
        KafkaSinkConfig(topic='t'),  # no brokers -> inherits
        brokers_fallback='shared:9093',
        security_fallback=KafkaSecurityConfig(protocol='SASL_SSL', sasl_mechanism='GSSAPI'),
    )
    await sink.connect()
    assert captured['bootstrap.servers'] == 'shared:9093'
    assert captured['security.protocol'] == 'SASL_SSL'


async def test_dlq_sink_applies_security(monkeypatch):
    captured = {}

    class FakeAIOProducer:
        def __init__(self, conf):
            captured.update(conf)

    monkeypatch.setattr('drakkar.sinks.dlq.AIOProducer', FakeAIOProducer)
    from drakkar.sinks.dlq import DLQSink

    sink = DLQSink(
        topic='dlq',
        brokers='b:9093',
        security=KafkaSecurityConfig(protocol='SSL', ssl_ca_location='/ca.pem'),
    )
    await sink.connect()
    assert captured['security.protocol'] == 'SSL'
    assert captured['ssl.ca.location'] == '/ca.pem'


def _app_with(consumer_security: KafkaSecurityConfig, sink_config: KafkaSinkConfig) -> DrakkarApp:
    """A minimal app whose only interesting property is its Kafka security."""
    config = DrakkarConfig(
        kafka=KafkaConfig(brokers='shared:9092', source_topic='in', security=consumer_security),
        executor=ExecutorConfig(binary_path='/bin/echo'),
        sinks=SinksConfig(kafka={'out': sink_config}),
        metrics=MetricsConfig(enabled=False),
        logging=LoggingConfig(level='WARNING', format='console'),
        ui=make_ui_config(enabled=False),
    )
    return DrakkarApp(handler=_SecurityProbeHandler(), config=config)


async def test_startup_warns_when_a_sink_drops_the_consumer_security():
    """A sink on its own brokers with no security, next to a secured consumer."""
    app = _app_with(
        consumer_security=KafkaSecurityConfig(protocol='SASL_SSL', sasl_mechanism='GSSAPI'),
        sink_config=KafkaSinkConfig(topic='out', brokers='other:9092'),
    )
    with capture_logs() as cap:
        await app._lifecycle._report_kafka_security()

    assert any(e['event'] == 'kafka_security' and e['protocol'] == 'SASL_SSL' for e in cap)
    mismatches = [e for e in cap if e['event'] == 'kafka_security_mismatch']
    assert len(mismatches) == 1
    assert mismatches[0]['client'] == 'sinks.kafka.out'
    assert 'plaintext' in mismatches[0]['message']


async def test_startup_does_not_warn_for_an_inheriting_sink():
    """Empty brokers means it inherits the consumer's security — nothing to flag."""
    app = _app_with(
        consumer_security=KafkaSecurityConfig(protocol='SASL_SSL', sasl_mechanism='GSSAPI'),
        sink_config=KafkaSinkConfig(topic='out'),
    )
    with capture_logs() as cap:
        await app._lifecycle._report_kafka_security()

    assert not [e for e in cap if e['event'] == 'kafka_security_mismatch']


async def test_startup_reports_plaintext_without_warning():
    app = _app_with(KafkaSecurityConfig(), KafkaSinkConfig(topic='out', brokers='other:9092'))
    with capture_logs() as cap:
        await app._lifecycle._report_kafka_security()

    reported = [e for e in cap if e['event'] == 'kafka_security']
    assert reported[0]['protocol'] == 'PLAINTEXT'
    assert not [e for e in cap if e['event'] == 'kafka_security_mismatch']


def test_env_override_reaches_the_password(monkeypatch):
    """Production credentials come from DK_ env, never from YAML."""
    monkeypatch.setenv('DK_KAFKA__SECURITY__PROTOCOL', 'SASL_SSL')
    monkeypatch.setenv('DK_KAFKA__SECURITY__SASL_MECHANISM', 'PLAIN')
    monkeypatch.setenv('DK_KAFKA__SECURITY__SASL_USERNAME', 'envuser')
    monkeypatch.setenv('DK_KAFKA__SECURITY__SASL_PASSWORD', 'envpass')
    monkeypatch.setenv('DK_UI__RELEASE__ENABLED', 'false')

    cfg = DrakkarConfig()
    assert cfg.kafka.security.sasl_username == 'envuser'
    assert cfg.kafka.security.to_client_config()['sasl.password'] == 'envpass'
