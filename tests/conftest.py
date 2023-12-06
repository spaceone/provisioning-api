import json
from unittest.mock import AsyncMock, Mock

import pytest
from fakeredis import aioredis
from nats.aio.msg import Msg
from redis._parsers.helpers import (
    parse_xread_resp3,
    string_keys_to_dict,
    bool_ok,
    parse_command_resp3,
    parse_sentinel_state_resp3,
    parse_sentinel_masters_resp3,
    parse_sentinel_slaves_and_sentinels_resp3,
)
from redis.utils import str_if_bytes
from nats.js.kv import KeyValue  # noqa: F401

from src.shared.adapters import (
    NatsAdapter,
    NatsKVAdapter,
    MessageQueueAdapter,
    KVStoreAdapter,
)
from consumer.port import ConsumerPort
from consumer.main import app
from events.port import EventsPort

FLAT_MESSAGE = {
    "publisher_name": "127.0.0.1",
    "ts": "2023-11-09T11:15:52.616061",
    "realm": "foo",
    "topic": "bar/baz",
    "body": '{"hello": "world"}',
}
SUBSCRIBER_INFO = {
    "name": "0f084f8c-1093-4024-b215-55fe8631ddf6",
    "realms_topics": ["foo:bar", "abc:def"],
    "fill_queue": True,
    "fill_queue_status": "done",
}


async def fake_redis():
    connection = aioredis.FakeRedis(decode_responses=True, protocol=2)
    connection.response_callbacks.update(
        {
            # Because fakeredis does not support RESP3 protocol, we need to manually patch some
            # responses of stream commands. Here is a list of operations we might need in the future:
            # ZRANGE ZINTER ZPOPMAX ZPOPMIN ZRANGEBYSCORE ZREVRANGE ZREVRANGEBYSCORE ZUNION HGETALL XREADGROUP"
            **string_keys_to_dict("XREAD XREADGROUP", parse_xread_resp3),
            "ACL LOG": lambda r: [
                {str_if_bytes(key): str_if_bytes(value) for key, value in x.items()}
                for x in r
            ]
            if isinstance(r, list)
            else bool_ok(r),
            "COMMAND": parse_command_resp3,
            "CONFIG GET": lambda r: {
                str_if_bytes(key)
                if key is not None
                else None: str_if_bytes(value)
                if value is not None
                else None
                for key, value in r.items()
            },
            "MEMORY STATS": lambda r: {
                str_if_bytes(key): value for key, value in r.items()
            },
            "SENTINEL MASTER": parse_sentinel_state_resp3,
            "SENTINEL MASTERS": parse_sentinel_masters_resp3,
            "SENTINEL SENTINELS": parse_sentinel_slaves_and_sentinels_resp3,
            "SENTINEL SLAVES": parse_sentinel_slaves_and_sentinels_resp3,
            "STRALGO": lambda r, **options: {
                str_if_bytes(key): str_if_bytes(value) for key, value in r.items()
            }
            if isinstance(r, dict)
            else str_if_bytes(r),
            "XINFO CONSUMERS": lambda r: [
                {str_if_bytes(key): value for key, value in x.items()} for x in r
            ],
            "XINFO GROUPS": lambda r: [
                {str_if_bytes(key): value for key, value in d.items()} for d in r
            ],
        }
    )

    try:
        return connection
    finally:
        await connection.aclose()


def fake_js():
    js = Mock()
    js.stream_info = AsyncMock()
    js.publish = AsyncMock()
    js.delete_msg = AsyncMock()
    js.add_consumer = AsyncMock()
    js.delete_stream = AsyncMock()

    sub = AsyncMock()
    js.pull_subscribe = AsyncMock(return_value=sub)
    sub.fetch = AsyncMock(
        return_value=[Msg(_client="nats", data=json.dumps(FLAT_MESSAGE).encode())]
    )

    return js


def mock_nats_mq(abstract_mq: MessageQueueAdapter):
    abstract_mq.message_queue.nats = AsyncMock()
    abstract_mq.message_queue.js = fake_js()


def mock_nats_kv(abstract_kv: KVStoreAdapter):
    abstract_kv.kv_store.nats = AsyncMock()
    abstract_kv.kv_store.js = fake_js()
    abstract_kv.kv_store.kv_store = AsyncMock()


def is_instance_by_class_name(obj, cls):
    """Compare class names between obj instance and cls class (for conditional mocking)
    Using this as a quick isinstance() replacement, because id(instance class) != id(implementation class).
    """

    return obj.__class__.__name__ == cls.__name__


async def port_fake_dependency() -> ConsumerPort:
    port = ConsumerPort()

    if is_instance_by_class_name(port.message_queue.message_queue, NatsAdapter):
        mock_nats_mq(port.message_queue)
    if is_instance_by_class_name(port.kv_store.kv_store, NatsKVAdapter):
        mock_nats_kv(port.kv_store)

    port.kv_store.kv_store.nats = AsyncMock()
    port.kv_store.kv_store.js = fake_js()
    port.kv_store.kv_store.kv_store = AsyncMock()

    port.kv_store.add_subscriber = AsyncMock()
    port.kv_store.get_subscribers_for_key = AsyncMock(
        return_value=[SUBSCRIBER_INFO["name"]]
    )
    port.kv_store.delete_subscriber = AsyncMock()
    port.kv_store.get_subscriber_info = AsyncMock(return_value=SUBSCRIBER_INFO)

    return port


async def events_port_fake_dependency() -> EventsPort:
    port = EventsPort()

    if is_instance_by_class_name(port.message_queue.message_queue, NatsAdapter):
        mock_nats_mq(port.message_queue)
    if is_instance_by_class_name(port.kv_store.kv_store, NatsKVAdapter):
        mock_nats_kv(port.kv_store)

    port.kv_store.add_subscriber = AsyncMock()
    port.kv_store.get_subscribers_for_key = AsyncMock(
        return_value=[SUBSCRIBER_INFO["name"]]
    )
    port.kv_store.delete_subscriber = AsyncMock()
    port.kv_store.get_subscriber_info = AsyncMock(return_value=SUBSCRIBER_INFO)

    return port


async def port_fake_dependency_without_sub():
    port = await port_fake_dependency()
    port.kv_store.get_subscriber_info = AsyncMock(return_value=None)
    return port


async def port_fake_dependency_events():
    port = await events_port_fake_dependency()
    port.kv_store.get_subscriber_info = AsyncMock(return_value=None)
    return port


@pytest.fixture(autouse=True)
def override_dependencies():
    # Override original port
    app.dependency_overrides[ConsumerPort.port_dependency] = port_fake_dependency
    yield  # This will ensure the setup is done before tests and cleanup after
    # Clear the overrides after the tests
    app.dependency_overrides.clear()


@pytest.fixture
def override_dependencies_without_sub():
    # Override original port
    app.dependency_overrides[
        ConsumerPort.port_dependency
    ] = port_fake_dependency_without_sub
    yield  # This will ensure the setup is done before tests and cleanup after
    # Clear the overrides after the tests
    app.dependency_overrides.clear()


@pytest.fixture
def override_dependencies_events():
    # Override original port
    app.dependency_overrides[EventsPort.port_dependency] = port_fake_dependency_events
    yield  # This will ensure the setup is done before tests and cleanup after
    # Clear the overrides after the tests
    app.dependency_overrides.clear()
