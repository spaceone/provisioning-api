import json
from copy import copy
from typing import Union
from unittest.mock import AsyncMock, Mock

import pytest
from fakeredis import aioredis
from nats.aio.msg import Msg
from nats.js.kv import KeyValue
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
from consumer.port import ConsumerPort
from consumer.main import app
from events.port import EventsPort

NAME = "0f084f8c-1093-4024-b215-55fe8631ddf6"

FLAT_MESSAGE = {
    "publisher_name": "127.0.0.1",
    "ts": "2023-11-09T11:15:52.616061",
    "realm": "foo",
    "topic": "bar/baz",
    "body": '{"hello": "world"}',
}
SUBSCRIBER_INFO = {
    "name": NAME,
    "realms_topics": ["foo:bar", "abc:def"],
    "fill_queue": True,
    "fill_queue_status": "done",
}

BASE_KV_OBJ = KeyValue.Entry(
    "KV_bucket",
    "",
    None,
    None,
    None,
    None,
    None,
)

kv_sub_info = copy(BASE_KV_OBJ)
kv_sub_info.key = f"subscriber:{NAME}"
kv_sub_info.value = (
    b'{"name": "0f084f8c-1093-4024-b215-55fe8631ddf6", "realms_topics": ["foo:bar"], "fill_queue": true, '
    b'"fill_queue_status": "done"}'
)

kv_subs = copy(BASE_KV_OBJ)
kv_subs.key = "abc:def"
kv_subs.value = (
    b"7e9e4ea6-6986-44cc-9fbc-7530c422fb21,000fjga6-6986-44cc-9000-7530c422f000"
)


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
    Msg.ack = AsyncMock()

    return js


class FakeNats:
    @classmethod
    async def delete(cls, key: str):
        pass

    @classmethod
    async def get_value_by_key(cls, key: str):
        values = {"abc:def": kv_subs, f"subscriber:{NAME}": kv_sub_info}
        return values[key]

    @classmethod
    async def put_value_by_key(cls, key: str, value: Union[str, dict]):
        pass


def fake_nats_adapter(port: Union[ConsumerPort, EventsPort]):
    port.nats_adapter.kv_store = AsyncMock()
    port.nats_adapter.js = fake_js()
    port.nats_adapter.kv_store.delete = AsyncMock(side_effect=FakeNats.delete)
    port.nats_adapter.kv_store.get = AsyncMock(side_effect=FakeNats.get_value_by_key)
    port.nats_adapter.kv_store.put = AsyncMock(side_effect=FakeNats.put_value_by_key)


async def consumer_port_fake_dependency() -> ConsumerPort:
    port = ConsumerPort()
    fake_nats_adapter(port)
    return port


async def events_port_fake_dependency() -> EventsPort:
    port = EventsPort()
    fake_nats_adapter(port)
    return port


@pytest.fixture(autouse=True)
def override_dependencies_consumer():
    # Override original port
    app.dependency_overrides[
        ConsumerPort.port_dependency
    ] = consumer_port_fake_dependency
    yield  # This will ensure the setup is done before tests and cleanup after
    # Clear the overrides after the tests
    app.dependency_overrides.clear()


@pytest.fixture
def override_dependencies_events():
    # Override original port
    app.dependency_overrides[EventsPort.port_dependency] = events_port_fake_dependency
    yield  # This will ensure the setup is done before tests and cleanup after
    # Clear the overrides after the tests
    app.dependency_overrides.clear()
