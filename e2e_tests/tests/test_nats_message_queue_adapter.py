# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import uuid

import pytest
from nats.js.errors import BadRequestError, ServerError

from univention.provisioning.backends.nats_mq import NatsKeys, NatsMessageQueue

from .e2e_settings import E2ETestSettings


@pytest.fixture
async def nats_mq_adapter(test_settings: E2ETestSettings):
    nats_adapter = NatsMessageQueue(test_settings.nats_url, test_settings.nats_user, test_settings.nats_password, 1)
    await nats_adapter.connect()
    yield nats_adapter
    await nats_adapter.close()


@pytest.fixture
async def test_stream(nats_mq_adapter: NatsMessageQueue):
    stream_name = "just_testing_again"
    await nats_mq_adapter.ensure_stream(stream_name, False)
    yield stream_name
    await nats_mq_adapter.delete_stream(stream_name)


@pytest.fixture
async def test_consumer(nats_mq_adapter: NatsMessageQueue, test_stream: str):
    await nats_mq_adapter.ensure_consumer(test_stream)
    yield True
    await nats_mq_adapter.delete_consumer(test_stream)


async def test_nats_connection(nats_mq_adapter: NatsMessageQueue):
    assert nats_mq_adapter


async def test_ensure_stream(nats_mq_adapter: NatsMessageQueue, test_stream):
    await nats_mq_adapter.ensure_stream(test_stream, False)
    await nats_mq_adapter.ensure_stream(test_stream, False)
    await nats_mq_adapter.ensure_stream(test_stream, False)


async def test_ensure_stream_with_filter(nats_mq_adapter: NatsMessageQueue):
    stream_name = "just_testing"
    await nats_mq_adapter.ensure_stream(stream_name, False, ["just_testing"])
    await nats_mq_adapter.delete_stream(stream_name)


async def test_ensure_stream_with_conflicting_filters(nats_mq_adapter: NatsMessageQueue):
    stream_name = "just_testing"
    await nats_mq_adapter.ensure_stream(stream_name, False, ["just_testing"])
    with pytest.raises(BadRequestError):
        await nats_mq_adapter.ensure_stream("just_testing2", False, ["*"])
    await nats_mq_adapter.delete_stream(stream_name)


async def test_update_stream_subject(nats_mq_adapter: NatsMessageQueue, test_stream):
    await nats_mq_adapter.ensure_stream(test_stream, False, [str(uuid.uuid4)[:8]])
    stream_info = await nats_mq_adapter._js.stream_info(NatsKeys.stream(test_stream))
    assert stream_info.config.subjects != [test_stream]


async def test_update_stream_retention(nats_mq_adapter: NatsMessageQueue, test_stream):
    with pytest.raises(ServerError):
        await nats_mq_adapter.ensure_stream(test_stream, True)


async def test_ensure_consumer(test_consumer: bool):
    assert test_consumer


async def test_ensure_consumer_with_name(nats_mq_adapter: NatsMessageQueue, test_stream: str, test_consumer):
    await nats_mq_adapter.ensure_consumer(test_stream)
    await nats_mq_adapter.ensure_consumer(test_stream)
