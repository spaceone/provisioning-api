# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import uuid
import pytest


import shared.client
import shared.models.queue
from tests.conftest import (
    REALMS_TOPICS,
    CONSUMER_PASSWORD,
)
from tests.e2e.helpers import (
    create_message_via_events_api,
    create_message_via_udm_rest_api,
    pop_all_messages,
)
from univention.admin.rest.client import UDM

SUBSCRIBER_NAME = str(uuid.uuid4())


@pytest.fixture
def provisioning_client() -> shared.client.AsyncClient:
    return shared.client.AsyncClient(SUBSCRIBER_NAME, CONSUMER_PASSWORD)


@pytest.fixture
async def simple_subscription(provisioning_client: shared.client.AsyncClient):
    subscriber_name = str(uuid.uuid4())

    await provisioning_client.create_subscription(
        subscriber_name, REALMS_TOPICS, CONSUMER_PASSWORD, False
    )

    yield SUBSCRIBER_NAME

    await provisioning_client.cancel_subscription(SUBSCRIBER_NAME)


async def test_create_subscription(
    provisioning_client: shared.client.AsyncClient, simple_subscription
):
    response = await provisioning_client.get_subscription(simple_subscription)
    assert response


async def test_get_empty_messages(
    provisioning_client: shared.client.AsyncClient, simple_subscription: str
):
    response = await provisioning_client.get_subscription_messages(
        name=simple_subscription,
        count=1,
        timeout=1,
    )

    assert response == []


async def test_send_message(
    provisioning_client: shared.client.AsyncClient,
    simple_subscription: str,
    provisioning_base_url: str,
):
    data = create_message_via_events_api(provisioning_base_url)

    response = await provisioning_client.get_subscription_messages(
        name=simple_subscription,
        count=1,
        timeout=10,
    )

    assert len(response) == 1
    assert response[0].body == data


@pytest.mark.xfail()
async def test_pop_message(
    provisioning_client: shared.client.AsyncClient, simple_subscription: str
):
    response = await provisioning_client.get_subscription_messages(
        name=simple_subscription, count=1, timeout=1, pop=True
    )

    assert response == []


async def test_get_real_messages(
    provisioning_client: shared.client.AsyncClient, simple_subscription: str, udm: UDM
):
    group = create_message_via_udm_rest_api(udm)  # noqa: F841

    response = await provisioning_client.get_subscription_messages(
        name=simple_subscription,
        timeout=5,
    )

    assert len(response) == 1


async def test_get_multiple_messages(
    provisioning_client: shared.client.AsyncClient, simple_subscription: str, udm: UDM
):
    group1 = create_message_via_udm_rest_api(udm)  # noqa: F841
    group2 = create_message_via_udm_rest_api(udm)  # noqa: F841
    group3 = create_message_via_udm_rest_api(udm)  # noqa: F841

    result = await pop_all_messages(provisioning_client, simple_subscription, 4)
    assert len(result) == 3


@pytest.mark.xfail()
async def test_get_messages_zero_timeout(
    provisioning_client: shared.client.AsyncClient, simple_subscription: str
):
    response = await provisioning_client.get_subscription_messages(
        name=simple_subscription,
        timeout=0,
    )

    assert response == []
