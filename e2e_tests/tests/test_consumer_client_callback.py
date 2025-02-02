# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import pytest

from univention.provisioning.consumer.api import MessageHandler, MessageHandlerSettings, ProvisioningConsumerClient
from univention.provisioning.models.message import Message


@pytest.fixture()
async def test_no_callback_function_provided(provisioning_client: ProvisioningConsumerClient, dummy_subscription: str):
    with pytest.raises(ValueError, match="Callback functions can't be empty"):
        await MessageHandler(
            client=provisioning_client,
            callbacks=[],
            settings=MessageHandlerSettings(max_acknowledgement_retries=5),
            message_limit=1,
        ).run()


@pytest.mark.timeout(10)
async def test_get_one_message(
    create_message_via_events_api,
    provisioning_client: ProvisioningConsumerClient,
    dummy_subscription: str,
    test_settings,
):
    create_message_via_events_api(test_settings)

    result = []

    async def test_callback(message: Message):
        result.append(message)

    await MessageHandler(
        client=provisioning_client,
        callbacks=[test_callback],
        settings=MessageHandlerSettings(max_acknowledgement_retries=5),
        message_limit=1,
    ).run()

    assert len(result) == 1


async def test_timeout_while_waiting_for_messages(
    create_message_via_events_api,
    provisioning_client: ProvisioningConsumerClient,
    dummy_subscription: str,
    test_settings,
):
    body = create_message_via_events_api(test_settings)

    response = await provisioning_client.get_subscription_message(
        dummy_subscription,
        timeout=10,
    )

    assert response.body == body


@pytest.mark.timeout(10)
async def test_get_multiple_messages(
    create_message_via_events_api,
    provisioning_client: ProvisioningConsumerClient,
    dummy_subscription: str,
    test_settings,
):
    create_message_via_events_api(test_settings)
    create_message_via_events_api(test_settings)
    create_message_via_events_api(test_settings)

    result = []

    async def test_callback(message: Message):
        result.append(message)

    await MessageHandler(
        client=provisioning_client,
        callbacks=[test_callback],
        settings=MessageHandlerSettings(max_acknowledgement_retries=5),
        message_limit=3,
    ).run()

    assert len(result) == 3
