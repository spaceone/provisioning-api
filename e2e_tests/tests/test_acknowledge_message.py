# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

from univention.provisioning.consumer.api import ProvisioningConsumerClient
from univention.provisioning.models.message import MessageProcessingStatus


async def test_get_multiple_messages(
    create_message_via_events_api,
    provisioning_client: ProvisioningConsumerClient,
    dummy_subscription: str,
    test_settings,
):
    body_1 = create_message_via_events_api(test_settings)
    body_2 = create_message_via_events_api(test_settings)

    for body in [body_1, body_2]:
        message = await provisioning_client.get_subscription_message(name=dummy_subscription, timeout=5)
        assert message.body == body
        await provisioning_client.set_message_status(
            dummy_subscription, message.sequence_number, MessageProcessingStatus.ok
        )


async def test_acknowledge_messages(
    create_message_via_events_api,
    provisioning_client: ProvisioningConsumerClient,
    dummy_subscription: str,
    test_settings,
):
    body = create_message_via_events_api(test_settings)

    message = await provisioning_client.get_subscription_message(name=dummy_subscription, timeout=5)

    assert message.body == body

    await provisioning_client.set_message_status(
        dummy_subscription, message.sequence_number, MessageProcessingStatus.ok
    )

    response2 = await provisioning_client.get_subscription_message(name=dummy_subscription, timeout=35)

    # Sometimes other messages like the "DC Backup Hosts" group can sneak into the queue
    # The test is considered successful if the response is empty, or if the response is not a redelivery
    # of the previous message
    if response2:
        assert response2.body != body


async def test_do_not_acknowledge_message(
    create_message_via_events_api,
    provisioning_client: ProvisioningConsumerClient,
    dummy_subscription: str,
    test_settings,
):
    body = create_message_via_events_api(test_settings)
    create_message_via_events_api(test_settings)
    create_message_via_events_api(test_settings)

    # test the first delivery of a message
    message = await provisioning_client.get_subscription_message(name=dummy_subscription, timeout=5)
    assert message.body == body

    # test that the next message will not be delivered until the first one is temporary out of the stream
    message2 = await provisioning_client.get_subscription_message(name=dummy_subscription, timeout=5)
    assert message2 is None

    # test that the same unacknowledged message is delivered after 30 seconds of unavailability
    message3 = await provisioning_client.get_subscription_message(name=dummy_subscription, timeout=30)
    assert message3.body == body
