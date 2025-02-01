# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

from unittest.mock import AsyncMock, call

import pytest

from univention.provisioning.models.constants import (
    DISPATCHER_QUEUE_NAME,
    DISPATCHER_SUBJECT_TEMPLATE,
    PREFILL_SUBJECT_TEMPLATE,
)
from univention.provisioning.models.message import MessageProcessingStatus
from univention.provisioning.models.subscription import FillQueueStatus
from univention.provisioning.rest.message_service import MessageService
from univention.provisioning.rest.mq_adapter_nats import NatsMessageQueue
from univention.provisioning.testing.mock_data import MESSAGE, SUBSCRIPTION_NAME

MESSAGE_PROCESSING_STATUS = MessageProcessingStatus.ok
MESSAGE_PROCESSING_SEQ_ID = 1


@pytest.fixture
def message_service() -> MessageService:
    ms = MessageService(subscriptions_db=AsyncMock(), mq=AsyncMock())
    ms._subscription_prefill_done.clear()
    return ms


@pytest.mark.anyio
class TestMessageService:
    prefill_subject = PREFILL_SUBJECT_TEMPLATE.format(subscription=SUBSCRIPTION_NAME)
    main_subject = DISPATCHER_SUBJECT_TEMPLATE.format(subscription=SUBSCRIPTION_NAME)

    async def test_get_next_message_prefill_running(self, message_service: MessageService):
        message_service.sub_service.get_subscription_queue_status = AsyncMock(return_value=FillQueueStatus.running)

        result = await message_service.get_next_message(SUBSCRIPTION_NAME, timeout=1, pop=True)

        message_service.sub_service.get_subscription_queue_status.assert_has_calls(
            [call(SUBSCRIPTION_NAME), call(SUBSCRIPTION_NAME)]
        )
        message_service.mq.get_message.assert_not_called()
        assert result is None

    async def test_get_next_message_from_prefill_subject(self, message_service: MessageService):
        message_service.sub_service.get_subscription_queue_status = AsyncMock(return_value=FillQueueStatus.done)
        message_service.mq.get_messages_from_prefill_queue = AsyncMock(return_value=MESSAGE)

        result = await message_service.get_next_message(SUBSCRIPTION_NAME, timeout=5, pop=True)

        message_service.sub_service.get_subscription_queue_status.assert_called_once_with(SUBSCRIPTION_NAME)
        message_service.mq.get_messages_from_prefill_queue.assert_called_once_with(SUBSCRIPTION_NAME, 5, True)
        assert result == MESSAGE

    async def test_get_next_message_from_main_subject(self, message_service: MessageService):
        message_service.sub_service.get_subscription_queue_status = AsyncMock(return_value=FillQueueStatus.done)
        message_service.mq.get_messages_from_main_queue = AsyncMock(return_value=MESSAGE)
        message_service._subscription_prefill_done[SUBSCRIPTION_NAME] = True

        result = await message_service.get_next_message(SUBSCRIPTION_NAME, timeout=5, pop=True)

        message_service.sub_service.get_subscription_queue_status.assert_not_called()
        message_service.mq.get_messages_from_main_queue.assert_has_calls([call(SUBSCRIPTION_NAME, 5, True)])
        assert result == MESSAGE

    async def test_post_message_status(self, message_service: MessageService):
        message_service.mq.delete_message = AsyncMock()

        result = await message_service.update_message_status(
            SUBSCRIPTION_NAME, MESSAGE_PROCESSING_SEQ_ID, MESSAGE_PROCESSING_STATUS
        )

        message_service.mq.delete_message.assert_called_once_with(SUBSCRIPTION_NAME, 1)
        assert result is None

    async def test_add_live_message(self):
        mq = NatsMessageQueue()
        mq.mq = AsyncMock()
        message_service = MessageService(subscriptions_db=AsyncMock(), mq=mq)

        await message_service.mq.enqueue_for_dispatcher(MESSAGE)

        message_service.mq.mq.add_message.assert_called_once_with(DISPATCHER_QUEUE_NAME, DISPATCHER_QUEUE_NAME, MESSAGE)
