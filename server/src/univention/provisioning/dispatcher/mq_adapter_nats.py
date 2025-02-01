# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

from typing import Optional

from univention.provisioning.backends import message_queue
from univention.provisioning.models.constants import DISPATCHER_QUEUE_NAME, DISPATCHER_SUBJECT_TEMPLATE
from univention.provisioning.models.message import Message, MQMessage

from .config import DispatcherSettings, dispatcher_settings
from .mq_port import MessageQueuePort


class NatsMessageQueueAdapter(MessageQueuePort):
    def __init__(self, settings: Optional[DispatcherSettings] = None):
        super().__init__(settings or dispatcher_settings())
        self.mq = message_queue(
            server=self.settings.nats_server,
            user=self.settings.nats_user,
            password=self.settings.nats_password,
            max_reconnect_attempts=self.settings.nats_max_reconnect_attempts,
        )

    async def __aenter__(self) -> MessageQueuePort:
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        await self.close()
        return False

    async def connect(self) -> None:
        await self.mq.connect()

    async def close(self) -> None:
        await self.mq.close()

    async def enqueue_message(self, queue: str, message: Message) -> None:
        await self.mq.add_message(queue, DISPATCHER_SUBJECT_TEMPLATE.format(subscription=queue), message)

    async def subscribe_to_queue(self) -> None:
        await self.mq.subscribe_to_queue(DISPATCHER_QUEUE_NAME, "dispatcher-service")

    async def wait_for_event(self) -> MQMessage:
        return await self.mq.wait_for_event()

    async def acknowledge_message(self, message: MQMessage) -> None:
        await self.mq.acknowledge_message(message)

    async def acknowledge_message_in_progress(self, message: MQMessage) -> None:
        await self.mq.acknowledge_message_in_progress(message)
