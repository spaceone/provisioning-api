# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

from typing import Optional, Tuple

from univention.provisioning.backends import message_queue
from univention.provisioning.backends.message_queue import Acknowledgements
from univention.provisioning.models.constants import PREFILL_QUEUE_NAME, PREFILL_SUBJECT_TEMPLATE
from univention.provisioning.models.message import BaseMessage, MQMessage

from .config import PrefillSettings, prefill_settings
from .mq_port import MessageQueuePort

PREFILL_FAILURES_STREAM = "prefill-failures"
PREFILL_DURABLE_NAME = "prefill-service"


class NatsMessageQueue(MessageQueuePort):
    """
    Handle a message queue communication with NATS.

    Use as an async context manager.
    """

    def __init__(self, settings: Optional[PrefillSettings] = None):
        super().__init__(settings or prefill_settings())
        self.mq = message_queue(
            server=self.settings.nats_server,
            user=settings.nats_user,
            password=settings.nats_password,
            max_reconnect_attempts=settings.nats_max_reconnect_attempts,
        )

    async def __aenter__(self) -> MessageQueuePort:
        await self.mq.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        await self.mq.close()
        return False

    async def add_message_to_failures_queue(self, message: BaseMessage) -> None:
        await self.mq.add_message(PREFILL_FAILURES_STREAM, PREFILL_FAILURES_STREAM, message)

    async def add_message_to_queue(self, queue: str, message: BaseMessage) -> None:
        await self.mq.add_message(queue, PREFILL_SUBJECT_TEMPLATE.format(subscription=queue), message)

    async def get_one_message(self) -> Tuple[MQMessage, Acknowledgements]:
        return await self.mq.get_one_message()

    async def initialize_subscription(self) -> None:
        await self.mq.initialize_subscription(PREFILL_QUEUE_NAME, False, None)

    async def prepare_failures_queue(self) -> None:
        await self.mq.ensure_stream(PREFILL_FAILURES_STREAM, False)
        await self.mq.ensure_consumer(PREFILL_FAILURES_STREAM)

    async def purge_queue(self, name: str) -> None:
        await self.mq.purge_stream(name, PREFILL_SUBJECT_TEMPLATE.format(subscription=name))
