# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

from typing import Any, Optional

import msgpack

from univention.provisioning.backends import message_queue
from univention.provisioning.backends.message_queue import Acknowledgements
from univention.provisioning.models.message import MQMessage

from .config import UDMTransformerSettings, udm_transformer_settings
from .subscriptions_port import SubscriptionsPort


def messagepack_decoder(data: bytes) -> Any:
    return msgpack.unpackb(data)


class NatsSubscriptions(SubscriptionsPort):
    """
    Communicate with message queueing system.
    This adapter implements it using NATS streams.

    Use as an asynchronous context manager to ensure the connection gets closed after usage.
    """

    def __init__(self, settings: Optional[UDMTransformerSettings] = None):
        super().__init__(settings)
        self.settings = settings or udm_transformer_settings()
        self.mq = message_queue(
            server=self.settings.nats_server,
            user=self.settings.nats_user,
            password=self.settings.nats_password,
        )

    async def __aenter__(self) -> SubscriptionsPort:
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        await self.close()
        return False

    async def connect(self):
        await self.mq.connect()

    async def close(self):
        await self.mq.close()

    async def initialize_subscription(self, stream: str, manual_delete: bool, subject: str):
        return await self.mq.initialize_subscription(stream, manual_delete, subject)

    async def get_one_message(self, timeout: float) -> tuple[MQMessage, Acknowledgements]:
        return await self.mq.get_one_message(timeout=timeout, binary_decoder=messagepack_decoder)
