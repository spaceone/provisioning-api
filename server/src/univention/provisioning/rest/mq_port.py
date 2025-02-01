# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import abc
from typing import Optional, Self

from univention.provisioning.models.message import Message, ProvisioningMessage
from univention.provisioning.models.subscription import NewSubscription

from .config import AppSettings


class MessageQueuePort(abc.ABC):
    """
    Handle a message queue communication.

    Use as an async context manager.
    """

    def __init__(self, settings: Optional[AppSettings] = None):
        self.settings = settings

    @abc.abstractmethod
    async def __aenter__(self) -> Self: ...

    @abc.abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool: ...

    @abc.abstractmethod
    async def enqueue_for_dispatcher(self, message: Message) -> None: ...

    async def get_messages_from_main_queue(
        self, subscription: str, timeout: float, pop: bool
    ) -> Optional[ProvisioningMessage]: ...

    async def get_messages_from_prefill_queue(
        self, subscription: str, timeout: float, pop: bool
    ) -> Optional[ProvisioningMessage]: ...

    @abc.abstractmethod
    async def delete_message(self, stream: str, seq_num: int): ...

    @abc.abstractmethod
    async def create_queue(self, stream: str, manual_delete: bool, subjects: list[str] | None = None): ...

    @abc.abstractmethod
    async def delete_queue(self, stream_name: str): ...

    @abc.abstractmethod
    async def create_consumer(self, subject): ...

    @abc.abstractmethod
    async def prepare_new_consumer_queue(self, consumer_name: str): ...

    @abc.abstractmethod
    async def request_prefill(self, subscription: NewSubscription): ...
