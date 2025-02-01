# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import abc
from typing import Optional, Self

from univention.provisioning.models.message import Message, MQMessage

from .config import DispatcherSettings


class MessageQueuePort(abc.ABC):
    def __init__(self, settings: Optional[DispatcherSettings] = None):
        self.settings = settings

    @abc.abstractmethod
    async def __aenter__(self) -> Self: ...

    @abc.abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool: ...

    @abc.abstractmethod
    async def connect(self) -> None: ...

    @abc.abstractmethod
    async def close(self) -> None: ...

    @abc.abstractmethod
    async def enqueue_message(self, queue: str, message: Message) -> None: ...

    @abc.abstractmethod
    async def subscribe_to_queue(self) -> None: ...

    @abc.abstractmethod
    async def wait_for_event(self) -> MQMessage: ...

    @abc.abstractmethod
    async def acknowledge_message(self, message: MQMessage) -> None: ...

    @abc.abstractmethod
    async def acknowledge_message_in_progress(self, message: MQMessage) -> None: ...
