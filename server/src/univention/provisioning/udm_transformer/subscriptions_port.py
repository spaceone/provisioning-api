# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import abc
from typing import Optional, Self

from univention.provisioning.backends.message_queue import Acknowledgements
from univention.provisioning.models.message import MQMessage

from .config import UDMTransformerSettings


class SubscriptionsPort(abc.ABC):
    """
    Communicate with message queueing system.

    Use as an asynchronous context manager to ensure the connection gets closed after usage.
    """

    def __init__(self, settings: Optional[UDMTransformerSettings] = None): ...

    @abc.abstractmethod
    async def __aenter__(self) -> Self: ...

    @abc.abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool: ...

    @abc.abstractmethod
    async def connect(self): ...

    @abc.abstractmethod
    async def close(self): ...

    @abc.abstractmethod
    async def initialize_subscription(self, stream: str, manual_delete: bool, subject: str): ...

    @abc.abstractmethod
    async def get_one_message(self, timeout: float) -> tuple[MQMessage, Acknowledgements]: ...
