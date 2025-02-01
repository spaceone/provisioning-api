# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import abc
from typing import Self

from univention.provisioning.models.message import Message


class EventSender(abc.ABC):
    """Send an event."""

    def __init__(self, url: str, username: str, password: str):
        self._url = url
        self._username = username
        self._password = password

    @abc.abstractmethod
    async def __aenter__(self) -> Self: ...

    @abc.abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool: ...

    @abc.abstractmethod
    async def connect(self) -> None: ...

    @abc.abstractmethod
    async def close(self) -> None: ...

    @abc.abstractmethod
    async def send_event(self, message: Message) -> None: ...
