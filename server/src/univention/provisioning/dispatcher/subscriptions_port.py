# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import abc
from typing import AsyncGenerator, Awaitable, Callable, Optional, Self

from univention.provisioning.models.subscription import Subscription

from .config import DispatcherSettings


class SubscriptionsPort(abc.ABC):
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
    async def get_all_subscriptions(self) -> AsyncGenerator[Subscription, None]: ...

    @abc.abstractmethod
    async def watch_for_subscription_changes(
        self, callback: Callable[[str, Optional[bytes]], Awaitable[None]]
    ) -> None: ...
