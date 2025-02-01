# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

from typing import AsyncGenerator, Awaitable, Callable, Optional

from univention.provisioning.backends import key_value_store
from univention.provisioning.models.constants import BucketName
from univention.provisioning.models.subscription import Subscription

from .config import DispatcherSettings, dispatcher_settings
from .subscriptions_port import SubscriptionsPort


class NatsSubscriptionsAdapter(SubscriptionsPort):
    def __init__(self, settings: Optional[DispatcherSettings] = None):
        super().__init__(settings or dispatcher_settings())
        self.kv = key_value_store(
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

    async def connect(self) -> None:
        await self.kv.init(buckets=[BucketName.subscriptions])

    async def close(self) -> None:
        await self.kv.close()

    async def get_all_subscriptions(self) -> AsyncGenerator[Subscription, None]:
        async for sub in self.kv.get_all_subscriptions():
            yield sub

    async def watch_for_subscription_changes(self, callback: Callable[[str, Optional[bytes]], Awaitable[None]]) -> None:
        await self.kv.watch_for_subscription_changes(callback)
