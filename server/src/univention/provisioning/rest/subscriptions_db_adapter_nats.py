# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import json
from typing import Optional, Union

from univention.provisioning.backends import key_value_store
from univention.provisioning.backends.key_value_db import KeyValueDB
from univention.provisioning.models.constants import BucketName
from univention.provisioning.models.subscription import Subscription

from .config import AppSettings, app_settings
from .subscriptions_db_port import NoSubscription, SubscriptionsDBPort

PREFILL_FAILURES_STREAM = "prefill-failures"
PREFILL_DURABLE_NAME = "prefill-service"


class NatsSubscriptionsDB(SubscriptionsDBPort):
    """
    Handle communication with key-value DB.

    Use as an async context manager.
    """

    def __init__(self, settings: Optional[AppSettings] = None):
        super().__init__(settings or app_settings())
        self.kv: Optional[KeyValueDB] = None

    async def __aenter__(self) -> SubscriptionsDBPort:
        self.kv = key_value_store(
            server=self.settings.nats_server,
            user=self.settings.nats_user,
            password=self.settings.nats_password,
        )
        await self.kv.init(buckets=[BucketName.subscriptions, BucketName.credentials])
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        await self.kv.close()
        self.kv = None
        return False

    async def get_dict_value(self, name: str, bucket: BucketName) -> Optional[dict]:
        result = await self.kv.get_value(name, bucket)
        return json.loads(result) if result else None

    async def get_list_value(self, key: str, bucket: BucketName) -> list[str]:
        result = await self.kv.get_value(key, bucket)
        return json.loads(result) if result else []

    async def get_str_value(self, key: str, bucket: BucketName) -> Optional[str]:
        return await self.kv.get_value(key, bucket)

    async def delete_kv_pair(self, key: str, bucket: BucketName):
        await self.kv.delete_kv_pair(key, bucket)

    async def put_value(
        self, key: str, value: Union[str, dict, list], bucket: BucketName, revision: Optional[int] = None
    ):
        await self.kv.put_value(key, value, bucket, revision)

    async def get_bucket_keys(self, bucket: BucketName) -> list[str]:
        return await self.kv.get_keys(bucket)

    async def delete_subscription(self, name: str) -> None:
        await self.delete_kv_pair(name, BucketName.credentials)
        await self.delete_kv_pair(name, BucketName.subscriptions)

    async def load_hashed_password(self, name: str) -> str:
        return await self.get_str_value(name, BucketName.credentials)

    async def load_subscription(self, name: str) -> Optional[Subscription]:
        """
        :raises NoSubscription: if subscription was not found.
        """
        if result := await self.get_dict_value(name, BucketName.subscriptions):
            return Subscription.model_validate(result)
        else:
            raise NoSubscription(f"Subscription not found: {name!r}")

    async def load_subscription_names(self):
        return await self.get_bucket_keys(BucketName.credentials)

    async def store_hashed_password(self, name: str, password: str) -> None:
        await self.put_value(name, password, BucketName.credentials)

    async def store_subscription(self, name: str, subscription: Subscription) -> None:
        await self.put_value(name, subscription.model_dump(), BucketName.subscriptions)
