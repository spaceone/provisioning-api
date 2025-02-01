# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import json
from typing import Any, Optional

from univention.provisioning.backends import key_value_store
from univention.provisioning.models.constants import BucketName

from .cache_port import Cache
from .config import UDMTransformerSettings, udm_transformer_settings


class CacheNats(Cache):
    """
    Store and retrieve data in a cache.
    This adapter implements it using the NATS k/v store.

    Use as an asynchronous context manager to ensure DB connection gets closed after usage.
    """

    _bucket_name = BucketName.cache

    def __init__(self, settings: Optional[UDMTransformerSettings] = None):
        super().__init__(settings)
        self.settings = settings or udm_transformer_settings()
        self._kv_store = key_value_store(
            server=self.settings.nats_server,
            user=self.settings.nats_user,
            password=self.settings.nats_password,
        )

    async def __aenter__(self) -> Cache:
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        await self.close()
        return False

    async def connect(self) -> None:
        await self._kv_store.init(buckets=[self._bucket_name])

    async def close(self) -> None:
        await self._kv_store.close()

    async def retrieve(self, key: str) -> dict[str, Any]:
        result = await self._kv_store.get_value(key, self._bucket_name)
        return json.loads(result) if result else {}

    async def store(self, key: str, value: str) -> None:
        await self._kv_store.put_value(key, value, self._bucket_name)
