# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import abc
from typing import Optional, Self, Union

from univention.provisioning.models.constants import BucketName
from univention.provisioning.models.subscription import Subscription

from .config import AppSettings


class NoSubscription(Exception): ...


class SubscriptionsDBPort(abc.ABC):
    """
    Load/Store data from/in the SubscriptionsDB.

    Use as an async context manager.
    """

    def __init__(self, settings: Optional[AppSettings] = None):
        self.settings = settings

    @abc.abstractmethod
    async def __aenter__(self) -> Self: ...

    @abc.abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool: ...

    @abc.abstractmethod
    async def get_dict_value(self, name: str, bucket: BucketName) -> Optional[dict]: ...

    @abc.abstractmethod
    async def get_list_value(self, key: str, bucket: BucketName) -> list[str]: ...

    @abc.abstractmethod
    async def get_str_value(self, key: str, bucket: BucketName) -> Optional[str]: ...

    @abc.abstractmethod
    async def delete_kv_pair(self, key: str, bucket: BucketName): ...

    @abc.abstractmethod
    async def put_value(
        self, key: str, value: Union[str, dict, list], bucket: BucketName, revision: Optional[int] = None
    ): ...

    @abc.abstractmethod
    async def get_bucket_keys(self, bucket: BucketName) -> list[str]: ...

    @abc.abstractmethod
    async def delete_subscription(self, name: str) -> None: ...

    @abc.abstractmethod
    async def load_hashed_password(self, name: str) -> str: ...

    @abc.abstractmethod
    async def load_subscription(self, name: str) -> Optional[Subscription]:
        """
        :raises NoSubscription: if subscription was not found.
        """
        ...

    @abc.abstractmethod
    async def load_subscription_names(self): ...

    @abc.abstractmethod
    async def store_hashed_password(self, name: str, password: str) -> None: ...

    @abc.abstractmethod
    async def store_subscription(self, name: str, subscription: Subscription) -> None: ...
