# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import abc
from typing import Optional, Self

from .config import UDMTransformerSettings


class Cache(abc.ABC):
    """
    Store and retrieve data in a cache.

    Use as an asynchronous context manager to ensure DB connection gets closed after usage.
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
    async def retrieve(self, key: str) -> dict: ...

    @abc.abstractmethod
    async def store(self, key: str, value: str): ...
