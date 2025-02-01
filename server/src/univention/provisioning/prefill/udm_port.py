# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import abc
import logging
from typing import Any, Optional, Self

logger = logging.getLogger(__name__)


class UDMPort(abc.ABC):
    """
    Client for the UDM REST API, providing the interfaces required by `PrefillService` and `UDMMessagingService`.

    It is intended to be used as an async context manager:
    ```
    async with UDMAdapter("http://localhost:9979/udm", "username", "password") as adapter:
        await adapter.get_object_types()
    ```
    """

    def __init__(self, url: str, username: str, password: str): ...

    @abc.abstractmethod
    async def __aenter__(self) -> Self: ...

    @abc.abstractmethod
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool: ...

    @abc.abstractmethod
    async def connect(self) -> None: ...

    @abc.abstractmethod
    async def close(self) -> None: ...

    @abc.abstractmethod
    async def get_object_types(self) -> list[dict[str, Any]]:
        """Return a list of available object types.

        Each entry has the keys `name`, `title` and `href`.
        """
        ...

    @abc.abstractmethod
    async def list_objects(self, object_type: str, position: Optional[str] = None) -> list[str]:
        """Return the URLs of all objects for the given type."""
        ...

    @abc.abstractmethod
    async def get_object(self, url: str) -> dict[str, Any]:
        """Fetch the given UDM object."""
        ...
