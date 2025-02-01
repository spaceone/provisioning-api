# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import aiohttp

from univention.provisioning.models.message import Message

from .event_sender_port import EventSender


class MessagesRestApiEventSender(EventSender):
    """Send an event through the Messages REST API."""

    def __init__(self, url: str, username: str, password: str):
        super().__init__(url=url, username=username, password=password)
        self._url = self._url.rstrip("/")
        self._auth = aiohttp.BasicAuth(username, password)
        self._headers = [("accept", "application/json")]
        self._session = None

    async def __aenter__(self) -> EventSender:
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        await self.close()
        return False

    async def connect(self):
        if not self._session:
            self._session = aiohttp.ClientSession(auth=self._auth, headers=self._headers, raise_for_status=True)

    async def close(self):
        if self._session:
            await self._session.close()

    async def send_event(self, message: Message):
        async with self._session.post(f"{self._url}/v1/messages", json=message.model_dump()):
            pass
