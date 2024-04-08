# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import logging
from typing import Any, Callable, Coroutine, Dict, List, Optional, Tuple

import aiohttp

from .config import Settings
from shared.models import (
    Subscription,
    NewSubscription,
    MessageProcessingStatus,
    Event,
    ProvisioningMessage,
    MessageProcessingStatusReport,
    Message,
)

logger = logging.getLogger(__file__)


# TODO: the subscription part will be delegated to an admin using an admin API
class AsyncClient:
    def __init__(
        self, settings: Optional[Settings] = None, concurrency_limit: int = 10
    ):
        self.settings = settings or Settings()
        auth = aiohttp.BasicAuth(
            self.settings.provisioning_api_username,
            self.settings.provisioning_api_password,
        )
        connector = aiohttp.TCPConnector(limit=concurrency_limit)
        self.session = aiohttp.ClientSession(
            auth=auth, connector=connector, raise_for_status=True
        )

    async def close(self):
        await self.session.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    # TODO: move this method to the AdminClient
    async def create_subscription(
        self,
        name: str,
        password: str,
        realms_topics: List[Tuple[str, str]],
        request_prefill: bool = False,
    ):
        logger.info("creating subscription for %s", str(realms_topics))
        subscription = NewSubscription(
            name=name,
            realms_topics=realms_topics,
            request_prefill=request_prefill,
            password=password,
        )

        logger.debug(subscription.model_dump())
        return await self.session.post(
            f"{self.settings.provisioning_api_base_url}/internal/admin/v1/subscriptions",
            json=subscription.model_dump(),
        )

    async def cancel_subscription(self, name: str):
        return await self.session.delete(
            f"{self.settings.consumer_registration_url}/subscriptions/{name}",
        )

    async def get_subscription(self, name: str) -> Subscription:
        response = await self.session.get(
            f"{self.settings.consumer_registration_url}/subscriptions/{name}"
        )
        data = await response.json()
        return Subscription.model_validate(data)

    async def get_subscription_messages(
        self,
        name: str,
        count: Optional[int] = None,
        timeout: Optional[float] = None,
        pop: Optional[bool] = None,
    ) -> List[ProvisioningMessage]:
        _params = {
            "count": count,
            "timeout": timeout,
            "pop": pop,
        }
        params = {k: v for k, v in _params.items() if v is not None}

        response = await self.session.get(
            f"{self.settings.consumer_messages_url}/subscriptions/{name}/messages",
            params=params,
        )
        msgs = await response.json()
        return [ProvisioningMessage.model_validate(msg) for msg in msgs]

    async def set_message_status(
        self, name: str, reports: List[MessageProcessingStatusReport]
    ):
        return await self.session.post(
            f"{self.settings.consumer_messages_url}/subscriptions/{name}/messages-status",
            json=[report.model_dump() for report in reports],
        )

    # TODO: move this method to the AdminClient
    async def get_subscriptions(self) -> List[Subscription]:
        response = await self.session.get(
            f"{self.settings.consumer_registration_url}/subscriptions"
        )
        data = await response.json()
        # TODO: parse a list of subscriptions instead
        return [Subscription.model_validate(data)]

    # FIXME: What is the purpose of this method? It looks like it wants to publish_event via Event API
    async def submit_message(
        self, realm: str, topic: str, body: Dict[str, Any], name: str
    ):
        message = Event(realm=realm, topic=topic, body=body)

        return await self.session.post(
            f"{self.settings.consumer_registration_url}/messages",
            json=message.model_dump(),
        )


class MessageHandler:
    def __init__(
        self,
        client: AsyncClient,
        subscription_name: str,
        callbacks: List[Callable[[Message], Coroutine[None, None, None]]],
        pop_after_handling: bool = True,
        message_limit: Optional[int] = None,
    ):
        """
        Each callback should be an asynchronous function to facilitate downstream asynchronous operations.
        To prevent race conditions between message processing and callback execution,
        each message and its callbacks are handled sequentially.

        Args:
            client: AsyncClient
            subscription_name: The name of the target subscription to listen on.
            callbacks: A list of asynchronous callback functions, each accepting a single message object as a parameter.
            message_limit: An optional integer specifying the maximum number of messages to process,
                primarily to facilitate testing.
            pop_after_handling: If False, messages are acknowledged immediately upon reception
                rather than after all callbacks for the message have been successfully executed.
        """
        if not callbacks:
            raise ValueError("Callback functions can't be empty")
        self.client = client
        self.subscription_name = subscription_name
        self.callbacks = callbacks
        self.pop_after_handling = pop_after_handling
        self.message_limit = message_limit

    async def _callback_wrapper(
        self,
        message: ProvisioningMessage,
        callback: Callable[[Message], Coroutine[None, None, None]],
    ):
        """
        Wrapper around a client's callback function to encapsulate
        error handling and message acknowledgement
        """
        # TODO: Message is not enough, we need a specific ClientMessage, that includes for example num_redelivered
        msg = Message(
            publisher_name=message.publisher_name,
            ts=message.ts,
            realm=message.realm,
            topic=message.topic,
            body=message.body,
        )
        await callback(msg)
        if not self.pop_after_handling:
            return
        try:
            # TODO: Retry acknowledgement
            report = MessageProcessingStatusReport(
                status=MessageProcessingStatus.ok,
                message_seq_num=message.sequence_number,
                publisher_name=message.publisher_name,
            )
            await self.client.set_message_status(self.subscription_name, [report])
        except (
            aiohttp.ClientError,
            aiohttp.ClientConnectionError,
            aiohttp.ClientResponseError,
        ) as exc:
            logger.error(
                "Failed to acknowledge message. It will be redelivered later. - %s",
                repr(exc),
            )

    async def run(
        self,
    ):
        """
        This method is designed for asynchronous execution, either by awaiting it within an async context
        or using `asyncio.run()`.
        It continuously listens for messages, either indefinitely or until a specified message limit is reached, and
        invokes a series of callbacks for each message.
        """
        counter = 0

        while True:
            messages = await self.client.get_subscription_messages(
                self.subscription_name,
                count=1,
                timeout=10,
                # TODO: pop is broken serverside at the moment
                # pop= not pop_after_handling,
            )

            for message in messages:
                for callback in self.callbacks:
                    await self._callback_wrapper(message, callback)
                if self.message_limit:
                    counter += 1
                    if counter >= self.message_limit:
                        return
