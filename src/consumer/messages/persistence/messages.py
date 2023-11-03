<<<<<<< HEAD
from typing import Annotated, List, Optional, Tuple
=======
import json
from typing import Annotated, Dict, cast, List, Optional, Tuple
>>>>>>> ede9f0c (fix: change method to pusblish message to the nats)

import core.models
import fastapi
from redis.asyncio import Redis
import redis.asyncio as redis
from consumer.core.persistence.redis import RedisDependency
from consumer.port import Port
from core.models import Message


class Keys:
    """A list of keys used in Redis and Nats for queueing messages."""

    def queue(subscriber_name):
        return f"queue:{subscriber_name}"

    def stream(subscriber_name):
        return f"stream:{subscriber_name}"

    def subject(subscriber_name):
        return f"subject:{subscriber_name}"


class MessageRepository:
    """Store and retrieve messages from Redis."""

    def __init__(self, redis: Redis):
        self.redis = redis
        self.port = Port(redis)

    async def add_live_message(self, subscriber_name: str, message: Message):
        """Enqueue the given message for a particular subscriber.

        The message is received "live" and placed at the end of the queue
        (i.e. with the current timestamp).

        :param str subscriber_name: Name of the subscriber.
        :param core.models.Message message: The message from the publisher.
        """

        await self.port.add_live_message(subscriber_name, message)

    async def add_prefill_message(
        self, subscriber_name: str, message: core.models.Message
    ):
        """Enqueue the given message for a particular subscriber.

        The message originates from the pre-fill process and is queued
        ahead of (== earlier than) live messages.

        :param str subscriber_name: Name of the subscriber.
        :param core.models.Message message: The message from the pre-fill task.
        """
        await self.port.add_prefill_message(subscriber_name, message)

    async def delete_prefill_messages(self, subscriber_name: str):
        """Delete all pre-fill messages from the subscriber's queue."""
        await self.port.delete_prefill_messages(subscriber_name)

    async def get_next_message(
        self, subscriber_name: str, block: Optional[int] = None
    ) -> Optional[Tuple[str, core.models.Message]]:
        """Retrieve the first message from the subscriber's stream.

        :param str subscriber_id: Id of the subscriber.
        :param int block: How long to block in milliseconds if no message is available.
        """
        return await self.port.get_next_message(subscriber_name, block)

    async def get_messages(
        self,
        subscriber_name: str,
        count: Optional[int] = None,
        first: int | str = "-",
        last: int | str = "+",
    ) -> List[Tuple[str, core.models.Message]]:
        """Return messages from a given queue.

        By default, *all* messages will be returned unless further restricted by
        parameters.

        :param str subscriber_id: Id of the subscriber.
        :param int count: How many messages to return at most.
        :param str first: Id of the first message to return.
        :param str last: Id of the last message to return.
        """
        return await self.port.get_messages(subscriber_name, count, first, last)

    async def delete_message(self, subscriber_name: str, message_id: str):
        """Remove a message from the subscriber's queue.

        :param str subscriber_id: Id of the subscriber.
        :param str message_id: Id of the message to delete.
        """
        await self.port.delete_message(subscriber_name, message_id)

    async def delete_queue(self, subscriber_name: str):
        """Delete the entire queue for the given consumer.

        :param str subscriber_id: Id of the subscriber.
        """

        await self.port.delete_queue(subscriber_name)


def get_message_repository(redis: RedisDependency) -> MessageRepository:
    return MessageRepository(redis)


DependsMessageRepo = Annotated[
    MessageRepository, fastapi.Depends(get_message_repository)
]
