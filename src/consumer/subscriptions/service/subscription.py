import json
import logging
import re
from typing import List, Optional

from consumer.port import ConsumerPort
from consumer.subscriptions.subscription.sink import SinkManager
from shared.models import Subscriber, NewSubscriber, FillQueueStatus

manager = SinkManager()
logger = logging.getLogger(__name__)


def match_subscription(
    sub_realm: str, sub_topic: str, msg_realm: str, msg_topic: str
) -> bool:
    """Decides whether a message is sent to a subscriber.

    Compares the subscriber's realm and topic to those of the message and
    returns `True` if the message should be sent to the subscriber.
    """

    if sub_realm != msg_realm:
        return False

    return re.fullmatch(sub_topic, msg_topic) is not None


class SubscriptionKeys:
    subscribers = "subscribers"

    def subscriber(subscriber_name: str) -> str:
        return f"subscriber:{subscriber_name}"


class SubscriptionService:
    def __init__(self, port: ConsumerPort):
        self._port = port

    async def get_subscribers(self) -> List[Subscriber]:
        """
        Return a list of names of all known subscribers.
        """

        names = await self.get_subscriber_names()
        subscribers = [await self.get_subscriber(name) for name in names]
        return subscribers

    async def get_subscriber_names(self):
        return await self.get_subscribers_for_key(SubscriptionKeys.subscribers)

    async def get_subscriber_info(self, name: str) -> Optional[dict]:
        sub = await self._port.get_value_by_key(SubscriptionKeys.subscriber(name))
        return json.loads(sub.value.decode("utf-8")) if sub else None

    async def get_subscriber(self, name: str) -> Subscriber:
        """
        Get information about a registered subscriber.
        """
        sub = await self.get_subscriber_info(name)
        if not sub:
            raise ValueError("Subscriber not found.")

        data = dict(
            name=sub["name"],
            realms_topics=sub["realms_topics"],
            fill_queue=sub["fill_queue"],
            fill_queue_status=sub["fill_queue_status"],
        )

        return Subscriber.model_validate(data)

    async def create_subscription(self, sub: NewSubscriber):
        """
        Add a new subscription.
        """
        if sub.fill_queue:
            fill_queue_status = FillQueueStatus.pending
        else:
            fill_queue_status = FillQueueStatus.done

        realm_topic_str = f"{sub.realm_topic[0]}:{sub.realm_topic[1]}"
        sub_info = await self.get_subscriber_info(sub.name)
        if sub_info:
            if realm_topic_str in sub_info["realms_topics"]:
                raise ValueError(
                    "Subscription for the given realm_topic already exists"
                )

            logger.info(
                f"Creating subscription for the realm_topic: '{realm_topic_str}'"
            )
            sub_info["realms_topics"].append(realm_topic_str)
            await self.set_sub_info(sub.name, sub_info)
            await self.update_realm_topic_subscribers(realm_topic_str, sub.name)

            logger.info("Subscription was created")
        else:
            await self.add_subscriber(sub, fill_queue_status, realm_topic_str)

    async def update_realm_topic_subscribers(self, realm_topic_str, name: str):
        await self.update_subscribers_for_key(realm_topic_str, name)

    async def add_subscriber(
        self, sub: NewSubscriber, fill_queue_status: FillQueueStatus, realm_topic_str
    ):
        logger.info(f"Creating new subscriber with the name: '{sub.name}'")

        sub_info = {
            "name": sub.name,
            "realms_topics": [f"{sub.realm_topic[0]}:{sub.realm_topic[1]}"],
            "fill_queue": sub.fill_queue,
            "fill_queue_status": fill_queue_status,
        }
        await self.set_sub_info(sub.name, sub_info)
        await self.add_sub_to_subscribers(sub.name)
        await self.update_realm_topic_subscribers(realm_topic_str, sub.name)

        logger.info("New subscriber was created")

    async def get_subscriber_queue_status(self, name: str) -> FillQueueStatus:
        """Get the pre-fill status of the given subscriber."""

        sub_info = await self.get_subscriber_info(name)
        if not sub_info:
            raise ValueError("Subscriber not found.")

        status = sub_info["fill_queue_status"]
        return FillQueueStatus[status]

    async def set_subscriber_queue_status(self, name: str, status: FillQueueStatus):
        """Set the pre-fill status of the given subscriber."""
        sub_info = await self.get_subscriber_info(name)
        if not sub_info:
            raise ValueError("Subscriber not found.")

        sub_info["fill_queue_status"] = status.name
        await self.set_sub_info(name, sub_info)

    async def cancel_subscription(self, name: str, realm_topic: str):
        sub_info = await self.get_subscriber_info(name)
        if not sub_info:
            raise ValueError("Subscriber not found.")

        realms_topics = sub_info["realms_topics"]
        if realm_topic not in realms_topics:
            raise ValueError("Subscription for the given realm_topic doesn't exist")

        realms_topics.remove(realm_topic)
        await self.set_sub_info(name, sub_info)

    async def set_sub_info(self, name, sub_info):
        await self._port.put_value_by_key(SubscriptionKeys.subscriber(name), sub_info)

    async def delete_subscriber(self, name: str):
        """
        Delete a subscriber and all of its data.
        """
        await manager.close(name)
        await self.delete_sub_from_subscribers(name)
        await self.delete_sub_info(SubscriptionKeys.subscriber(name))
        await self._port.delete_queue(name)

    async def delete_sub_from_subscribers(self, name: str):
        await self.delete_subscriber_from_key(SubscriptionKeys.subscribers, name)

    async def delete_sub_info(self, name: str):
        await self._port.delete_key(SubscriptionKeys.subscriber(name))

    async def get_subscribers_for_key(self, key: str) -> List[str]:
        names = await self._port.get_value_by_key(key)
        return names.value.decode("utf-8").split(",") if names else []

    async def delete_subscriber_from_key(self, key: str, name: str):
        subs = await self.get_subscribers_for_key(key)
        subs.remove(name)
        if not subs:
            await self._port.delete_key(key)
        else:
            await self._port.put_value_by_key(key, ",".join(subs))

    async def update_subscribers_for_key(self, key: str, value: str) -> None:
        subs = await self._port.get_value_by_key(key)
        if subs:
            value = subs.value.decode("utf-8") + f",{value}"
        await self._port.put_value_by_key(key, value)

    async def add_sub_to_subscribers(self, name: str):
        await self.update_subscribers_for_key(SubscriptionKeys.subscribers, name)
