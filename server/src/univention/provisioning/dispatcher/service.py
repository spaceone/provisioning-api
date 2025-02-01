# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH
import asyncio
import logging

from univention.provisioning.backends.message_queue import MessageAckManager
from univention.provisioning.models.message import Message, MQMessage
from univention.provisioning.models.subscription import Subscription

from .mq_port import MessageQueuePort
from .subscriptions_port import SubscriptionsPort

logger = logging.getLogger(__name__)


class DispatcherService:
    def __init__(self, ack_manager: MessageAckManager, mq: MessageQueuePort, subscriptions: SubscriptionsPort):
        self.ack_manager = ack_manager
        self.mq = mq
        self.subscriptions_db = subscriptions
        self._subscriptions: dict[str, dict[str, set[Subscription]]] = {}  # {realm: {topic: {Subscription, ..}}}

    async def dispatch_events(self):
        logger.info("Storing event in consumer queues")
        await self.mq.subscribe_to_queue()

        # Initially fill self._subscriptions before starting to handle messages.
        await self.update_subscriptions_mapping()

        async with asyncio.TaskGroup() as task_group:
            # Background task that that informs about changes to the subscription data in the KV store.
            task_group.create_task(
                self.subscriptions_db.watch_for_subscription_changes(self.update_subscriptions_mapping)
            )

            while True:
                logger.debug("Waiting for an event...")
                message = await self.mq.wait_for_event()
                message_handler = self.handle_message(message)
                acknowledge_message_in_progress = lambda: self.mq.acknowledge_message_in_progress(message)  # noqa: E731
                await task_group.create_task(
                    self.ack_manager.process_message_with_ack_wait_extension(
                        message_handler, acknowledge_message_in_progress
                    )
                )

    async def handle_message(self, message: MQMessage):
        data = message.data
        if data.get("realm") == "udm":
            old = data.get("body", {}).get("old", {})
            new = data.get("body", {}).get("new", {})
            dn = new.get("dn") or old.get("dn")
            details = f"DN: {dn!r} "
        else:
            details = ""
        logger.info(
            "Received message to handle (%sPublisher: %r Realm: %r Topic: %r TS: %s).",
            details,
            data.get("publisher_name"),
            data.get("realm"),
            data.get("topic"),
            data.get("ts"),
        )
        logger.debug("Message content: %r", data)

        validated_msg = Message.model_validate(data)

        subscriptions = self._subscriptions.get(validated_msg.realm, {}).get(validated_msg.topic, [])

        for sub in subscriptions:
            logger.info("Sending message to %r", sub.name)
            await self.mq.enqueue_message(sub.name, validated_msg)
        if not subscriptions:
            logger.info("No consumers for message with realm: %r topic: %r.", validated_msg.realm, validated_msg.topic)

        await self.mq.acknowledge_message(message)

    async def update_subscriptions_mapping(self, *args, **kwargs) -> None:
        new_subscriptions_mapping: dict[str, dict[str, set[Subscription]]] = {}
        async for sub in self.subscriptions_db.get_all_subscriptions():
            for realm_topic in sub.realms_topics:
                new_subscriptions_mapping.setdefault(realm_topic.realm, {}).setdefault(realm_topic.topic, set()).add(
                    sub
                )
        self._subscriptions = new_subscriptions_mapping
        logger.info(
            "Subscriptions mapping updated: %r",
            {r: {t: {_s.name for _s in s} for t, s in v.items()} for r, v in self._subscriptions.items()},
        )
