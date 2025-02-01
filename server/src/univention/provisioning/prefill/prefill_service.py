# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH
import logging
import re
from datetime import datetime
from typing import Optional

from pydantic import ValidationError

from univention.provisioning.backends.message_queue import Empty, MessageAckManager
from univention.provisioning.models.constants import PublisherName
from univention.provisioning.models.message import (
    Body,
    Message,
    MQMessage,
    PrefillMessage,
    SimpleMessage,
)
from univention.provisioning.models.subscription import FillQueueStatus

from .config import PrefillSettings, prefill_settings
from .mq_port import MessageQueuePort
from .udm_port import UDMPort
from .update_sub_q_status_port import UpdateSubscriptionsQueueStatusPort

logger = logging.getLogger(__name__)


class PrefillFailedError(Exception): ...


class PrefillService:
    def __init__(
        self,
        ack_manager: MessageAckManager,
        mq: MessageQueuePort,
        udm: UDMPort,
        update_sub_q_status: UpdateSubscriptionsQueueStatusPort,
        settings: Optional[PrefillSettings] = None,
    ):
        super().__init__()
        self.ack_manager = ack_manager
        self.mq = mq
        self.udm = udm
        self.update_sub_q_status = update_sub_q_status
        self.settings = settings or prefill_settings()

        self.max_prefill_attempts = self.settings.max_prefill_attempts

    async def handle_requests_to_prefill(self):
        logger.info("Handling the requests to prefill")

        await self.mq.prepare_failures_queue()
        await self.mq.initialize_subscription()

        while True:
            logger.debug("Waiting for new prefill requests...")
            try:
                message, acknowledgements = await self.mq.get_one_message()
            except Empty:
                continue

            message_handler = self.handle_message(message)
            try:
                await self.ack_manager.process_message_with_ack_wait_extension(
                    message_handler,
                    acknowledgements.acknowledge_message_in_progress,
                )
            except PrefillFailedError:
                logger.warning(
                    "A recoverable error happened while processing the prefill request. "
                    "The prefill request will be scheduled for redelivery and retried at a later time."
                )
                await acknowledgements.acknowledge_message_negatively()
                continue
            except ValidationError:
                await acknowledgements.acknowledge_message_negatively()
                raise
            except Exception:
                logger.exception("Unknown error occurred while processing the prefill request.")
                await acknowledgements.acknowledge_message_negatively()
                raise

            await acknowledgements.acknowledge_message()

    async def handle_message(self, message: MQMessage):
        data = message.data
        logger.info(
            "Received message to handle (Subscription: %r Realms-Topics: %r TS: %s).",
            data.get("subscription_name"),
            data.get("realms_topics"),
            data.get("ts"),
        )
        logger.debug("Message content: %r", data)
        try:
            validated_message = PrefillMessage.model_validate(data)
        except ValidationError:
            logger.exception(
                "failed to parse message with sequence_number: %r",
                message.sequence_number,
            )
            raise

        if self.max_prefill_attempts != -1 and message.num_delivered > self.max_prefill_attempts:
            logger.error(
                "The maximum number of retries for prefilling the subscription %s has been reached. "
                "The prefill will not be retried again and the subscription will be marked as failed. "
                "To trigger the prefill again, delete and recreate the subscription."
            )
            await self.add_to_failure_queue(data)
            await self.update_sub_q_status.update_subscription_queue_status(
                validated_message.subscription_name, FillQueueStatus.failed
            )
            return

        await self._handle_message(validated_message)

        await self.update_sub_q_status.update_subscription_queue_status(
            validated_message.subscription_name, FillQueueStatus.done
        )

    async def _handle_message(self, message: PrefillMessage):
        await self.mq.purge_queue(message.subscription_name)

        for realm_topic in message.realms_topics:
            if realm_topic.realm != "udm":
                # FIXME: unhandled realm
                logger.error("Unhandled realm: %r", realm_topic.realm)
                continue

            logger.info(
                "Started the prefill for the subscriber %r with the topic %r",
                message.subscription_name,
                realm_topic.topic,
            )
            await self.update_sub_q_status.update_subscription_queue_status(
                message.subscription_name, FillQueueStatus.running
            )
            await self.fetch_udm(message.subscription_name, realm_topic.topic)

    async def fetch_udm(self, subscription_name: str, topic: str) -> None:
        """
        Start fetching all data for the given topic.
        Find all UDM object types that match the given topic.
        """

        udm_modules = await self.udm.get_object_types()
        udm_match = [module for module in udm_modules if self.match_topic(topic, module["name"])]

        if not udm_match:
            logger.warning("No UDM module matches topic %r", topic)
            return

        for module in udm_match:
            this_topic = module["name"]
            logger.info("Grabbing %r objects.", this_topic)
            await self._fill_udm_topic(this_topic, subscription_name)

    @staticmethod
    def match_topic(sub_topic: str, module_name: str) -> bool:
        """Determines if a UDM module name matches the specified subscription topic."""

        return re.fullmatch(sub_topic, module_name) is not None

    async def _fill_udm_topic(self, object_type: str, subscription_name: str):
        """Find the DNs of all UDM objects for an object_type."""

        # Note: the size of the HTTP response is limited in the UDM REST API container
        # by the UCR variable `directory/manager/web/sizelimit`
        # (default: 400.000 bytes).
        #
        # We should probably use pagination to fetch the objects, but the parameters
        # `page` and `limit` are marked as "Broken/Experimental" in the UDM REST API.
        #
        # For now, first request all objects without their properties,
        # then do one request per object-URL to fetch the whole object.

        urls = await self.udm.list_objects(object_type)
        for url in urls:
            logger.info("Grabbing object from: %r", url)
            await self._fill_object(url, object_type, subscription_name)

    async def _fill_object(self, url: str, object_type: str, subscription_name: str):
        """Retrieve the object for the given DN."""
        obj = await self.udm.get_object(url)

        message = Message(
            publisher_name=PublisherName.udm_pre_fill,
            ts=datetime.now(),
            realm="udm",
            topic=object_type,
            body=Body(old={}, new=obj),
        )
        logger.info("Sending to the consumer prefill queue from: %r", url)
        await self.mq.add_message_to_queue(subscription_name, message)

    async def add_to_failure_queue(self, data: dict) -> None:
        logger.info("Adding request to the prefill failures queue")
        message = SimpleMessage(
            publisher_name=PublisherName.udm_pre_fill,
            ts=datetime.now(),
            body=data,
        )
        await self.mq.add_message_to_failures_queue(message)
