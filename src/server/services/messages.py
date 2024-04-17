# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH
import asyncio
import logging
from datetime import datetime
from typing import List, Optional

from .port import Port
from .subscriptions import SubscriptionService
from shared.models import (
    MessageProcessingStatusReport,
    MessageProcessingStatus,
    PublisherName,
    ProvisioningMessage,
    FillQueueStatus,
    Message,
    NewSubscription,
    PrefillMessage,
    PREFILL_SUBJECT_TEMPLATE,
    DISPATCHER_SUBJECT_TEMPLATE,
    DISPATCHER_STREAM,
    PREFILL_STREAM,
)


class MessageService:
    def __init__(self, port: Port):
        self._port = port
        self.logger = logging.getLogger(__name__)

    async def get_next_message(
        self,
        subscription_name: str,
        pop: bool,
        timeout: float = 5,
    ) -> Optional[ProvisioningMessage]:
        """Retrieve the first message from the subscription's stream.

        :param str subscription_name: Name of the subscription.
        :param bool pop: If the message should be deleted after request.
        :param float timeout: Max duration of the request before it expires.
        """

        response = await self.get_messages(subscription_name, timeout, count=1, pop=pop)
        return response[0] if response else None

    async def get_messages(
        self,
        subscription_name: str,
        timeout: float,
        count: int,
        pop: bool,
    ) -> List[ProvisioningMessage]:
        """Return messages from a given queue.

        :param str subscription_name: Name of the subscription.
        :param float timeout: Max duration of the request before it expires.
        :param int count: How many messages to return at most.
        :param bool pop: If messages should be deleted after request.
        """

        # TODO: Timeout of 0 leads to internal server error

        sub_service = SubscriptionService(self._port)
        queue_status = await sub_service.get_subscription_queue_status(
            subscription_name
        )

        messages = []
        prefill_stream = await self._port.stream_exists(
            PREFILL_SUBJECT_TEMPLATE.format(subscription=subscription_name)
        )

        if queue_status == FillQueueStatus.done and prefill_stream:
            messages = await self.get_messages_from_prefill_queue(
                subscription_name, timeout, count, pop
            )
        elif not prefill_stream:
            messages.extend(
                await self.get_messages_from_main_queue(
                    subscription_name, timeout, count, pop
                )
            )

        return messages

    async def get_messages_from_main_queue(
        self, subscription: str, timeout: float, count: int, pop: bool
    ) -> List[ProvisioningMessage]:
        main_subject = DISPATCHER_SUBJECT_TEMPLATE.format(subscription=subscription)
        self.logger.info(
            "Getting the messages for the '%s' from the main subject", main_subject
        )
        return await self._port.get_messages(
            subscription, main_subject, timeout, count, pop
        )

    async def get_messages_from_prefill_queue(
        self, subscription: str, timeout: float, count: int, pop: bool
    ) -> List[ProvisioningMessage]:
        prefill_subject = PREFILL_SUBJECT_TEMPLATE.format(subscription=subscription)
        self.logger.info(
            "Getting the messages for the '%s' from the prefill subject",
            prefill_subject,
        )
        messages = await self._port.get_messages(
            subscription, prefill_subject, timeout, count, pop
        )
        if len(messages) < count:
            self.logger.info("All messages from the prefill queue have been delivered")
            messages.extend(
                await self.get_messages_from_main_queue(
                    subscription, timeout, count - len(messages), pop
                )
            )
            if pop:
                await self._port.delete_stream(prefill_subject)
        return messages

    async def post_messages_status(
        self, subscription_name: str, reports: List[MessageProcessingStatusReport]
    ):
        tasks = [
            self.delete_message(subscription_name, report)
            for report in reports
            if report.status == MessageProcessingStatus.ok
        ]
        # Gather all tasks and run them concurrently
        await asyncio.gather(*tasks)

    async def delete_message(
        self, subscription_name: str, report: MessageProcessingStatusReport
    ):
        """Delete the messages from the subscriber's queue."""

        if report.publisher_name == PublisherName.udm_pre_fill:
            subject = PREFILL_SUBJECT_TEMPLATE.format(subscription=subscription_name)
        else:
            subject = DISPATCHER_SUBJECT_TEMPLATE.format(subscription=subscription_name)

        await self._port.delete_message(
            subscription_name, subject, report.message_seq_num
        )

    async def add_live_event(self, event: Message):
        await self._port.add_message(DISPATCHER_STREAM, DISPATCHER_STREAM, event)

    async def send_request_to_prefill(self, subscription: NewSubscription):
        self.logger.info("Sending the requests to prefill")
        message = PrefillMessage(
            publisher_name="consumer-registration",
            ts=datetime.now(),
            realms_topics=subscription.realms_topics,
            subscription_name=subscription.name,
        )
        await self._port.add_message(PREFILL_STREAM, PREFILL_STREAM, message)
