# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import logging
import time
from typing import Optional

from univention.provisioning.models.message import MessageProcessingStatus, ProvisioningMessage
from univention.provisioning.models.subscription import FillQueueStatus

from .mq_port import MessageQueuePort
from .subscription_service import SubscriptionService
from .subscriptions_db_port import SubscriptionsDBPort

logger = logging.getLogger(__name__)


class MessageService:
    _subscription_prefill_done: dict[str, bool] = {}

    def __init__(self, subscriptions_db: SubscriptionsDBPort, mq: MessageQueuePort):
        self.mq = mq
        self.sub_service = SubscriptionService(subscriptions_db=subscriptions_db, mq=mq)

    async def get_next_message(
        self,
        subscription_name: str,
        timeout: float,
        pop: bool,
    ) -> Optional[ProvisioningMessage]:
        """Retrieve the first message from the subscription's stream.

        :param str subscription_name: Name of the subscription.
        :param bool pop: If the message should be deleted after request.
        :param float timeout: Max duration of the request before it expires.
        """
        timeout = max(timeout, 0.1)  # Timeout of 0 leads to internal server error
        t0 = time.perf_counter()
        if self._subscription_prefill_done.get(subscription_name, False):
            message = await self.mq.get_messages_from_main_queue(subscription_name, timeout, pop)
            queue = "main"
        else:
            if (
                await self.sub_service.check_subscription_queue_status(subscription_name, timeout)
                != FillQueueStatus.done
            ):  # take ~1.5ms
                logger.warning(
                    "Prefill status for subscription %r did not reach 'done' within the timeout period.",
                    subscription_name,
                )
                return None

            message = await self.mq.get_messages_from_prefill_queue(subscription_name, timeout, pop)
            queue = "prefill"
            if message is None:
                logger.info(
                    "All messages from the prefill subject for %r have been delivered. Will not check again.",
                    subscription_name,
                )
                self._subscription_prefill_done[subscription_name] = True
        logger.debug(
            "Retrieved%s message from %s queue for %r. (%.1f ms)",
            " a" if message else " no",
            queue,
            subscription_name,
            (time.perf_counter() - t0) * 1000,
        )
        return message

    async def update_message_status(self, subscription_name: str, seq_num: int, status: MessageProcessingStatus):
        if status == MessageProcessingStatus.ok:
            await self.mq.delete_message(subscription_name, seq_num)
