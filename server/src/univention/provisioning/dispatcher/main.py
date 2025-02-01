# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import asyncio
import logging
from importlib.metadata import version

from daemoniker import Daemonizer

from univention.provisioning.backends.message_queue import MessageAckManager
from univention.provisioning.utils.log import setup_logging

from .config import DispatcherSettings, dispatcher_settings
from .mq_adapter_nats import NatsMessageQueueAdapter
from .service import DispatcherService
from .subscriptions_adapter_nats import NatsSubscriptionsAdapter

logger = logging.getLogger(__name__)


async def main(settings: DispatcherSettings):
    with Daemonizer():
        async with NatsMessageQueueAdapter(settings) as mq, NatsSubscriptionsAdapter(settings) as subscriptions:
            service = DispatcherService(ack_manager=MessageAckManager(), mq=mq, subscriptions=subscriptions)
            await service.dispatch_events()


def run():
    settings = dispatcher_settings()
    setup_logging(settings.log_level)
    logger.info("Starting Dispatcher service version %r.", version("nubus-provisioning-dispatcher"))
    asyncio.run(main(settings))


if __name__ == "__main__":
    run()
