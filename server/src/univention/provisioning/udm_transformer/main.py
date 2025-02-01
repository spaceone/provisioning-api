# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import asyncio
import logging
from importlib.metadata import version

from daemoniker import Daemonizer

from univention.provisioning.backends.message_queue import MessageAckManager
from univention.provisioning.utils.log import setup_logging

from .cache_adapter_nats import CacheNats
from .config import UDMTransformerSettings, udm_transformer_settings
from .event_sender_adapter_messages_api import MessagesRestApiEventSender
from .ldap2udm_adapter import Ldap2UdmAdapter
from .subscriptions_adapter_nats import NatsSubscriptions
from .transformer_service import TransformerService

UDM_TRANSFORMER_CONSUMER_NAME = "udm-transformer"

logger = logging.getLogger(__name__)


async def main(settings: UDMTransformerSettings):
    with Daemonizer():
        async with (
            CacheNats(settings) as cache,
            MessagesRestApiEventSender(
                settings.provisioning_api_url, settings.events_username_udm, settings.events_password_udm
            ) as event_sender,
            NatsSubscriptions(settings) as subscriptions,
        ):
            await TransformerService(
                ack_manager=MessageAckManager(),
                cache=cache,
                event_sender=event_sender,
                ldap2udm=Ldap2UdmAdapter(settings),
                subscriptions=subscriptions,
                settings=settings,
            ).listen_for_ldap_events()


def run():
    # UDM adds an unwanted handler to the root logger
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    settings = udm_transformer_settings()
    setup_logging(settings.log_level)
    logger.info("Starting UDM Transformer version %r.", version("nubus-provisioning-udm-transformer"))
    asyncio.run(main(settings))


if __name__ == "__main__":
    run()
