# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import asyncio
import logging

from daemoniker import Daemonizer

from univention.provisioning.backends.message_queue import MessageAckManager
from univention.provisioning.utils.log import setup_logging

from .config import PrefillSettings, prefill_settings
from .mq_adapter_nats import NatsMessageQueue
from .prefill_service import PrefillService
from .udm_adapter import UDMAdapter
from .update_sub_q_status_adapter_rest_api import SubscriptionsRestApiAdapter

logger = logging.getLogger(__name__)


async def main(settings: PrefillSettings):
    with Daemonizer():
        async with (
            NatsMessageQueue(settings) as mq,
            UDMAdapter(settings.udm_url, settings.udm_username, settings.udm_password) as udm,
            SubscriptionsRestApiAdapter(
                settings.provisioning_api_url, settings.prefill_username, settings.prefill_password
            ) as update_sub_q_status,
        ):
            service = PrefillService(
                ack_manager=MessageAckManager(),
                mq=mq,
                udm=udm,
                update_sub_q_status=update_sub_q_status,
                settings=settings,
            )
            await service.handle_requests_to_prefill()


def run():
    settings = prefill_settings()
    setup_logging(settings.log_level)
    asyncio.run(main(settings))


if __name__ == "__main__":
    run()
