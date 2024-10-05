# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import asyncio

from daemoniker import Daemonizer

from univention.provisioning.utils.log import setup_logging

from .config import dispatcher_settings
from .port import DispatcherPort
from .service import DispatcherService


async def run_dispatcher():
    async with DispatcherPort.port_context() as port:
        service = DispatcherService(port)
        await service.dispatch_events()


def main():
    with Daemonizer():
        asyncio.run(run_dispatcher())


if __name__ == "__main__":
    dispatcher_settings = dispatcher_settings()
    setup_logging(dispatcher_settings.log_level)
    main()
