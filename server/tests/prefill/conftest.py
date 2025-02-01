# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import pytest


@pytest.fixture(scope="session", autouse=True)
def anyio_backend():
    return "asyncio"
