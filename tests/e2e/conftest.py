# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import uuid

import pytest

from univention.admin.rest.client import UDM
from tests.conftest import REALMS_TOPICS
from tests import set_test_env_vars

set_test_env_vars()

import shared.client  # noqa: E402


def pytest_addoption(parser):
    # Portal tests options
    parser.addoption(
        "--provisioning-api-base-url",
        default="http://localhost:7777/",
        help="Base URL of the UDM REST API",
    )
    parser.addoption(
        "--udm-rest-api-base-url",
        default="http://localhost:9979/udm/",
        help="Base URL of the UDM REST API",
    )
    parser.addoption(
        "--udm-admin-username", default="cn=admin", help="UDM admin login password"
    )
    parser.addoption(
        "--udm-admin-password", default="univention", help="UDM admin login password"
    )


@pytest.fixture(scope="session")
def provisioning_api_base_url(pytestconfig) -> str:
    return pytestconfig.option.provisioning_api_base_url.rstrip("/")


@pytest.fixture(scope="session")
def udm_admin_username(pytestconfig) -> str:
    return pytestconfig.option.udm_admin_username


@pytest.fixture(scope="session")
def udm_admin_password(pytestconfig) -> str:
    return pytestconfig.option.udm_admin_password


@pytest.fixture(scope="session")
def udm_rest_api_base_url(pytestconfig) -> str:
    """Base URL to reach the UDM Rest API."""
    return pytestconfig.option.udm_rest_api_base_url.rstrip("/") + "/"


@pytest.fixture
def udm(udm_rest_api_base_url, udm_admin_username, udm_admin_password) -> UDM:
    udm = UDM(udm_rest_api_base_url, udm_admin_username, udm_admin_password)
    # test the connection
    udm.get_ldap_base()
    return udm


@pytest.fixture
def settings(provisioning_api_base_url) -> shared.client.Settings:
    return shared.client.Settings(
        subscription_name="",
        provisioning_api_base_url=provisioning_api_base_url,
        # FIXME: Not yet used
        provisioning_api_username="",
        provisioning_api_password="",
        # FIXME: should be dropped
        realms_topics=[],
        request_prefill=False,
    )


@pytest.fixture
def provisioning_client(settings) -> shared.client.AsyncClient:
    return shared.client.AsyncClient(settings)


@pytest.fixture
async def simple_subscription(provisioning_client: shared.client.AsyncClient) -> str:
    subscriber_name = str(uuid.uuid4())
    await provisioning_client.create_subscription(
        name=subscriber_name,
        realms_topics=REALMS_TOPICS,
        password="",
        request_prefill=False,
    )

    yield subscriber_name

    await provisioning_client.cancel_subscription(subscriber_name)
