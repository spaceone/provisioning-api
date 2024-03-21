# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import uuid

import pytest

from univention.admin.rest.client import UDM
from tests.conftest import REALMS_TOPICS

import shared.client  # noqa: E402


def pytest_addoption(parser):
    # Portal tests options
    parser.addoption(
        "--provisioning-api-base-url",
        default="http://localhost:7777/",
        help="Base URL of the UDM REST API",
    )
    parser.addoption(
        "--provisioning-admin-username",
        default="admin",
        help="UDM admin login password",
    )
    parser.addoption(
        "--provisioning-admin-password",
        default="provisioning",
        help="UDM admin login password",
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
    parser.addoption("--ldap-server-uri", default="ldap://localhost:389")
    parser.addoption(
        "--ldap-host-dn", default="cn=admin,dc=univention-organization,dc=intranet"
    )
    parser.addoption("--ldap-password", default="univention")


@pytest.fixture(scope="session")
def provisioning_api_base_url(pytestconfig) -> str:
    return pytestconfig.option.provisioning_api_base_url.rstrip("/")


@pytest.fixture(scope="session")
def udm_admin_username(pytestconfig) -> str:
    return pytestconfig.option.udm_admin_username


@pytest.fixture(scope="session")
def udm_admin_password(pytestconfig) -> str:
    return pytestconfig.option.udm_admin_password


@pytest.fixture
def udm(pytestconfig) -> UDM:
    udm = UDM(
        pytestconfig.option.udm_rest_api_base_url.rstrip("/") + "/",
        pytestconfig.option.udm_admin_username,
        pytestconfig.option.udm_admin_password,
    )
    # test the connection
    udm.get_ldap_base()
    return udm


@pytest.fixture
def subscription_name() -> str:
    return str(uuid.uuid4())


@pytest.fixture
def subscription_password() -> str:
    return str(uuid.uuid4())


@pytest.fixture
def settings(
    provisioning_api_base_url, subscription_name, subscription_password
) -> shared.client.Settings:
    return shared.client.Settings(
        provisioning_api_base_url=provisioning_api_base_url,
        provisioning_api_username=subscription_name,
        provisioning_api_password=subscription_password,
    )


@pytest.fixture
def admin_settings(pytestconfig, provisioning_api_base_url) -> shared.client.Settings:
    return shared.client.Settings(
        provisioning_api_base_url=provisioning_api_base_url,
        provisioning_api_username=pytestconfig.option.provisioning_admin_username,
        provisioning_api_password=pytestconfig.option.provisioning_admin_password,
    )


@pytest.fixture
async def provisioning_client(settings) -> shared.client.AsyncClient:
    async with shared.client.AsyncClient(settings) as client:
        yield client


@pytest.fixture
async def provisioning_admin_client(admin_settings) -> shared.client.AsyncClient:
    async with shared.client.AsyncClient(admin_settings) as client:
        yield client


@pytest.fixture
async def simple_subscription(
    provisioning_admin_client: shared.client.AsyncClient,
    provisioning_client: shared.client.AsyncClient,
    subscription_name,
    subscription_password,
) -> str:
    await provisioning_admin_client.create_subscription(
        name=subscription_name,
        realms_topics=REALMS_TOPICS,
        password=subscription_password,
        request_prefill=False,
    )

    yield subscription_name

    await provisioning_client.cancel_subscription(subscription_name)
