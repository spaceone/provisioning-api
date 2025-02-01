# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import json
import uuid
from typing import Any, AsyncGenerator, Callable, Coroutine, Optional

import msgpack
import nats
import pytest
import requests
from nats.aio.client import Client as NatsClient
from nats.js.errors import NotFoundError

from univention.admin.rest.client import UDM, HTTPError, NotFound
from univention.admin.rest.client import Object as UdmObject
from univention.provisioning.consumer.api import ProvisioningConsumerClient, ProvisioningConsumerClientSettings
from univention.provisioning.models.message import Body, PublisherName
from univention.provisioning.models.subscription import RealmTopic

from .e2e_settings import E2ETestSettings
from .mock_data import (
    DUMMY_REALMS_TOPICS,
    DUMMY_TOPIC,
    REALM,
    USERS_REALMS_TOPICS,
    USERS_TOPIC,
)


@pytest.fixture(scope="session", autouse=True)
def anyio_backend():
    return "asyncio"


def pytest_addoption(parser):
    # Portal tests options
    parser.addoption(
        "--environment",
        default="local",
        help=(
            "set the environment you are running the tests in."
            "accepted values are: 'local', 'dev-env', 'pipeline' and 'gaia'"
        ),
    )


@pytest.fixture(scope="session")
def test_settings(pytestconfig) -> E2ETestSettings:
    environment = pytestconfig.option.environment
    assert environment in (
        "local",
        "dev-env",
        "pipeline",
        "gaia",
        "gaia_nubus",
    ), "invalid value for --environment"

    with open("./e2e_settings.json") as f:
        json_settings = json.load(f)

    settings = E2ETestSettings(**json_settings["local"])
    if environment == "local":
        return settings

    return settings._replace(**json_settings[environment])


@pytest.fixture(scope="session")
def udm(test_settings: E2ETestSettings) -> UDM:
    udm = UDM(
        test_settings.udm_rest_api_base_url,
        test_settings.udm_rest_api_username,
        test_settings.udm_rest_api_password,
    )
    # test the connection
    udm.get_ldap_base()
    return udm


@pytest.fixture(scope="session")
def ldap_base(udm) -> str:
    return udm.get_ldap_base()


@pytest.fixture
def subscriber_name() -> str:
    return str(uuid.uuid4())


@pytest.fixture
def subscriber_password() -> str:
    return str(uuid.uuid4())


@pytest.fixture
def client_settings(
    test_settings: E2ETestSettings, subscriber_name, subscriber_password
) -> ProvisioningConsumerClientSettings:
    return ProvisioningConsumerClientSettings(
        provisioning_api_base_url=test_settings.provisioning_api_base_url,
        provisioning_api_username=subscriber_name,
        provisioning_api_password=subscriber_password,
        log_level="DEBUG",
    )


@pytest.fixture
def admin_client_settings(test_settings: E2ETestSettings) -> ProvisioningConsumerClientSettings:
    return ProvisioningConsumerClientSettings(
        provisioning_api_base_url=test_settings.provisioning_api_base_url,
        provisioning_api_username=test_settings.provisioning_admin_username,
        provisioning_api_password=test_settings.provisioning_admin_password,
        log_level="DEBUG",
    )


@pytest.fixture
async def provisioning_client(
    client_settings,
) -> AsyncGenerator[ProvisioningConsumerClient, Any]:
    async with ProvisioningConsumerClient(client_settings) as client:
        yield client


@pytest.fixture
async def provisioning_admin_client(
    admin_client_settings,
) -> AsyncGenerator[ProvisioningConsumerClient, Any]:
    async with ProvisioningConsumerClient(admin_client_settings) as client:
        yield client


@pytest.fixture
async def create_subscription(
    subscriber_name,
    subscriber_password,
    provisioning_admin_client: ProvisioningConsumerClient,
) -> AsyncGenerator[Callable[[list[RealmTopic]], Coroutine[Any, Any, str]], None]:
    async def _create_subscription(realms_topics: list[RealmTopic]) -> str:
        await provisioning_admin_client.create_subscription(
            name=subscriber_name,
            password=subscriber_password,
            realms_topics=realms_topics,
            request_prefill=False,
        )
        return subscriber_name

    yield _create_subscription

    await provisioning_admin_client.cancel_subscription(subscriber_name)


@pytest.fixture
async def real_subscription(create_subscription):
    return await create_subscription(USERS_REALMS_TOPICS)


@pytest.fixture
async def dummy_subscription(create_subscription):
    return await create_subscription(DUMMY_REALMS_TOPICS)


@pytest.fixture(scope="session")
def maildomain(udm):
    name = "ldif-producer.unittests"
    domains = udm.get("mail/domain")
    if maildomains := list(domains.search(f"name={name}")):
        maildomain = domains.get(maildomains[0].dn)
        print(f"Using existing mail domain {maildomain!r}.")
    else:
        maildomain = domains.new()
        maildomain.properties.update({"name": name})
        maildomain.save()
        print(f"Created new mail domain {maildomain!r}.")
    yield name
    maildomain.delete()
    print(f"Deleted mail domain {maildomain!r}.")


@pytest.fixture()
async def nats_connection(test_settings: E2ETestSettings) -> AsyncGenerator[NatsClient, Any]:
    nc = NatsClient()
    await nc.connect(
        servers=test_settings.nats_url,
        user=test_settings.nats_user,
        password=test_settings.nats_password,
        max_reconnect_attempts=1,
        connect_timeout=5,
    )
    yield nc

    await nc.close()


@pytest.fixture(scope="session")
def ldif_producer_stream_name() -> str:
    return "stream:ldif-producer"


@pytest.fixture
def get_and_delete_all_messages(
    nats_connection,
) -> Callable[[str], AsyncGenerator[dict[str, Any], Any]]:
    """Retrieve all messages of a streamm (iterator) and delete them."""

    async def _get_and_delete_all_messages(
        stream_name: str,
    ) -> AsyncGenerator[dict[str, Any], Any]:
        manager = nats.js.manager.JetStreamManager(nats_connection)
        info = await manager.stream_info(stream_name)
        for seq_id in range(info.state.first_seq, info.state.last_seq + 1):
            try:
                raw_msg = await manager.get_msg(stream_name, seq_id)
            except NotFoundError:
                print(seq_id, "NOT FOUND")
                continue
            msg = msgpack.unpackb(raw_msg.data)
            _print(msg["body"])
            yield msg["body"]
            await manager.delete_msg(stream_name, seq_id)

    def _print(body):
        old = body["old"]
        new = body["new"]
        old_dn = old["entryDN"][0].decode() if old else ""
        new_dn = new["entryDN"][0].decode() if new else ""
        msg_id = body["message_id"]
        req_id = body["request_id"]
        req_type = body["ldap_request_type"]
        print(f"msg: [{msg_id}][{req_id}] {req_type:<6} | old DN: {old_dn!r} | new DN: {new_dn!r}")
        if req_type in ("MODIFY", "MODRDN"):
            if not old:
                print("                        | !!! old=None")
                return
            diff: dict[str, tuple[Any, Any]] = {}
            for k, v in old.items():
                if k in {"entryCSN", "modifyTimestamp"}:
                    continue
                if k not in new:
                    diff[k] = (v, None)
                    continue
                if v != new[k]:
                    diff[k] = (v, new[k])
            for k, v in new.items():
                if k not in old:
                    diff[k] = (None, v)
            print(f"                        | diff: {diff!r}")

    return _get_and_delete_all_messages


@pytest.fixture
def purge_stream(nats_connection) -> Callable[[str], Coroutine[Any, Any, None]]:
    """Delete all messages from stream."""

    async def _purge_stream(stream_name: str):
        manager = nats.js.manager.JetStreamManager(nats_connection)
        await manager.purge_stream(stream_name)
        print(f"Purged NATS stream {stream_name!r}.")

    return _purge_stream


# @pytest.fixture(autouse=True)
# async def auto_purge_stream(ldif_producer_stream_name, purge_stream):
#     await purge_stream(ldif_producer_stream_name)


@pytest.fixture
def delete_udm_object(udm: UDM) -> Callable[[str, str], Coroutine[Any, Any, None]]:
    async def _delete_udm_object(udm_module: str, dn: str):
        mod = udm.get(udm_module)
        try:
            obj = mod.get(dn)
            obj.delete()
        except NotFound:
            print(f"Object doesn't exist: {udm_module!r}: {dn!r}.")
        except HTTPError as exc:
            print(f"HTTPError: {exc.response!r} | {exc.error_details!r} | {exc!s}")
        else:
            print(f"Deleted {udm_module!r}: {dn!r}.")

    return _delete_udm_object


@pytest.fixture
async def schedule_delete_udm_object(delete_udm_object) -> Callable[[str, str], None]:
    objects: list[tuple[str, str]] = []

    def _schedule_delete_udm_object(udm_module: str, dn: str):
        objects.append((udm_module, dn))

    yield _schedule_delete_udm_object

    for udm_module_, dn_ in objects:
        await delete_udm_object(udm_module_, dn_)


@pytest.fixture(scope="session")
def udm_module_exists(udm: UDM) -> Callable[[str], bool]:
    def _udm_module_exists(module_name: str) -> bool:
        return module_name in {m.name for m in udm.modules()}

    return _udm_module_exists


@pytest.fixture(scope="session")
def create_message_via_events_api() -> Callable[[E2ETestSettings], Body]:
    def _create_message_via_events_api(test_settings: E2ETestSettings) -> Body:
        body = {
            "old": {},
            "new": {str(uuid.uuid1()): str(uuid.uuid1()), "dn": "cn=foo,dc=bar", "objectType": "foo/bar"},
        }
        payload = {
            "publisher_name": PublisherName.consumer_client_test,
            "ts": "2024-02-07T09:01:33.835Z",
            "realm": REALM,
            "topic": DUMMY_TOPIC,
            "body": body,
        }
        response = requests.post(
            test_settings.messages_url,
            json=payload,
            auth=(test_settings.provisioning_events_username, test_settings.provisioning_events_password),
        )

        print(response.json())
        assert response.status_code == 202, "Failed to post message to queue"

        return Body.model_validate(body)

    return _create_message_via_events_api


@pytest.fixture(scope="session")
def create_udm_obj() -> Callable[[UDM, str, dict, Optional[str]], UdmObject]:
    def _create_udm_obj(udm: UDM, object_type: str, properties: dict, position: Optional[str] = None) -> UdmObject:
        objs = udm.get(object_type)
        assert objs
        obj = objs.new(position=position)
        obj.properties.update(properties)
        obj.save()
        return obj

    return _create_udm_obj


@pytest.fixture(scope="session")
def create_user_via_udm_rest_api(create_udm_obj) -> Callable[[UDM, Optional[dict]], UdmObject]:
    def _create_user_via_udm_rest_api(udm: UDM, extended_attributes: Optional[dict] = None) -> UdmObject:
        base_properties = {
            "username": str(uuid.uuid1()),
            "firstname": "John",
            "lastname": "Doe",
            "password": "password",
            "pwdChangeNextLogin": True,
        }
        properties = {**base_properties, **(extended_attributes or {})}
        return create_udm_obj(udm, USERS_TOPIC, properties)

    return _create_user_via_udm_rest_api


@pytest.fixture(scope="session")
def create_extended_attribute_via_udm_rest_api(create_udm_obj) -> Callable[[UDM], UdmObject]:
    def _create_extended_attribute_via_udm_rest_api(udm: UDM):
        properties = {
            "name": "UniventionPasswordSelfServiceEmail",
            "CLIName": "PasswordRecoveryEmail",
            "module": ["users/user"],
            "syntax": "emailAddress",
            "default": "",
            "ldapMapping": "univentionPasswordSelfServiceEmail",
            "objectClass": "univentionPasswordSelfService",
            "shortDescription": "Password recovery e-mail address",
            "tabAdvanced": False,
            "tabName": "Password recovery",
            "multivalue": False,
            "valueRequired": False,
            "mayChange": True,
            "doNotSearch": False,
            "deleteObjectClass": False,
            "overwriteTab": False,
            "fullWidth": True,
        }
        position = f"cn=custom attributes,cn=univention,{udm.get_ldap_base()}"
        return create_udm_obj(udm, "settings/extended_attribute", properties, position)

    return _create_extended_attribute_via_udm_rest_api
