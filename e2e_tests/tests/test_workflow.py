# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import asyncio
import uuid

import ldap3
import pytest
import requests

from univention.provisioning.models.constants import PublisherName
from univention.provisioning.models.subscription import FillQueueStatus

from .e2e_settings import E2ETestSettings

REALM = "udm"
TOPIC = "groups/group"
TOPIC_2 = "container/dc"
PASSWORD = "password"


@pytest.fixture(scope="session")
def ldap_connection(test_settings: E2ETestSettings):
    server = ldap3.Server(test_settings.ldap_server_uri)
    connection = ldap3.Connection(server, test_settings.ldap_bind_dn, test_settings.ldap_bind_password)
    assert connection.bind()
    return connection
    # connection.unbind()


@pytest.fixture(scope="function")
def subscription_name(test_settings: E2ETestSettings):
    name = str(uuid.uuid4())

    response = requests.post(
        test_settings.subscriptions_url,
        json={
            "name": name,
            "realms_topics": [{"realm": REALM, "topic": TOPIC}],
            "request_prefill": False,
            "password": PASSWORD,
        },
        auth=(test_settings.provisioning_admin_username, test_settings.provisioning_admin_password),
    )
    assert response.status_code == 201, "creating client subscription failed"

    yield name

    response = requests.delete(
        f"{test_settings.subscriptions_url}/{name}",
        auth=(test_settings.provisioning_admin_username, test_settings.provisioning_admin_password),
    )
    assert response.status_code == 200, "deleting client subscription failed"


@pytest.fixture(scope="function")
def ldap_group(ldap_connection, subscription_name, test_settings, request):
    dn = f"cn=test_group1235,cn=groups,{test_settings.ldap_base}"
    result = ldap_connection.add(
        dn=dn,
        object_class=("posixGroup", "univentionObject", "univentionGroup"),
        attributes={"univentionObjectType": "groups/group", "gidNumber": 1},
    )
    assert result, "No ldap change triggered, possible reasons: invalid request or test-data collision"

    yield ldap_connection, dn

    ldap_connection.delete(dn)


async def test_workflow(test_settings, ldap_group, subscription_name):
    ldap_connection, dn = ldap_group

    # Test object was created
    response = requests.get(
        f"{test_settings.subscriptions_messages_url(subscription_name)}/next?pop=true",
        auth=(subscription_name, PASSWORD),
    )
    assert response.status_code == 200

    message = response.json()

    assert message["realm"] == REALM
    assert message["topic"] == TOPIC
    assert message["publisher_name"] == PublisherName.udm_listener
    assert isinstance(message["body"]["old"], dict)
    assert not message["body"]["old"]
    assert message["body"]["new"]["dn"] == dn

    # Test modifying object
    new_description = "New description"
    changes = {"description": [(ldap3.MODIFY_REPLACE, [new_description])]}
    ldap_connection.modify(dn, changes)

    response = requests.get(
        f"{test_settings.subscriptions_messages_url(subscription_name)}/next?pop=true",
        auth=(subscription_name, PASSWORD),
    )
    assert response.status_code == 200

    message = response.json()

    assert message["realm"] == REALM
    assert message["topic"] == TOPIC
    assert message["publisher_name"] == PublisherName.udm_listener
    assert message["body"]["old"]["dn"] == dn
    assert message["body"]["new"]["properties"]["description"] == new_description

    # Test deleting object
    ldap_connection.delete(dn)

    response = requests.get(
        f"{test_settings.subscriptions_messages_url(subscription_name)}/next?pop=true",
        auth=(subscription_name, PASSWORD),
    )
    assert response.status_code == 200

    message = response.json()

    assert message["realm"] == REALM
    assert message["topic"] == TOPIC
    assert message["publisher_name"] == PublisherName.udm_listener
    assert isinstance(message["body"]["new"], dict)
    assert not message["body"]["new"]
    assert message["body"]["old"]["dn"] == dn


async def test_prefill_with_multiple_topics(test_settings):
    name = str(uuid.uuid4())

    response = requests.post(
        test_settings.subscriptions_url,
        json={
            "name": name,
            "realms_topics": [{"realm": REALM, "topic": TOPIC}, {"realm": REALM, "topic": TOPIC_2}],
            "request_prefill": True,
            "password": PASSWORD,
        },
        auth=(test_settings.provisioning_admin_username, test_settings.provisioning_admin_password),
    )
    assert response.status_code == 201

    # ensure that the pre-fill process is finished successfully
    while True:
        response = requests.get(f"{test_settings.subscriptions_url}/{name}", auth=(name, PASSWORD))
        assert response.status_code == 200
        prefill_queue_status = response.json()["prefill_queue_status"]
        if prefill_queue_status == FillQueueStatus.failed:
            assert False
        elif prefill_queue_status == FillQueueStatus.done:
            break
        await asyncio.sleep(1)

    topics = []
    while True:
        response = requests.get(
            f"{test_settings.subscriptions_messages_url(name)}/next?pop=true", auth=(name, PASSWORD)
        )
        assert response.status_code == 200

        message = response.json()
        if not message:
            break
        topics.append(message["topic"])

        assert message["realm"] == REALM
        assert message["publisher_name"] == PublisherName.udm_pre_fill

    expected_topics_order = [TOPIC, TOPIC_2]
    received_topics_order = []
    for topic in topics:
        if topic not in received_topics_order:
            received_topics_order.append(topic)

    assert received_topics_order == expected_topics_order
