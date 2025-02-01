# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import asyncio
import time
import uuid
from typing import Any, Callable

import nats
import pytest

from univention.admin.rest.client import UDM

NUM_TEST_USERS = 10


class MissingUdmExtension(Exception): ...


@pytest.fixture(scope="session")
def opendesk_extensions(udm) -> bool:
    # Check if opendesk extensions need to be disabled.
    return bool(next(udm.get("settings/extended_attribute").search("cn=opendeskFileshareEnabledUser"), None))


@pytest.fixture(scope="session")
def user_properties_factory(maildomain, opendesk_extensions) -> Callable[[str], dict[str, str | bool]]:
    def _user_properties(username: str) -> dict[str, str | bool]:
        properties = {
            "username": username,
            "firstname": "test_event_generation",
            "lastname": username,
            "password": "univention",
            "mailPrimaryAddress": f"{username}@{maildomain}",
        }
        if opendesk_extensions:
            properties.update(
                {
                    "isOxUser": False,
                    "opendeskFileshareEnabled": False,
                    "opendeskFileshareAdmin": False,
                    "opendeskProjectmanagementEnabled": False,
                    "opendeskProjectmanagementAdmin": False,
                    "opendeskKnowledgemanagementEnabled": False,
                    "opendeskLivecollaborationEnabled": False,
                }
            )
        return properties

    return _user_properties


async def get_messages(
    get_and_delete_all_messages, ldif_producer_stream_name
) -> tuple[list[dict[str, Any]], list[tuple[str, str, str]]]:
    messages_found = []
    messages_found_short: list[tuple[str, str, str]] = []
    async for msg in get_and_delete_all_messages(ldif_producer_stream_name):
        messages_found.append(msg)
        dn_old = msg["old"]["entryDN"][0].decode() if msg["old"] else ""
        dn_new = msg["new"]["entryDN"][0].decode() if msg["new"] else ""
        uni_obj_type = msg["old"]["univentionObjectType"][0] if msg["old"] else msg["new"]["univentionObjectType"][0]
        messages_found_short.append((msg["ldap_request_type"], uni_obj_type.decode(), dn_old or dn_new))
    return messages_found, messages_found_short


def create_user(udm: UDM, props):
    users = udm.get("users/user")
    assert users
    user = users.new()
    user.properties.update(props)

    try:
        user.save()
        return user
    except Exception as exc:
        return exc


@pytest.mark.skip("only works with the ldif-producer")
@pytest.mark.timeout(10)
@pytest.mark.asyncio
async def test_create_delete_maildomain(get_and_delete_all_messages, ldap_base, ldif_producer_stream_name, maildomain):
    assert maildomain
    name = "ldif-producer.unittests"
    async for msg in get_and_delete_all_messages(ldif_producer_stream_name):
        assert msg["ldap_request_type"] == "ADD"
        assert not msg["old"]
        assert msg["new"]["univentionObjectType"] == [b"mail/domain"]
        assert msg["new"]["cn"] == [name.encode()]
        assert msg["new"]["entryDN"][0].decode() == f"cn={name},cn=domain,cn=mail,{ldap_base}"


@pytest.mark.skip("only works with the ldif-producer")
@pytest.mark.timeout(30)
@pytest.mark.asyncio
async def test_create_user(
    get_and_delete_all_messages,
    ldif_producer_stream_name,
    schedule_delete_udm_object,
    ldap_base,
    nats_connection,
    udm,
    user_properties_factory,
):
    user_mod = udm.get("users/user")

    usernames = [str(uuid.uuid4()) for _ in range(NUM_TEST_USERS)]
    user_properties = [user_properties_factory(username) for username in usernames]

    messages_expected: list[tuple[str, str, str]] = []
    for username in usernames:
        messages_expected.extend(
            [
                ("ADD", "users/user", f"uid={username},cn=users,{ldap_base}"),
                ("MODIFY", "groups/group", f"cn=Domain Users,cn=groups,{ldap_base}"),
            ]
        )
        schedule_delete_udm_object("users/user", f"uid={username},cn=users,{ldap_base}")

    for i, props in enumerate(user_properties):
        user = user_mod.new()
        user.properties.update(props)
        user.save()
        print(f"Created user {i:<3} {user.dn!r}.")

    manager = nats.js.manager.JetStreamManager(nats_connection)
    timeout = time.time() + 60
    while time.time() < timeout:
        info = await manager.stream_info(ldif_producer_stream_name)
        if info.state.messages >= len(messages_expected):
            break
        await asyncio.sleep(1)

    messages_found, messages_found_short = await get_messages(get_and_delete_all_messages, ldif_producer_stream_name)
    assert len(messages_expected) == len(messages_found_short)
    assert set(messages_expected) == set(messages_found_short)
    assert messages_expected == messages_found_short


@pytest.mark.skip("only works with the ldif-producer")
@pytest.mark.timeout(30)
@pytest.mark.asyncio
async def test_create_delete_user(
    get_and_delete_all_messages,
    ldap_base,
    ldif_producer_stream_name,
    udm,
    user_properties_factory,
    nats_connection,
):
    usernames = [str(uuid.uuid4()) for _ in range(NUM_TEST_USERS)]
    user_properties = [user_properties_factory(username) for username in usernames]

    messages_expected: list[tuple[str, str, str]] = []
    for username in usernames:
        messages_expected.extend(
            [
                ("ADD", "users/user", f"uid={username},cn=users,{ldap_base}"),
                ("MODIFY", "groups/group", f"cn=Domain Users,cn=groups,{ldap_base}"),
                ("MODIFY", "groups/group", f"cn=Domain Guests,cn=groups,{ldap_base}"),
                ("DELETE", "users/user", f"uid={username},cn=users,{ldap_base}"),
                ("MODIFY", "groups/group", f"cn=Domain Users,cn=groups,{ldap_base}"),
                ("MODIFY", "groups/group", f"cn=Domain Guests,cn=groups,{ldap_base}"),
            ]
        )

    user_mod = udm.get("users/user")
    group_mod = udm.get("groups/group")
    guest_group_dn = list(group_mod.search("name=Domain Guests"))[0].dn
    for i, props in enumerate(user_properties):
        user = user_mod.new()
        user.properties.update(props)
        user.save()
        print(f"Created user {i:>3}/{len(usernames)} {user.dn!r}.")
        user.properties["groups"].append(guest_group_dn)
        user.save()
        print(f"Added user {i:>3}/{len(usernames)} to group 'Domain Guests'.")
        user.delete()
        print(f"Deleted user {i:>3}/{len(usernames)} {user.dn!r}.")

    # wait for messages to arrive
    manager = nats.js.manager.JetStreamManager(nats_connection)
    timeout = time.time() + 60
    while time.time() < timeout:
        info = await manager.stream_info(ldif_producer_stream_name)
        if info.state.messages >= len(messages_expected):
            break
        await asyncio.sleep(1)

    messages_found, messages_found_short = await get_messages(get_and_delete_all_messages, ldif_producer_stream_name)
    assert len(messages_expected) == len(messages_found_short)
    assert set(messages_expected) == set(messages_found_short)
    assert messages_expected == messages_found_short

    # verify message content
    dn = b""
    username = b""
    attrs_all = (
        "createTimestamp",
        "entryCSN",
        "entryUUID",
        "modifyTimestamp",
        "objectClass",
    )
    attrs_groups = ("cn", "gidNumber", "sambaSID")
    attrs_users = (
        "displayName",
        "gidNumber",
        "givenName",
        "krb5Key",
        "mailPrimaryAddress",
        "sn",
        "uid",
        "uidNumber",
        "userPassword",
    )
    for index, msg in enumerate(messages_found):
        msg_attrs = set(msg["old"].keys() if msg["old"] else []).union(set(msg["new"].keys() if msg["new"] else []))
        assert all(key in msg_attrs for key in attrs_all)
        if index % 6 == 0:
            assert msg["ldap_request_type"] == "ADD"
            assert msg["new"] and not msg["old"]
            assert msg["new"]["univentionObjectType"] == [b"users/user"]
            assert msg["new"]["gidNumber"] == [b"5001"]
            assert all(key in msg_attrs for key in attrs_users)
            dn = msg["new"]["entryDN"][0]
            username = msg["new"]["uid"][0]
        elif index % 6 == 1:
            assert msg["ldap_request_type"] == "MODIFY"
            assert msg["new"] and msg["old"]
            assert msg["old"]["entryDN"][0].decode() == f"cn=Domain Users,cn=groups,{ldap_base}"
            assert msg["new"]["univentionObjectType"] == [b"groups/group"]
            assert msg["new"]["gidNumber"] == [b"5001"]
            assert all(key in msg_attrs for key in attrs_groups)
            assert dn not in msg["old"].get("uniqueMember", [])
            assert username not in msg["old"].get("memberUid", [])
            assert dn in msg["new"].get("uniqueMember", [])
            assert username in msg["new"].get("memberUid", [])
        elif index % 6 == 2:
            assert msg["ldap_request_type"] == "MODIFY"
            assert msg["new"] and msg["old"]
            assert msg["new"]["entryDN"][0].decode() == f"cn=Domain Guests,cn=groups,{ldap_base}"
            assert msg["new"]["univentionObjectType"] == [b"groups/group"]
            assert msg["new"]["gidNumber"] == [b"5002"]
            assert dn not in msg["old"].get("uniqueMember", [])
            assert username not in msg["old"].get("memberUid", [])
            assert dn in msg["new"].get("uniqueMember", [])
            assert username in msg["new"].get("memberUid", [])
        elif index % 6 == 3:
            assert msg["ldap_request_type"] == "DELETE"
            assert not msg["new"] and msg["old"]
            assert msg["old"]["univentionObjectType"] == [b"users/user"]
            assert msg["old"]["gidNumber"] == [b"5001"]
            assert msg["old"]["entryDN"][0] == dn
            assert msg["old"]["uid"][0] == username
            assert all(key in msg_attrs for key in attrs_users)
        elif index % 6 == 4:
            assert msg["ldap_request_type"] == "MODIFY"
            assert msg["new"] and msg["old"]
            assert msg["old"]["entryDN"][0].decode() == f"cn=Domain Users,cn=groups,{ldap_base}"
            assert all(key in msg_attrs for key in attrs_groups)
            # Fixme: DN missing in old, because of Refint overlay
            # assert dn in msg["old"].get("uniqueMember", [])
            assert msg["old"]["modifiersName"] == [b"cn=Referential Integrity Overlay"]  # <-- Refint
            assert username in msg["old"].get("memberUid", [])
            assert dn not in msg["new"].get("uniqueMember", [])
            assert username not in msg["new"].get("memberUid", [])
        elif index % 6 == 5:
            assert msg["ldap_request_type"] == "MODIFY"
            assert msg["new"] and msg["old"]
            assert msg["new"]["entryDN"][0].decode() == f"cn=Domain Guests,cn=groups,{ldap_base}"
            # Fixme: DN missing in old, because of Refint overlay
            # assert dn in msg["old"].get("uniqueMember", [])
            assert msg["old"]["modifiersName"] == [b"cn=Referential Integrity Overlay"]  # <-- Refint
            assert username in msg["old"].get("memberUid", [])
            assert dn not in msg["new"].get("uniqueMember", [])
            assert username not in msg["new"].get("memberUid", [])


@pytest.mark.skip("only works with the ldif-producer")
@pytest.mark.timeout(10)
@pytest.mark.asyncio
async def test_rename_user(
    get_and_delete_all_messages,
    ldif_producer_stream_name,
    schedule_delete_udm_object,
    ldap_base,
    nats_connection,
    udm,
    user_properties_factory,
):
    user_name_old = str(uuid.uuid4())
    user_name_new = str(uuid.uuid4())

    user_properties = user_properties_factory(user_name_old)

    messages_expected: list[tuple[str, str, str]] = [
        ("ADD", "users/user", f"uid={user_name_old},cn=users,{ldap_base}"),
        ("MODIFY", "groups/group", f"cn=Domain Users,cn=groups,{ldap_base}"),
        ("MODRDN", "users/user", f"uid={user_name_old},cn=users,{ldap_base}"),
        ("MODIFY", "users/user", f"uid={user_name_new},cn=users,{ldap_base}"),
        # filtered out: MODIFY change uniqueMember (refint)
        ("MODIFY", "groups/group", f"cn=Domain Users,cn=groups,{ldap_base}"),
    ]

    schedule_delete_udm_object("users/user", f"uid={user_name_old},cn=users,{ldap_base}")
    schedule_delete_udm_object("users/user", f"uid={user_name_new},cn=users,{ldap_base}")

    user_mod = udm.get("users/user")
    user = user_mod.new()
    user.properties.update(user_properties)
    user.save()
    dn_old = user.dn
    print(f"Created user {dn_old!r}.")

    user.reload()
    assert user.dn == dn_old
    user.properties["username"] = user_name_new
    user.save()
    dn_new = user.dn
    print(f"Renamed user from {user_name_old!r} to {user_name_new!r}. New DN: {dn_new!r}")
    assert dn_old != dn_new

    manager = nats.js.manager.JetStreamManager(nats_connection)
    timeout = time.time() + 60
    while time.time() < timeout:
        info = await manager.stream_info(ldif_producer_stream_name)
        if info.state.messages >= len(messages_expected):
            break
        await asyncio.sleep(1)

    messages_found, messages_found_short = await get_messages(get_and_delete_all_messages, ldif_producer_stream_name)
    assert len(messages_expected) == len(messages_found_short)
    assert set(messages_expected) == set(messages_found_short)
    assert messages_expected == messages_found_short

    # verify message content
    for index, msg in enumerate(messages_found):
        if index == 0:  # create user (UDM)
            assert msg["new"] and not msg["old"]
            assert msg["new"]["uid"][0].decode() == user_name_old
            assert msg["new"]["entryDN"][0].decode() == dn_old
        elif index == 1:  # add user to group (UDM)
            assert msg["new"] and msg["old"]
            assert dn_old.encode() not in msg["old"].get("uniqueMember", [])
            assert user_name_old.encode() not in msg["old"].get("memberUid", [])
            assert dn_old.encode() in msg["new"].get("uniqueMember", [])
            assert user_name_old.encode() in msg["new"].get("memberUid", [])
        elif index == 2:  # rename user (UDM)
            assert msg["new"] and msg["old"]
            assert msg["old"]["uid"][0].decode() == user_name_old
            assert msg["old"]["entryDN"][0].decode() == dn_old
            assert msg["new"]["uid"][0].decode() == user_name_new
            assert msg["new"]["entryDN"][0].decode() == dn_new
        elif index == 3:  # ERROR: old=None (?)
            # Fixme: msg["old"] is None, should be a dict with data in MODIFY
            # assert msg["new"] and msg["old"]
            assert msg["new"]["uid"][0].decode() == user_name_new
            assert msg["new"]["entryDN"][0].decode() == dn_new
        elif index == 4:  # change memberUid (UDM)
            assert msg["new"] and msg["old"]
            assert dn_new.encode() in msg["old"].get("uniqueMember", [])  # refint already changed it
            assert user_name_old.encode() in msg["old"].get("memberUid", [])
            assert dn_new.encode() in msg["new"].get("uniqueMember", [])
            assert user_name_new.encode() in msg["new"].get("memberUid", [])
        else:
            raise AssertionError(f"Unexpected message. {index=} {msg=}")


@pytest.mark.skip("only works with the ldif-producer")
@pytest.mark.timeout(10)
async def test_create_modify_delete_group(
    get_and_delete_all_messages,
    ldap_base,
    ldif_producer_stream_name,
    maildomain,
    udm,
    user_properties_factory,
    nats_connection,
):
    group_name = str(uuid.uuid4())
    group_props = {
        "name": group_name,
        "description": f"The group '{group_name}'.",
        "mailAddress": f"{group_name}@{maildomain}",
    }
    group_dn = f"cn={group_name},cn=groups,{ldap_base}"
    user_name = str(uuid.uuid4())
    user_properties = user_properties_factory(user_name)

    user_dn = f"uid={user_name},cn=users,{ldap_base}"
    messages_expected: list[tuple[str, str, str]] = [
        ("ADD", "groups/group", group_dn),  # create group
        ("ADD", "users/user", user_dn),  # create user
        ("MODIFY", "groups/group", group_dn),  # add user to group (users primaryGroup)
        ("MODIFY", "groups/group", group_dn),  # change description
        ("DELETE", "users/user", user_dn),  # delete user
        ("MODIFY", "groups/group", group_dn),  # remove user from group (user deleted)
        ("DELETE", "groups/group", group_dn),  # delete group
    ]
    group_mod = udm.get("groups/group")
    user_mod = udm.get("users/user")

    group = group_mod.new()
    group.properties.update(group_props)
    group.save()
    print(f"Created group {group.dn!r}.")
    assert group.dn == group_dn

    user_properties["primaryGroup"] = group.dn

    user = user_mod.new()
    user.properties.update(user_properties)
    user.save()
    print(f"Created user {user.dn!r}.")
    assert user.dn == user_dn

    group.reload()
    group.properties["description"] = new_description = f"New description for group {group_name!r}."
    group.save()
    print(f"Modified group {group.dn!r} (changed description).")

    user.reload()
    user.properties["groups"].append(group_dn)
    user.save()
    print(f"Added user {user.dn!r} to group {group.dn}.")

    user.delete()
    print(f"Deleted user {user.dn!r}.")

    group.reload()
    group.delete()
    print(f"Deleted group {group.dn!r}.")

    # wait for messages to arrive
    manager = nats.js.manager.JetStreamManager(nats_connection)
    timeout = time.time() + 60
    while time.time() < timeout:
        info = await manager.stream_info(ldif_producer_stream_name)
        if info.state.messages >= len(messages_expected):
            break
        await asyncio.sleep(1)

    messages_found, messages_found_short = await get_messages(get_and_delete_all_messages, ldif_producer_stream_name)
    assert len(messages_expected) == len(messages_found_short)
    assert set(messages_expected) == set(messages_found_short)
    assert messages_expected == messages_found_short

    # verify message content
    msg = messages_found[0]  # create group
    assert msg["new"] and not msg["old"]
    msg = messages_found[1]  # create user
    assert msg["new"] and not msg["old"]
    msg = messages_found[2]  # add user to group (users primaryGroup)
    assert msg["new"] and msg["old"]
    assert user_dn.encode() not in msg["old"].get("uniqueMember", [])
    assert user_name.encode() not in msg["old"].get("memberUid", [])
    assert user_dn.encode() in msg["new"].get("uniqueMember", [])
    assert user_name.encode() in msg["new"].get("memberUid", [])
    msg = messages_found[3]  # change description
    # Fixme: msg["old"] is None, should be a dict with data in MODIFY
    # assert msg["new"] and msg["old"]
    # assert group_props["description"].encode() == msg["old"]["description"][0]
    assert new_description.encode() == msg["new"]["description"][0]
    msg = messages_found[4]  # delete user
    assert not msg["new"] and msg["old"]
    msg = messages_found[5]  # remove user from group (because user was deleted)
    # Fixme: msg["old"] is None, should be a dict with data in MODIFY
    # assert not msg["new"] and msg["old"]
    # assert user_dn.encode() in msg["old"].get("uniqueMember", [])
    assert user_dn.encode() not in msg["new"].get("uniqueMember", [])
    msg = messages_found[6]  # delete group
    assert not msg["new"] and msg["old"]


@pytest.mark.skip("only works with the ldif-producer")
@pytest.mark.timeout(10)
@pytest.mark.asyncio
async def test_rename_group(
    maildomain,
    get_and_delete_all_messages,
    ldif_producer_stream_name,
    schedule_delete_udm_object,
    ldap_base,
    nats_connection,
    udm,
):
    group_name_old = str(uuid.uuid4())
    group_dn_old = f"cn={group_name_old},cn=groups,{ldap_base}"
    group_name_new = str(uuid.uuid4())
    group_dn_new = f"cn={group_name_new},cn=groups,{ldap_base}"
    group_props = {
        "name": group_name_old,
        "description": f"The group '{group_name_old}'.",
        "mailAddress": f"{group_name_old}@{maildomain}",
    }
    messages_expected: list[tuple[str, str, str]] = [
        ("ADD", "groups/group", group_dn_old),  # create group (UDM)
        ("MODRDN", "groups/group", group_dn_new),  # rename group (UDM)
        ("MODIFY", "groups/group", group_dn_new),  # Fixme: old=None (?)
    ]

    schedule_delete_udm_object("groups/group", group_dn_old)
    schedule_delete_udm_object("groups/group", group_dn_new)

    group_mod = udm.get("groups/group")
    group = group_mod.new()
    group.properties.update(group_props)
    group.save()
    dn_old = group.dn
    print(f"Created group {dn_old!r}.")

    group.reload()
    assert group.dn == dn_old
    group.properties["name"] = group_name_new
    group.save()
    dn_new = group.dn
    print(f"Renamed group from {group_name_old!r} to {group_name_new!r}. New DN: {dn_new!r}")
    assert dn_old != dn_new

    manager = nats.js.manager.JetStreamManager(nats_connection)
    timeout = time.time() + 60
    while time.time() < timeout:
        info = await manager.stream_info(ldif_producer_stream_name)
        if info.state.messages >= len(messages_expected):
            break
        await asyncio.sleep(1)

    messages_found, messages_found_short = await get_messages(get_and_delete_all_messages, ldif_producer_stream_name)
    assert len(messages_expected) == len(messages_found_short)
    assert set(messages_expected) == set(messages_found_short)
    assert messages_expected == messages_found_short

    # verify message content
    for index, msg in enumerate(messages_found):
        if index == 0:  # create group (UDM)
            assert msg["new"] and not msg["old"]
            assert msg["new"]["cn"][0].decode() == group_name_old
            assert msg["new"]["entryDN"][0].decode() == group_dn_old
        elif index == 1:  # rename group (UDM)
            assert msg["new"] and not msg["old"]
            assert msg["new"]["cn"][0].decode() == group_name_new
            assert msg["new"]["entryDN"][0].decode() == group_dn_new
        elif index == 2:  # ERROR: old=None (?)
            # Fixme: msg["old"] is None, should be a dict with data in MODIFY
            # assert msg["new"] and msg["old"]
            assert msg["new"]["cn"][0].decode() == group_name_new
            assert msg["new"]["entryDN"][0].decode() == group_dn_new
        else:
            raise AssertionError(f"Unexpected message. {index=} {msg=}")


# Only works after stack-data
@pytest.mark.xfail(raises=MissingUdmExtension, reason="Missing OX UDM Extension")
@pytest.mark.timeout(10)
def test_create_accessprofile(udm: UDM, udm_module_exists):
    if not udm_module_exists("oxmail/accessprofile"):
        raise MissingUdmExtension

    accessprofile = udm.get("oxmail/accessprofile")
    assert accessprofile
    profile = accessprofile.new()
    name = str(uuid.uuid1())

    profile.properties.update({"name": name, "displayName": name})

    profile.save()
    profile.delete()


# Only works after stack-data
@pytest.mark.xfail(raises=MissingUdmExtension, reason="Missing OX UDM Extension")
@pytest.mark.timeout(10)
def test_create_functional_account(udm: UDM, maildomain, udm_module_exists):
    if not udm_module_exists("oxmail/functional_account"):
        raise MissingUdmExtension

    accessprofile = udm.get("oxmail/functional_account")
    assert accessprofile
    account = accessprofile.new()
    name = str(uuid.uuid1())

    account.properties.update({"name": name, "mailPrimaryAddress": f"{name}@{maildomain}"})

    account.save()
    account.delete()
