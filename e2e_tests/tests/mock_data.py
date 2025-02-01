# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import json
from copy import deepcopy
from datetime import datetime

from nats.aio.msg import Msg
from nats.js.kv import KeyValue

from univention.provisioning.models.message import (
    Body,
    Message,
    MQMessage,
    PrefillMessage,
    ProvisioningMessage,
    PublisherName,
)
from univention.provisioning.models.subscription import RealmTopic, Subscription

NATS_SERVER = "nats://localhost:4222"

REALM = "udm"
GROUPS_TOPIC = "groups/group"
USERS_TOPIC = "users/user"
DUMMY_TOPIC = "tests/topic"
BODY = Body(
    old={"old": "Old", "dn": "uid=foo,dc=bar", "objectType": "foo/bar"},
    new={"new": "New", "dn": "uid=foo,dc=bar", "objectType": "foo/bar"},
)
FLAT_BODY = {
    "old": {"old": "Old", "dn": "uid=foo,dc=bar", "objectType": "foo/bar"},
    "new": {"new": "New", "dn": "uid=foo,dc=bar", "objectType": "foo/bar"},
}
PUBLISHER_NAME = PublisherName.ldif_producer
GROUPS_REALMS_TOPICS = [RealmTopic(realm=REALM, topic=GROUPS_TOPIC)]
GROUPS_REALMS_TOPICS_as_dicts = [r.model_dump() for r in GROUPS_REALMS_TOPICS]
USERS_REALMS_TOPICS = [RealmTopic(realm=REALM, topic=USERS_TOPIC)]
DUMMY_REALMS_TOPICS = [RealmTopic(realm=REALM, topic=DUMMY_TOPIC)]
SUBSCRIPTION_NAME = "0f084f8c-1093-4024-b215-55fe8631ddf6"
REPLY = f"$JS.ACK.stream:{SUBSCRIPTION_NAME}.durable_name:{SUBSCRIPTION_NAME}.1.1.1.1699615014739091916.0"

CONSUMER_PASSWORD = "password"
CONSUMER_HASHED_PASSWORD = "$2b$12$G56ltBheLThdzppmOX.bcuAdZ.Ffx65oo7Elc.OChmzENtXtA1iSe"

SUBSCRIPTION_INFO = {
    "name": SUBSCRIPTION_NAME,
    "realms_topics": GROUPS_REALMS_TOPICS,
    "request_prefill": True,
    "prefill_queue_status": "done",
}
SUBSCRIPTION_INFO_dumpable = deepcopy(SUBSCRIPTION_INFO)
SUBSCRIPTION_INFO_dumpable["realms_topics"] = [r.model_dump() for r in SUBSCRIPTION_INFO_dumpable["realms_topics"]]
MESSAGE = Message(
    publisher_name=PUBLISHER_NAME,
    ts=datetime(2023, 11, 9, 11, 15, 52, 616061),
    realm=REALM,
    topic=GROUPS_TOPIC,
    body=BODY,
)
PREFILL_MESSAGE = PrefillMessage(
    publisher_name=PUBLISHER_NAME,
    ts=datetime(2023, 11, 9, 11, 15, 52, 616061),
    realms_topics=GROUPS_REALMS_TOPICS,
    subscription_name=SUBSCRIPTION_NAME,
)
PROVISIONING_MESSAGE = ProvisioningMessage(
    publisher_name=PUBLISHER_NAME,
    ts=datetime(2023, 11, 9, 11, 15, 52, 616061),
    realm=REALM,
    topic=GROUPS_TOPIC,
    body=BODY,
    sequence_number=1,
    num_delivered=1,
)

FLAT_BASE_MESSAGE = {
    "publisher_name": PUBLISHER_NAME,
    "ts": "2023-11-09T11:15:52.616061",
}
FLAT_MESSAGE = deepcopy(FLAT_BASE_MESSAGE)
FLAT_MESSAGE["body"] = FLAT_BODY
FLAT_MESSAGE["realm"] = REALM
FLAT_MESSAGE["topic"] = GROUPS_TOPIC

FLAT_PREFILL_MESSAGE = deepcopy(FLAT_BASE_MESSAGE)
FLAT_PREFILL_MESSAGE["subscription_name"] = SUBSCRIPTION_NAME
FLAT_PREFILL_MESSAGE["realms_topics"] = GROUPS_REALMS_TOPICS

FLAT_PREFILL_MESSAGE_MULTIPLE_TOPICS = deepcopy(FLAT_PREFILL_MESSAGE)
FLAT_PREFILL_MESSAGE_MULTIPLE_TOPICS["realms_topics"] = [
    RealmTopic(realm=REALM, topic=GROUPS_TOPIC),
    RealmTopic(realm=REALM, topic=USERS_TOPIC),
]

MSG = Msg(
    _client="nats",
    reply=REPLY,
    data=json.dumps(FLAT_MESSAGE).encode(),
    _metadata=Msg.Metadata(
        sequence=Msg.Metadata.SequencePair(consumer=5, stream=5),
        num_pending=0,
        num_delivered=1,
        timestamp=datetime(2023, 11, 9, 11, 15, 52, 616061),
        stream=f"stream:{SUBSCRIPTION_NAME}",
        consumer=SUBSCRIPTION_NAME,
        domain=None,
    ),
)
_FLAT_PREFILL_MESSAGE_dumpable = deepcopy(FLAT_PREFILL_MESSAGE)
_FLAT_PREFILL_MESSAGE_dumpable["realms_topics"] = [
    r.model_dump() for r in _FLAT_PREFILL_MESSAGE_dumpable["realms_topics"]
]
MSG_PREFILL = Msg(
    _client="nats",
    reply=REPLY,
    data=json.dumps(_FLAT_PREFILL_MESSAGE_dumpable).encode(),
    _metadata=Msg.Metadata(
        sequence=Msg.Metadata.SequencePair(consumer=5, stream=5),
        num_pending=0,
        num_delivered=1,
        timestamp=datetime(2023, 11, 9, 11, 15, 52, 616061),
        stream="stream:prefill",
        consumer="prefill-service",
        domain=None,
    ),
)
MSG_PREFILL_REDELIVERED = Msg(
    _client="nats",
    reply=REPLY,
    data=json.dumps(_FLAT_PREFILL_MESSAGE_dumpable).encode(),
    _metadata=Msg.Metadata(
        sequence=Msg.Metadata.SequencePair(consumer=5, stream=5),
        num_pending=0,
        num_delivered=4,
        timestamp=datetime(2023, 11, 9, 11, 15, 52, 616061),
        stream="stream:prefill",
        consumer="prefill-service",
        domain=None,
    ),
)

MQMESSAGE = MQMessage(
    subject="",
    reply=REPLY,
    data=FLAT_MESSAGE,
    headers=None,
    num_delivered=1,
    sequence_number=1,
)

MQMESSAGE_PREFILL = deepcopy(MQMESSAGE)
MQMESSAGE_PREFILL.data = FLAT_PREFILL_MESSAGE

MQMESSAGE_PREFILL_MULTIPLE_TOPICS = deepcopy(MQMESSAGE_PREFILL)
MQMESSAGE_PREFILL_MULTIPLE_TOPICS.data = FLAT_PREFILL_MESSAGE_MULTIPLE_TOPICS

MQMESSAGE_PREFILL_REDELIVERED = deepcopy(MQMESSAGE_PREFILL)
MQMESSAGE_PREFILL_REDELIVERED.num_delivered = 20

FLAT_MESSAGE_ENCODED = (
    b'{"publisher_name": "ldif-producer", "ts": "2023-11-09T11:15:52.616061", '
    b'"realm": "udm", "topic": "groups/group", '
    b'"body": {"old": {"old": "Old", "dn": "uid=foo,dc=bar", "objectType": "foo/bar"}, "new": {"new": "New", "dn": "uid=foo,dc=bar", "objectType": "foo/bar"}}}'
)

BASE_KV_OBJ = KeyValue.Entry(
    bucket="KV_bucket", key="", value=None, revision=1, delta=None, created=None, operation=None
)

SUBSCRIPTIONS = {REALM: {GROUPS_TOPIC: {Subscription.model_validate(SUBSCRIPTION_INFO)}}}
