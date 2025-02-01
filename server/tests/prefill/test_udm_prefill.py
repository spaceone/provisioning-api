# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

from copy import deepcopy
from datetime import datetime
from unittest.mock import ANY, AsyncMock, call, patch

import pytest
from polyfactory.factories.pydantic_factory import ModelFactory
from polyfactory.pytest_plugin import register_fixture

from univention.provisioning.backends.message_queue import MessageAckManager
from univention.provisioning.models.constants import PREFILL_SUBJECT_TEMPLATE, PublisherName
from univention.provisioning.models.message import Body, Message
from univention.provisioning.prefill.config import PrefillSettings
from univention.provisioning.prefill.mq_port import MessageQueuePort
from univention.provisioning.prefill.prefill_service import PrefillService
from univention.provisioning.prefill.udm_port import UDMPort
from univention.provisioning.prefill.update_sub_q_status_port import UpdateSubscriptionsQueueStatusPort
from univention.provisioning.testing.mock_data import (
    GROUPS_TOPIC,
    MQMESSAGE_PREFILL,
    MQMESSAGE_PREFILL_MULTIPLE_TOPICS,
    MQMESSAGE_PREFILL_REDELIVERED,
    SUBSCRIPTION_NAME,
    USERS_TOPIC,
)


class EscapeLoopException(Exception): ...


@register_fixture
class PrefillSettingsFactory(ModelFactory[PrefillSettings]): ...


@pytest.fixture
def udm_prefill(prefill_settings_factory: ModelFactory[PrefillSettings]) -> PrefillService:
    udm_prefill = PrefillService(
        ack_manager=MessageAckManager(),
        mq=AsyncMock(spec_set=MessageQueuePort),
        udm=AsyncMock(spec_set=UDMPort),
        update_sub_q_status=AsyncMock(spec_set=UpdateSubscriptionsQueueStatusPort),
        settings=prefill_settings_factory.build(),
    )
    udm_prefill.max_prefill_attempts = 5
    return udm_prefill


@pytest.mark.anyio
class TestUDMPreFill:
    prefill_subject = PREFILL_SUBJECT_TEMPLATE.format(subscription=SUBSCRIPTION_NAME)
    mocked_date = datetime(2023, 11, 9, 11, 15, 52, 616061)
    url = f"http://udm-rest-api:9979/udm/{GROUPS_TOPIC}/..."
    url_2 = f"http://udm-rest-api:9979/udm/{USERS_TOPIC}/..."
    udm_modules = {
        "name": GROUPS_TOPIC,
        "title": "Group",
        "href": f"http://udm-rest-api:9979/udm/{GROUPS_TOPIC}/",
    }
    udm_modules_2 = deepcopy(udm_modules)
    udm_modules_2["name"] = USERS_TOPIC
    udm_modules_2["href"] = f"http://udm-rest-api:9979/udm/{USERS_TOPIC}/"
    obj = {
        "uri": url,
        "dn": "",
        "objectType": GROUPS_TOPIC,
        "position": "",
        "properties": {},
        "uuid": "",
    }
    obj_2 = deepcopy(obj)
    obj_2["objectType"] = USERS_TOPIC
    obj_2["uri"] = url_2
    msg = Message(
        publisher_name=PublisherName.udm_pre_fill,
        ts=mocked_date,
        realm="udm",
        topic=GROUPS_TOPIC,
        body=Body(old={}, new=obj),
    )
    msg2 = deepcopy(msg)
    msg2.topic = USERS_TOPIC
    msg2.body.new = obj_2

    @patch("univention.provisioning.prefill.prefill_service.datetime")
    async def test_handle_requests_to_prefill(self, mock_datetime, udm_prefill: PrefillService):
        mock_datetime.now.return_value = self.mocked_date
        mock_acknowledgements = AsyncMock()
        udm_prefill.mq.get_one_message = AsyncMock(
            side_effect=[
                (MQMESSAGE_PREFILL, mock_acknowledgements),
                EscapeLoopException("Stop waiting for the new event"),
            ]
        )
        udm_prefill.udm.get_object_types = AsyncMock(return_value=[self.udm_modules])
        udm_prefill.udm.list_objects = AsyncMock(return_value=[self.url])
        udm_prefill.udm.get_object = AsyncMock(return_value=self.obj)

        with pytest.raises(EscapeLoopException):
            await udm_prefill.handle_requests_to_prefill()

        udm_prefill.mq.initialize_subscription.assert_called_once()
        udm_prefill.mq.purge_queue.assert_called_once_with(SUBSCRIPTION_NAME)
        udm_prefill.mq.get_one_message.assert_has_calls([call(), call()])
        udm_prefill.udm.get_object_types.assert_called_once_with()
        udm_prefill.udm.list_objects.assert_called_once_with(GROUPS_TOPIC)
        udm_prefill.udm.get_object.assert_called_once_with(self.url)
        mock_acknowledgements.acknowledge_message.assert_called_once()
        udm_prefill.mq.add_message_to_queue.assert_called_once_with(SUBSCRIPTION_NAME, self.msg)
        udm_prefill.mq.add_message_to_failures_queue.assert_not_called()

    @patch("univention.provisioning.prefill.prefill_service.datetime")
    async def test_handle_requests_to_prefill_multiple_topics(self, mock_datetime, udm_prefill: PrefillService):
        mock_datetime.now.return_value = self.mocked_date
        mock_acknowledgements = AsyncMock()
        udm_prefill.mq.get_one_message = AsyncMock(
            side_effect=[
                (MQMESSAGE_PREFILL_MULTIPLE_TOPICS, mock_acknowledgements),
                EscapeLoopException("Stop waiting for the new event"),
            ]
        )
        udm_prefill.udm.get_object_types = AsyncMock(side_effect=[[self.udm_modules], [self.udm_modules_2]])
        udm_prefill.udm.list_objects = AsyncMock(side_effect=[[self.url], [self.url_2]])
        udm_prefill.udm.get_object = AsyncMock(side_effect=[self.obj, self.obj_2])

        with pytest.raises(EscapeLoopException, match="Stop waiting for the new event"):
            await udm_prefill.handle_requests_to_prefill()

        udm_prefill.mq.initialize_subscription.assert_called_once_with()
        udm_prefill.mq.purge_queue.assert_called_once_with(SUBSCRIPTION_NAME)
        udm_prefill.mq.get_one_message.assert_has_calls([call(), call()])
        udm_prefill.udm.get_object_types.assert_has_calls([call(), call()])
        udm_prefill.udm.list_objects.assert_has_calls([call(GROUPS_TOPIC), call(USERS_TOPIC)])
        udm_prefill.udm.get_object.assert_has_calls([call(self.url), call(self.url_2)])
        mock_acknowledgements.acknowledge_message.assert_called_once()
        udm_prefill.mq.add_message_to_queue.assert_has_calls(
            [
                call(SUBSCRIPTION_NAME, self.msg),
                call(SUBSCRIPTION_NAME, self.msg2),
            ]
        )
        udm_prefill.mq.add_message_to_failures_queue.assert_not_called()

    @patch("univention.provisioning.prefill.prefill_service.datetime")
    async def test_handle_requests_to_prefill_moving_to_failures(self, mock_datetime, udm_prefill: PrefillService):
        mock_datetime.now.return_value = self.mocked_date
        mock_acknowledgements = AsyncMock()
        udm_prefill.mq.get_one_message = AsyncMock(
            side_effect=[
                (MQMESSAGE_PREFILL_REDELIVERED, mock_acknowledgements),
                EscapeLoopException("Stop waiting for the new event"),
            ]
        )

        with pytest.raises(EscapeLoopException, match="Stop waiting for the new event"):
            await udm_prefill.handle_requests_to_prefill()

        udm_prefill.mq.initialize_subscription.assert_called_once_with()
        udm_prefill.mq.get_one_message.assert_has_calls([call(), call()])
        udm_prefill.udm.get_object_types.assert_not_called()
        udm_prefill.udm.list_objects.assert_not_called()
        udm_prefill.udm.get_object.assert_not_called()
        mock_acknowledgements.acknowledge_message.assert_called_once()
        udm_prefill.mq.add_message_to_queue.assert_not_called()
        udm_prefill.mq.add_message_to_failures_queue.assert_called_once_with(ANY)

    @patch("univention.provisioning.prefill.prefill_service.datetime")
    async def test_fetch_no_udm_module(self, mock_datetime, udm_prefill: PrefillService):
        mock_datetime.now.return_value = self.mocked_date
        udm_prefill.udm.get_object_types = AsyncMock(return_value=[self.udm_modules])

        result = await udm_prefill.fetch_udm("test-subject", "test-topic")

        assert result is None
        udm_prefill.udm.get_object_types.assert_called_once_with()
        udm_prefill.udm.list_objects.assert_not_called()
        udm_prefill.udm.get_object.assert_not_called()
        udm_prefill.mq.add_message_to_queue.assert_not_called()


class TestMatchMethod:
    def test_subscription_match(self):
        test_cases = [
            # list of test cases:
            # subscription sub.topic, target.topic, expected result
            ("users/user", "users/user", True),
            ("users/user", "groups/group", False),
            ("users/.*", "users/user", True),
            ("users/.*", "groups/group", False),
            (".*", "users/user", True),
            (".*", "groups/group", True),
        ]

        for sub_topic, target_topic, expectation in test_cases:
            assert PrefillService.match_topic(sub_topic, target_topic) == expectation
