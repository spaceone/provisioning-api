import requests
import uuid

from consumer.subscriptions.api import v1_prefix as subscriptions_api_prefix
from consumer.messages.api import v1_prefix as messages_api_prefix

from udm_messaging.port import UDMMessagingPort
from udm_messaging.service.udm import UDMMessagingService

REALM = "udm"
TOPIC = "users/user"
BODY = {"old": {"New": "Object"}, "new": {"Old": "Object"}}

def test_workflow():

    name = str(uuid.uuid4())

    realm_topic = [REALM, TOPIC]
    # call of Consumer: create subscription
    response = requests.post(
            f"{subscriptions_api_prefix}/subscription/",
            json={
                "name": name,
                "realm_topic": realm_topic,
                "fill_queue": True,
            },
        )
    assert response.status_code == 201

    # Skip triggering LDAP

    # call of UDMMessagingService: send event to Event REST API

    async with UDMMessagingPort.port_context() as port:
        service = UDMMessagingService(port)
        service.send_event({"New": "Object"}, {"Old": "Object"})

    # call of Dispatcher: get event

    response = requests.get(
        f"{messages_api_prefix}/subscription/{name}/message"
    )
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["data"]["realm"] == REALM
    assert data[0]["data"]["topic"] == TOPIC
    assert data[0]["data"]["body"] == BODY
    assert data[0]["data"]["publisher_name"] == "127.0.0.1"

