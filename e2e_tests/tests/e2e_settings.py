# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH
from typing import NamedTuple
from urllib.parse import urljoin


class E2ETestSettings(NamedTuple):
    provisioning_api_base_url: str
    provisioning_admin_username: str
    provisioning_admin_password: str

    provisioning_events_username: str
    provisioning_events_password: str

    nats_url: str
    nats_user: str
    nats_password: str

    ldap_server_uri: str
    ldap_base: str
    ldap_bind_dn: str
    ldap_bind_password: str

    udm_rest_api_base_url: str
    udm_rest_api_username: str
    udm_rest_api_password: str

    @property
    def subscriptions_url(self) -> str:
        return urljoin(self.provisioning_api_base_url, "/v1/subscriptions")

    def subscriptions_messages_url(self, name: str) -> str:
        return f"{self.subscriptions_url}/{name}/messages"

    @property
    def messages_url(self) -> str:
        return urljoin(self.provisioning_api_base_url, "/v1/messages")
