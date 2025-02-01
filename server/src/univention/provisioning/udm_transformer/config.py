# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

from functools import lru_cache
from typing import Literal

from pydantic_settings import BaseSettings

from univention.provisioning.models.constants import PublisherName

Loglevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class UDMTransformerSettings(BaseSettings):
    # Python log level
    log_level: Loglevel

    # Nats user name specific to UdmProducerSettings
    nats_user: str
    # Nats password specific to UdmProducerSettings
    nats_password: str
    # Nats: host
    nats_host: str
    # Nats: port
    nats_port: int
    # Enables toggling between `ldif-producer` and `udm-listener`
    ldap_publisher_name: PublisherName

    # Events API: username
    events_username_udm: str
    # Events API: password
    events_password_udm: str

    # LDAP: host
    ldap_host: str
    # LDAP: port
    ldap_port: int
    # LDAP: tls_mode
    ldap_tls_mode: str
    # LDAP: base_dn
    ldap_base_dn: str
    # LDAP: host_dn
    ldap_bind_dn: str
    # LDAP: password
    ldap_bind_pw: str

    # Provisioning REST API: host
    provisioning_api_host: str
    # Provisioning REST API: port
    provisioning_api_port: int

    @property
    def nats_server(self) -> str:
        return f"nats://{self.nats_host}:{self.nats_port}"

    @property
    def ldap_server_uri(self) -> str:
        return f"ldap://{self.ldap_host}:{self.ldap_port}"

    @property
    def provisioning_api_url(self) -> str:
        return f"http://{self.provisioning_api_host}:{self.provisioning_api_port}"


@lru_cache(maxsize=1)
def udm_transformer_settings() -> UDMTransformerSettings:
    return UDMTransformerSettings()
