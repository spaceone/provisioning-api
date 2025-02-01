# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

from functools import lru_cache

from pydantic_settings import BaseSettings


class AppSettings(BaseSettings):
    # Python log level
    log_level: str

    # FastAPI: debug mode: send traceback in response on errors
    debug: bool
    # FastAPI: webserver root path
    root_path: str
    # FastAPI: disable CORS checks
    cors_all: bool

    # Admin API: username
    admin_username: str
    # Admin API: password
    admin_password: str

    # Nats user name specific to Consumer and Internal API
    nats_user: str
    # Nats password specific to Consumer and Internal API
    nats_password: str
    # Nats: host
    nats_host: str
    # Nats: port
    nats_port: int
    # TODO: Remove this, it's not used and a security risk
    # Admin Nats user name
    admin_nats_user: str
    # Admin Nats password
    admin_nats_password: str

    # Prefill: username
    prefill_username: str
    # Prefill: password
    prefill_password: str

    # Events API: username
    events_username_udm: str
    # Events API: password
    events_password_udm: str

    @property
    def nats_server(self) -> str:
        return f"nats://{self.nats_host}:{self.nats_port}"


@lru_cache(maxsize=1)
def app_settings() -> AppSettings:
    return AppSettings()
