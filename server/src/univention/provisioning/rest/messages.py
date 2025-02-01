# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

from typing import Annotated

import fastapi
from fastapi import Depends
from fastapi.security import HTTPBasic

from univention.provisioning.models.message import Message

from .dependencies import MQDependency, authenticate_events_endpoint

router = fastapi.APIRouter(prefix="/v1/messages", tags=["messages"])
security = HTTPBasic()


@router.post("", status_code=fastapi.status.HTTP_202_ACCEPTED)
async def create_new_message(
    msg: Message,
    authentication: Annotated[None, Depends(authenticate_events_endpoint)],
    mq: MQDependency,
):
    """Publish a new message to the incoming queue."""

    # TODO: set publisher_name from authentication data

    await mq.enqueue_for_dispatcher(msg)
