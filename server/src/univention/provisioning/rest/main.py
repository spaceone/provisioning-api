# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH


import logging
from importlib.metadata import version

from asgi_correlation_id import CorrelationIdMiddleware
from asgi_correlation_id.context import correlation_id
from fastapi import FastAPI, HTTPException, Request, Response, status
from fastapi.exception_handlers import http_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi_utils.timing import add_timing_middleware

from univention.provisioning.models.constants import PREFILL_QUEUE_NAME
from univention.provisioning.utils.log import setup_logging

from .config import app_settings
from .messages import router as messages_api_router
from .mq_adapter_nats import NatsMessageQueue
from .subscriptions import router as subscriptions_api_router

settings = app_settings()
setup_logging(settings.log_level)
logger = logging.getLogger(__name__)


app = FastAPI(
    debug=settings.debug,
    description="APIs for subscription and message handling.",
    root_path=settings.root_path,
    title="Provisioning REST APIs",
    version=version("nubus-provisioning-rest-api"),
)
add_timing_middleware(app, record=logger.info)
app.add_middleware(CorrelationIdMiddleware)

if settings.cors_all:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

app.include_router(subscriptions_api_router)
app.include_router(messages_api_router)


def add_exception_handlers(_app: FastAPI):
    """Workaround for FastAPI not catching exceptions in sub-apps: https://github.com/fastapi/fastapi/issues/1802"""

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> Response:
        """Add Correlation-ID to HTTP 500."""
        return await http_exception_handler(
            request,
            HTTPException(
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                "Internal server error",
                headers={
                    CorrelationIdMiddleware.header_name: correlation_id.get() or "",
                    "Access-Control-Expose-Headers": CorrelationIdMiddleware.header_name,
                },
            ),
        )


add_exception_handlers(app)


@app.on_event("startup")
async def startup_task():
    logger.info("Started %s version %r.", app.title, version("nubus-provisioning-rest-api"))

    async with NatsMessageQueue(settings) as mq:
        logger.info("Checking MQ connectivity...")
        await mq.create_queue(PREFILL_QUEUE_NAME, False)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    exc_str = f"{exc}".replace("\n", " ").replace("   ", " ")
    logger.error("%s: %s", request, exc_str)
    content = {"status_code": status.HTTP_422_UNPROCESSABLE_ENTITY, "message": exc_str, "data": None}
    return JSONResponse(content=content, status_code=status.HTTP_422_UNPROCESSABLE_ENTITY)
