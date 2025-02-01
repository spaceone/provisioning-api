# SPDX-License-Identifier: AGPL-3.0-only
# SPDX-FileCopyrightText: 2024 Univention GmbH

import asyncio
import logging
from typing import Optional

import cachetools.func
from fastapi import HTTPException, status
from fastapi.security import HTTPBasicCredentials
from passlib.context import CryptContext

from univention.provisioning.models.subscription import FillQueueStatus, NewSubscription, Subscription

from .mq_port import MessageQueuePort
from .subscriptions_db_port import NoSubscription, SubscriptionsDBPort

logger = logging.getLogger(__name__)
password_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
PASSWORD_CACHE_TTL = 30.0
REALM_TOPIC_TEMPLATE = "{realm}:{topic}"


@cachetools.func.ttl_cache(maxsize=32, ttl=PASSWORD_CACHE_TTL)
def verify_and_update_password(
    cleartext_pw: str, hashed_pw: str, subscription_name: Optional[str] = None
) -> tuple[bool, str | None]:
    """
    Caching wrapper around CryptContext.verify_and_update()

    Caches all hits and exceptions!
    But the caller (SubscriptionService.authenticate_user()) will delete negative hits, so we cache only positive hits.
    """
    return password_context.verify_and_update(cleartext_pw, hashed_pw)  # takes ~200ms


class SubscriptionService:
    def __init__(self, subscriptions_db: SubscriptionsDBPort, mq: MessageQueuePort):
        self.sub_db = subscriptions_db
        self.mq = mq

    async def get_subscription(self, name: str) -> Subscription:
        """
        Get information about a registered subscription.
        """
        try:
            return await self.sub_db.load_subscription(name)
        except NoSubscription as exc:
            raise ValueError(str(exc))

    async def get_subscriptions(self) -> list[Subscription]:
        """
        Return a list of all known subscriptions.
        """
        names = await self.sub_db.load_subscription_names()
        return [await self.get_subscription(name) for name in names]

    @staticmethod
    def hash_password(password: str) -> str:
        return password_context.hash(password)

    async def is_subscriptions_matching(self, new_sub: NewSubscription, existing_sub: Subscription) -> bool:
        """
        Determine if `new_sub` and `existing_sub` have the same values in their common fields
        (ignoring `prefill_queue_status` and `password`).
        """
        if existing_sub.request_prefill != new_sub.request_prefill:
            return False

        if new_sub.realms_topics != existing_sub.realms_topics:
            return False

        hashed_password = await self.sub_db.load_hashed_password(new_sub.name)
        valid = password_context.verify(new_sub.password, hashed_password)
        if not valid:
            return False

        return True

    async def register_subscription(self, new_sub: NewSubscription) -> bool:
        """
        Register a new subscription.

        :returns: True is a new subscription was created or False if a matching one already exists.
        :raises HTTPException(409): If a subscription already exists, but its configuration does not match `new_sub`.
        """

        try:
            existing_sub = await self.sub_db.load_subscription(new_sub.name)
            if not await self.is_subscriptions_matching(new_sub, existing_sub):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Subscription with the given name already registered but with different parameters.",
                )
            return False
        except NoSubscription:
            logger.info(
                "Registering new subscription (name: %r realms_topics: %r request_prefill: %r).",
                new_sub.name,
                new_sub.realms_topics,
                new_sub.request_prefill,
            )
            encrypted_password = self.hash_password(new_sub.password)
            await self.sub_db.store_hashed_password(new_sub.name, encrypted_password)
            await self.prepare_and_store_subscription_info(new_sub)
            logger.info("New subscription was registered: %r", new_sub.name)
            return True

    async def prepare_and_store_subscription_info(self, new_sub: NewSubscription):
        if new_sub.request_prefill:
            prefill_queue_status = FillQueueStatus.pending
        else:
            prefill_queue_status = FillQueueStatus.done

        subscription = Subscription(
            name=new_sub.name,
            realms_topics=new_sub.realms_topics,
            request_prefill=new_sub.request_prefill,
            prefill_queue_status=prefill_queue_status,
        )
        await self.sub_db.store_subscription(new_sub.name, subscription)
        await self.mq.prepare_new_consumer_queue(new_sub.name)
        await self.mq.create_consumer(new_sub.name)

    async def get_subscription_queue_status(self, name: str) -> FillQueueStatus:
        """Get the pre-fill status of the given subscription."""
        subscription = await self.get_subscription(name)
        return subscription.prefill_queue_status

    async def set_subscription_queue_status(self, name: str, status: FillQueueStatus):
        """Set the pre-fill status of the given subscription."""
        subscription = await self.get_subscription(name)
        subscription.prefill_queue_status = status.name
        await self.sub_db.store_subscription(name, subscription)

    async def delete_subscription(self, name: str) -> None:
        """
        Delete a subscription and all of its data.
        """
        _ = await self.get_subscription(name)
        await self.sub_db.delete_subscription(name)
        await self.mq.delete_queue(name)

    @staticmethod
    def handle_authentication_error(message: str):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=message,
            headers={"WWW-Authenticate": "Basic"},
        )

    async def authenticate_user(self, credentials: HTTPBasicCredentials, subscription_name: Optional[str] = None):
        if subscription_name and subscription_name != credentials.username:
            self.handle_authentication_error("You do not have access to this data")

        hashed_password = await self.sub_db.load_hashed_password(credentials.username)
        cached_func_args = (credentials.password, hashed_password, subscription_name)
        valid, new_hash = verify_and_update_password(*cached_func_args)
        if valid:
            if new_hash:
                logger.info("Storing new password hash for user %r.", credentials.username)
                await self.sub_db.store_hashed_password(credentials.username, new_hash)
        else:
            # cache only positive authentication attempts -> remove cache entry for invalid password
            cache_key = verify_and_update_password.cache_key(*cached_func_args)
            verify_and_update_password.cache.pop(cache_key, None)
            self.handle_authentication_error("Incorrect username or password")

    async def check_subscription_queue_status(self, subscription_name: str, timeout: float) -> FillQueueStatus:
        loop = asyncio.get_event_loop()
        end_time = loop.time() + timeout
        while loop.time() < end_time:
            queue_status = await self.get_subscription_queue_status(subscription_name)
            if queue_status == FillQueueStatus.done:
                return queue_status
            await asyncio.sleep(1)

        return await self.get_subscription_queue_status(subscription_name)
