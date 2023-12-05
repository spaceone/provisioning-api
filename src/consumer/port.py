import contextlib
import json
from typing import List, Annotated, Optional, Union

from fastapi import Depends

from shared.adapters.nats_adapter import NatsAdapter
from shared.adapters.redis_adapter import RedisAdapter
from shared.config import settings
from shared.models import Message

from shared.models.queue import NatsMessage


class ConsumerPort:
    def __init__(self):
        self.redis_adapter = RedisAdapter()
        self.nats_adapter = NatsAdapter()

    @staticmethod
    @contextlib.asynccontextmanager
    async def port_context():
        port = ConsumerPort()
        await port.nats_adapter.nats.connect(
            servers=[f"nats://{settings.nats_host}:{settings.nats_port}"]
        )
        await port.nats_adapter.create_kv_store()
        try:
            yield port
        finally:
            await port.close()

    @staticmethod
    async def port_dependency():
        port = ConsumerPort()
        await port.nats_adapter.nats.connect(
            servers=[f"nats://{settings.nats_host}:{settings.nats_port}"]
        )
        await port.nats_adapter.create_kv_store()
        try:
            yield port
        finally:
            await port.close()

    async def close(self):
        await self.redis_adapter.close()
        await self.nats_adapter.close()

    async def add_live_message(self, subscriber_name: str, message: Message):
        await self.nats_adapter.add_message(subscriber_name, message)

    async def add_prefill_message(self, subscriber_name: str, message: Message):
        await self.redis_adapter.add_prefill_message(subscriber_name, message)

    async def delete_prefill_messages(self, subscriber_name: str):
        await self.redis_adapter.delete_prefill_messages(subscriber_name)

    async def get_next_message(
        self, subscriber_name: str, timeout: float, pop: bool
    ) -> List[NatsMessage]:
        return await self.nats_adapter.get_messages(subscriber_name, timeout, 1, pop)

    async def get_messages(
        self, subscriber_name: str, timeout: float, count: int, pop: bool
    ) -> List[NatsMessage]:
        return await self.nats_adapter.get_messages(
            subscriber_name, timeout, count, pop
        )

    async def remove_message(self, msg: NatsMessage):
        await self.nats_adapter.remove_message(msg)

    async def delete_queue(self, subscriber_name: str):
        await self.nats_adapter.delete_stream(subscriber_name)

    async def get_dict_value(self, name: str) -> Optional[dict]:
        sub = await self.nats_adapter.get_value(name)
        return json.loads(sub.value.decode("utf-8")) if sub else None

    async def get_list_value(self, key: str) -> List[str]:
        names = await self.nats_adapter.get_value(key)
        return names.value.decode("utf-8").split(",") if names else []

    async def get_str_value(self, key: str) -> Optional[str]:
        subs = await self.nats_adapter.get_value(key)
        return subs.value.decode("utf-8") if subs else None

    async def delete_kv_pair(self, key: str):
        await self.nats_adapter.delete_kv_pair(key)

    async def put_value(self, key: str, value: Union[str, dict]):
        await self.nats_adapter.put_value(key, value)


ConsumerPortDependency = Annotated[ConsumerPort, Depends(ConsumerPort.port_dependency)]
