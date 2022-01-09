import dataclasses
from typing import Callable, Dict, List, Type

import aioredis
import pydantic
import redis

from pybrook.config import ARTIFICIAL_NAMESPACE, MSG_ID_FIELD, SPECIAL_CHAR
from pybrook.consumers.base import (
    AsyncStreamConsumer,
    BaseStreamConsumer,
    SyncStreamConsumer,
)
from pybrook.encoding import redisable_encoder


class BaseFieldGenerator(BaseStreamConsumer):
    @dataclasses.dataclass
    class Dependency:
        name: str
        value_type: Type

    def __init__(self,
                 *,
                 generator: Callable,
                 redis_url: str,
                 field_name: str,
                 namespace: str = ARTIFICIAL_NAMESPACE,
                 dependency_stream: str,
                 dependencies: List[Dependency],
                 read_chunk_length: int = 100):
        self.generator = generator
        self.dependencies = dependencies
        self.field_name = field_name
        self.output_stream_name = f'{SPECIAL_CHAR}{namespace}{SPECIAL_CHAR}{field_name}'
        pydantic_fields = {
            dep.name: (dep.value_type, pydantic.Field())
            for dep in dependencies
        }
        self.dep_model: Type[pydantic.BaseModel] = pydantic.create_model(
            field_name + 'Model',
            **pydantic_fields  # type: ignore
        )
        super().__init__(redis_url=redis_url,
                         consumer_group_name=field_name,
                         input_streams=[dependency_stream],
                         read_chunk_length=read_chunk_length)


class AsyncFieldGenerator(AsyncStreamConsumer, BaseFieldGenerator):
    async def process_message_async(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: aioredis.Redis,
            pipeline: aioredis.client.Pipeline) -> Dict[str, Dict[str, str]]:
        message_id = message.pop(MSG_ID_FIELD)
        dependencies = self.dep_model(**message).dict()
        value = await self.generator(**dependencies)
        return {
            self.output_stream_name:
            redisable_encoder({
                MSG_ID_FIELD: message_id,
                self.field_name: value
            })
        }


class SyncFieldGenerator(SyncStreamConsumer, BaseFieldGenerator):
    def process_message_sync(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: redis.Redis,
            pipeline: redis.client.Pipeline) -> Dict[str, Dict[str, str]]:
        message_id = message.pop(MSG_ID_FIELD)
        dependencies = self.dep_model(**message).dict()
        value = self.generator(**dependencies)
        return {
            self.output_stream_name:
            redisable_encoder({
                MSG_ID_FIELD: message_id,
                self.field_name: value
            })
        }
