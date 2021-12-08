import dataclasses
from typing import Any, Callable, Dict, Iterable, List, Tuple, Type

import aioredis
import pydantic
import redis
from pydantic import BaseModel

from pybrook.config import ARTIFICIAL_NAMESPACE, FIELD_PREFIX, MSG_ID_FIELD
from pybrook.consumers.base import StreamConsumer
from pybrook.utils import redisable_encoder


class FieldGenerator(StreamConsumer):
    @dataclasses.dataclass
    class Dependency:
        name: str
        value_type: Type

    def __init__(self,
                 *,
                 generator_sync: Callable = None,
                 generator_async: Callable = None,
                 redis_url: str,
                 field_name: str,
                 namespace: str = ARTIFICIAL_NAMESPACE,
                 dependency_stream: str,
                 dependencies: List[Dependency],
                 read_chunk_length: int = 100):
        if not (generator_sync or generator_async):
            raise RuntimeError(
                'FieldGenerator does not have a purpose without a generator function!'
            )
        self.generator_sync = generator_sync
        self.generator_async = generator_async
        self.dependencies = dependencies
        self.field_name = field_name
        self.output_stream_name = f'{FIELD_PREFIX}{namespace}{FIELD_PREFIX}{field_name}'
        pydantic_fields = {
            dep.name: (dep.value_type, pydantic.Field())
            for dep in dependencies
        }
        self.dep_model: Type[BaseModel] = pydantic.create_model(
            field_name + 'Model',
            **pydantic_fields  # type: ignore
        )
        super().__init__(redis_url=redis_url,
                         consumer_group_name=field_name,
                         input_streams=[dependency_stream],
                         read_chunk_length=read_chunk_length)

    async def process_message_async(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: aioredis.Redis,
            pipeline: aioredis.client.Pipeline) -> Dict[str, Dict[str, str]]:
        message_id = message.pop(MSG_ID_FIELD)
        dependencies = self.dep_model(**message).dict()
        value = await self.generator_async(**dependencies)
        return {
            self.output_stream_name:
            redisable_encoder({
                MSG_ID_FIELD: message_id,
                self.field_name: value
            })
        }

    def process_message_sync(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: redis.Redis,
            pipeline: redis.client.Pipeline) -> Dict[str, Dict[str, str]]:
        message_id = message.pop(MSG_ID_FIELD)
        dependencies = self.dep_model(**message).dict()
        value = self.generator_sync(**dependencies)
        return {
            self.output_stream_name:
            redisable_encoder({
                MSG_ID_FIELD: message_id,
                self.field_name: value
            })
        }
