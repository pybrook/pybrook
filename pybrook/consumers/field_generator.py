#  PyBrook
#
#  Copyright (C) 2023  Michał Rokita
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

import dataclasses
from typing import Callable, Dict, List, Type, Union

import redis.asyncio as aioredis
import pydantic
import redis

from pybrook.config import ARTIFICIAL_NAMESPACE, MSG_ID_FIELD, SPECIAL_CHAR
from pybrook.consumers.base import (
    AsyncStreamConsumer,
    BaseStreamConsumer,
    SyncStreamConsumer,
)
from pybrook.encoding import decode_stream_message, encode_stream_message

AnyRedis = Union[aioredis.Redis, redis.Redis]


class BaseFieldGenerator(BaseStreamConsumer):
    @dataclasses.dataclass
    class Dep:  # noqa: WPS431
        name: str
        value_type: Type

    def __init__(self,
                 *,
                 generator: Callable,
                 redis_url: str,
                 field_name: str,
                 namespace: str = ARTIFICIAL_NAMESPACE,
                 dependency_stream: str,
                 dependencies: List[Dep],
                 redis_deps: List[str] = None,
                 read_chunk_length: int = 200,
                 **kwargs):
        self.generator = generator
        self.dependencies = dependencies
        self.field_name = field_name
        self.redis_deps = redis_deps or []
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
                         use_thread_executor=True,
                         consumer_group_name=f'{field_name}{SPECIAL_CHAR}fg',
                         input_streams=[dependency_stream],
                         read_chunk_length=read_chunk_length,
                         **kwargs)

    def __repr__(self):
        return f'<{self.__class__.__name__} input_streams={self.input_streams}>'

    def call_generator(self, dependencies, redis_conn: AnyRedis):
        if self.redis_deps:
            return self.generator(**dependencies,
                                  **{k: redis_conn
                                     for k in self.redis_deps})
        return self.generator(**dependencies)


class AsyncFieldGenerator(AsyncStreamConsumer, BaseFieldGenerator):
    async def process_message_async(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: aioredis.Redis,
            pipeline: aioredis.client.Pipeline) -> Dict[str, Dict[str, str]]:
        message = decode_stream_message(message)
        message_id = message.pop(MSG_ID_FIELD)
        try:
            dependencies = self.dep_model(**message).dict()
        except Exception:
            raise ValueError(message)
        value = await self.call_generator(dependencies, redis_conn)
        return {
            self.output_stream_name:
            encode_stream_message({
                MSG_ID_FIELD: message_id,
                self.field_name: value
            })
        }


class SyncFieldGenerator(SyncStreamConsumer, BaseFieldGenerator):
    def process_message_sync(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: redis.Redis,
            pipeline: redis.client.Pipeline) -> Dict[str, Dict[str, str]]:
        message = decode_stream_message(message)
        message_id = message.pop(MSG_ID_FIELD)
        try:
            dependencies = self.dep_model(**message).dict()
        except Exception:
            raise ValueError(message)
        value = self.call_generator(dependencies, redis_conn)
        return {
            self.output_stream_name:
            encode_stream_message({
                MSG_ID_FIELD: message_id,
                self.field_name: value
            })
        }
