from typing import Iterable, Dict

import aioredis

from pybrook.config import FIELD_PREFIX, MSG_ID_FIELD
from pybrook.consumers.base import StreamConsumer


class DependencyResolver(StreamConsumer):
    def __init__(self,
                 *,
                 redis_url: str,
                 resolver_name: str,
                 dependency_names: Iterable[str],
                 read_chunk_length: int = 1):
        self._dependency_names = tuple(set(dependency_names))
        self._num_dependencies = len(dependency_names)
        self._resolver_name = resolver_name
        input_streams = tuple(f'{FIELD_PREFIX}{name}'
                              for name in self._dependency_names)
        super().__init__(redis_url=redis_url,
                         consumer_group_name=resolver_name,
                         input_streams=input_streams,
                         read_chunk_length=read_chunk_length)

    @property
    def output_stream_key(self):
        return f'{FIELD_PREFIX}{self._resolver_name}_deps'

    def dependency_map_key(self, message_id: str):
        return f'{self.output_stream_key}{FIELD_PREFIX}{message_id}'

    async def process_message_async(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: aioredis.Redis,
            pipeline: aioredis.client.Pipeline) -> Dict[str, Dict[str, str]]:
        raise NotImplementedError

    def process_message_sync(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: aioredis.Redis,
            pipeline: aioredis.client.Pipeline) -> Dict[str, Dict[str, str]]:
        field_name = stream_name[1:]
        field_value = message[field_name]
        message_id = message[MSG_ID_FIELD]
        dep_key = self.dependency_map_key(message_id)
        redis_conn.hset(dep_key, field_name, field_value)
        pipeline.watch(dep_key)
        if redis_conn.hlen(dep_key) == self._num_dependencies:
            pipeline.multi()
            pipeline.delete(dep_key)
            return {
                self.output_stream_key: {
                    MSG_ID_FIELD: message_id,
                    **redis_conn.hgetall(dep_key)
                }
            }
        return {}