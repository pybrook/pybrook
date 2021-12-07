import os
from typing import Dict, List

import aioredis
import redis
from loguru import logger

from pybrook.config import FIELD_PREFIX, MSG_ID_FIELD
from pybrook.consumers.base import StreamConsumer


class DependencyResolver(StreamConsumer):
    def __init__(self,
                 *,
                 redis_url: str,
                 resolver_name: str,
                 output_stream_name: str = None,
                 dependencies: Dict[str, str],
                 read_chunk_length: int = 10):
        """

        Args:
            redis_url:
            resolver_name:
            dependencies:
                Keys are source streams, values are dependency names used as keys in the output stream.
                Source streams should contain just the internal msgid and values
                for the specific field.
            read_chunk_length:
        """
        self._dependencies = dependencies
        self._num_dependencies = len(dependencies)
        self._resolver_name = resolver_name
        if not output_stream_name:
            output_stream_name = f'{FIELD_PREFIX}{self._resolver_name}{FIELD_PREFIX}deps'
        self.output_stream_name: str = output_stream_name
        input_streams = list(dependencies.keys())
        super().__init__(redis_url=redis_url,
                         consumer_group_name=resolver_name,
                         input_streams=input_streams,
                         read_chunk_length=read_chunk_length)

    def __repr__(self):
        return f'<{self.__class__.__name__} output_stream_name=\'{self.output_stream_name}\'' \
               f' input_streams={self.input_streams}, dependencies={self._dependencies}>'

    def dependency_map_key(self, message_id: str):
        return f'{self.output_stream_name}{FIELD_PREFIX}{message_id}'

    async def process_message_async(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: aioredis.Redis,
            pipeline: aioredis.client.Pipeline) -> Dict[str, Dict[str, str]]:
        raise NotImplementedError

    def process_message_sync(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: redis.Redis,
            pipeline: redis.client.Pipeline) -> Dict[str, Dict[str, str]]:
        field_name = stream_name.split('@')[-1]
        dependency_name = self._dependencies[stream_name]
        field_value = message[field_name]
        message_id = message[MSG_ID_FIELD]
        dep_key = self.dependency_map_key(message_id)
        redis_conn.hset(dep_key, dependency_name, field_value)
        logger.debug(
            f'{self._resolver_name} ({os.getpid()}): HSET {dep_key} {dependency_name} {field_value}'
        )
        pipeline.watch(dep_key)
        if redis_conn.hlen(dep_key) == self._num_dependencies:
            pipeline.multi()
            pipeline.delete(dep_key)
            return {
                self.output_stream_name: {
                    MSG_ID_FIELD: message_id,
                    **redis_conn.hgetall(dep_key)
                }
            }
        return {}
