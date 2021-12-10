from itertools import chain
from typing import Dict

import aioredis
import redis

from pybrook.config import FIELD_PREFIX, MSG_ID_FIELD
from pybrook.consumers.base import SyncStreamConsumer


class DependencyResolver(SyncStreamConsumer):
    def __init__(self,
                 *,
                 redis_url: str,
                 resolver_name: str,
                 output_stream_name: str = None,
                 dependencies: Dict[str, str],
                 read_chunk_length: int = 100):
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
        input_streams = list(set(dependencies.values()))
        super().__init__(redis_url=redis_url,
                         consumer_group_name=resolver_name,
                         input_streams=input_streams,
                         read_chunk_length=read_chunk_length)

    def __repr__(self):
        return f'<{self.__class__.__name__} output_stream_name=\'{self.output_stream_name}\'' \
               f' input_streams={self.input_streams}, dependencies={self._dependencies}>'

    def dependency_map_key(self, message_id: str):
        return f'{FIELD_PREFIX}depmap{self.output_stream_name}{FIELD_PREFIX}{message_id}'

    def process_message_sync(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: redis.Redis,
            pipeline: redis.client.Pipeline) -> Dict[str, Dict[str, str]]:
        message_id = message.pop(MSG_ID_FIELD)
        dep_key = self.dependency_map_key(message_id)
        incr_key = dep_key + f'{FIELD_PREFIX}incr'
        message = {k: message[k] for k in self._dependencies if k in message}
        with redis_conn.pipeline() as p:
            p.multi()
            p.hset(dep_key, mapping=message)  # type: ignore
            p.incrby(incr_key, len(message))
            _, incr_num = p.execute()
        if incr_num == self._num_dependencies:
            dependencies = redis_conn.hgetall(dep_key)
            pipeline.multi()
            pipeline.delete(dep_key, incr_key)
            return {
                self.output_stream_name: {
                    MSG_ID_FIELD: message_id,
                    **dependencies
                }
            }
        return {}
