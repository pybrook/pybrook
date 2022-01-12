import dataclasses
from typing import Dict, List

import redis

from pybrook.config import MSG_ID_FIELD, SPECIAL_CHAR
from pybrook.consumers.base import SyncStreamConsumer
from pybrook.encoding import redisable_value_decoder


class DependencyResolver(SyncStreamConsumer):
    @dataclasses.dataclass
    class Dependency:
        src_stream: str
        src_key: str
        dst_key: str
        history_length: int = 0

    def __init__(self,
                 *,
                 redis_url: str,
                 resolver_name: str,
                 output_stream_name: str = None,
                 dependencies: List[Dependency],
                 read_chunk_length: int = 100):
        """

        Args:
            redis_url: Redis server URL
            resolver_name: Name of the resolver, used for the consumer group
            output_stream_name: name of the output stream
            dependencies:
                Keys are source streams, values are dependency names used as keys in the output stream.
                Source streams should contain just the internal msgid and values
                for the specific field.
            read_chunk_length: Redis XACK COUNT arg
        """
        self._dependencies: List[DependencyResolver.Dependency] = dependencies
        self._num_dependencies = len(dependencies)
        self._resolver_name = resolver_name
        if not output_stream_name:
            output_stream_name = f'{SPECIAL_CHAR}{self._resolver_name}{SPECIAL_CHAR}deps'
        self.output_stream_name: str = output_stream_name
        input_streams = list(set(s.src_stream for s in dependencies))
        super().__init__(redis_url=redis_url,
                         consumer_group_name=resolver_name,
                         input_streams=input_streams,
                         read_chunk_length=read_chunk_length)

    def __repr__(self):
        return f'<{self.__class__.__name__} output_stream_name=\'{self.output_stream_name}\'' \
               f' input_streams={self.input_streams}, dependencies={self._dependencies}>'

    def dependency_map_key(self, message_id: str):
        return f'{SPECIAL_CHAR}depmap{self.output_stream_name}{SPECIAL_CHAR}{message_id}'

    def process_message_sync(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: redis.Redis,
            pipeline: redis.client.Pipeline) -> Dict[str, Dict[str, str]]:
        message_id_enc = message.pop(MSG_ID_FIELD)
        message_id = redisable_value_decoder(message_id_enc)
        dep_key = self.dependency_map_key(message_id)
        incr_key = dep_key + f'{SPECIAL_CHAR}incr'
        new_deps = {
            k.dst_key: message[k.src_key]
            for k in self._dependencies if k.src_key in message
        }
        with redis_conn.pipeline() as p:
            p.multi()
            p.hset(dep_key, mapping=new_deps)  # type: ignore
            p.incrby(incr_key, len(new_deps))
            _, incr_num = p.execute()
        if incr_num == self._num_dependencies:
            dependencies = redis_conn.hgetall(dep_key)
            pipeline.multi()
            pipeline.delete(dep_key, incr_key)
            return {
                self.output_stream_name: {
                    MSG_ID_FIELD: message_id_enc,
                    **dependencies
                }
            }
        return {}
