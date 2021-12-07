from typing import Dict, Iterable

import aioredis
import redis

from pybrook.config import FIELD_PREFIX, MSG_ID_FIELD, OBJECT_ID_FIELD
from pybrook.consumers.base import StreamConsumer


class Splitter(StreamConsumer):

    def __init__(self, *, redis_url: str, consumer_group_name: str, input_streams: Iterable[str], namespace: str,
                 read_chunk_length: int = 1):
        self.namespace: str = namespace
        super().__init__(redis_url=redis_url, consumer_group_name=consumer_group_name, input_streams=input_streams,
                         read_chunk_length=read_chunk_length)

    async def process_message_async(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: aioredis.Redis,
            pipeline: aioredis.client.Pipeline) -> Dict[str, Dict[str, str]]:
        obj_id = message.pop(OBJECT_ID_FIELD)
        obj_msg_id = await redis_conn.incr(self.get_obj_msg_id_key(obj_id))
        return self.split_msg(message, obj_id=obj_id, obj_msg_id=obj_msg_id)

    def split_msg(self, message: Dict[str, str], *, obj_id: str,
                  obj_msg_id: str):
        message_id = f'{obj_id}:{obj_msg_id}'
        return {
            f'{FIELD_PREFIX}{self.namespace}{FIELD_PREFIX}{k}': {
                MSG_ID_FIELD: message_id,
                k: v
            }
            for k, v in message.items()
        }

    def get_obj_msg_id_key(self, obj_id: str):
        return f'{FIELD_PREFIX}id{FIELD_PREFIX}{obj_id}'

    def process_message_sync(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: redis.Redis,
            pipeline: redis.client.Pipeline) -> Dict[str, Dict[str, str]]:
        obj_id = message.pop(OBJECT_ID_FIELD)
        obj_msg_id = str(redis_conn.incr(self.get_obj_msg_id_key(obj_id)))
        return self.split_msg(message, obj_id=obj_id, obj_msg_id=obj_msg_id)
