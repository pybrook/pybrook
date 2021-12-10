from typing import Dict, Iterable

import aioredis
import redis

from pybrook.config import FIELD_PREFIX, MSG_ID_FIELD
from pybrook.consumers.base import (
    AsyncStreamConsumer,
    BaseStreamConsumer,
    SyncStreamConsumer,
)


class BaseSplitter(BaseStreamConsumer):
    def __init__(self,
                 *,
                 redis_url: str,
                 consumer_group_name: str,
                 input_streams: Iterable[str],
                 object_id_field: str,
                 namespace: str,
                 read_chunk_length: int = 100):
        self.namespace: str = namespace
        self.object_id_field: str = object_id_field
        super().__init__(redis_url=redis_url,
                         consumer_group_name=consumer_group_name,
                         input_streams=input_streams,
                         read_chunk_length=read_chunk_length)

    def split_msg(self, message: Dict[str, str], *, obj_id: str,
                  obj_msg_id: str):
        message_id = f'{obj_id}:{obj_msg_id}'
        return {
            f'{FIELD_PREFIX}{self.namespace}{FIELD_PREFIX}split': {
                MSG_ID_FIELD: message_id,
                **message
            }
        }

    def get_obj_msg_id_key(self, obj_id: str):
        return f'{FIELD_PREFIX}id{FIELD_PREFIX}{obj_id}'


class AsyncSplitter(AsyncStreamConsumer, BaseSplitter):
    async def process_message_async(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: aioredis.Redis,
            pipeline: aioredis.client.Pipeline) -> Dict[str, Dict[str, str]]:
        obj_id = message[self.object_id_field]
        obj_msg_id = await redis_conn.incr(self.get_obj_msg_id_key(obj_id))
        return self.split_msg(message, obj_id=obj_id, obj_msg_id=obj_msg_id)


class SyncSplitter(SyncStreamConsumer, BaseSplitter):
    def process_message_sync(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: redis.Redis,
            pipeline: redis.client.Pipeline) -> Dict[str, Dict[str, str]]:
        obj_id = message[self.object_id_field]
        obj_msg_id = str(redis_conn.incr(self.get_obj_msg_id_key(obj_id)))
        return self.split_msg(message, obj_id=obj_id, obj_msg_id=obj_msg_id)
