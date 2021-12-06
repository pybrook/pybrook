from typing import Dict

import aioredis

from pybrook.config import FIELD_PREFIX, MSG_ID_FIELD, OBJECT_ID_FIELD
from pybrook.consumers.base import StreamConsumer


class Splitter(StreamConsumer):
    async def process_message_async(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: aioredis.Redis,
            pipeline: aioredis.client.Pipeline) -> Dict[str, Dict[str, str]]:
        obj_id = message.pop(OBJECT_ID_FIELD)
        obj_msg_id = await redis_conn.incr(self.get_obj_msg_id_key(obj_id))
        return self.split_msg(message, obj_id=obj_id, obj_msg_id=obj_msg_id)

    @staticmethod
    def split_msg(message: Dict[str, str], *, obj_id: str, obj_msg_id: str):
        message_id = f'{obj_id}:{obj_msg_id}'
        return {
            f'{FIELD_PREFIX}{k}': {
                MSG_ID_FIELD: message_id,
                k: v
            }
            for k, v in message.items()
        }

    @staticmethod
    def get_obj_msg_id_key(obj_id: str):
        return f'{FIELD_PREFIX}id{FIELD_PREFIX}{obj_id}'

    def process_message_sync(self, stream_name, message, *,
                             redis_conn: aioredis.Redis,
                             pipeline: aioredis.client.Pipeline):
        obj_id = message.pop(OBJECT_ID_FIELD)
        obj_msg_id = str(
            redis_conn.incr(self.get_obj_msg_id_key(obj_id)))
        return self.split_msg(message, obj_id=obj_id, obj_msg_id=obj_msg_id)


