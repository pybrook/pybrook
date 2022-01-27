from typing import Dict, Iterable

import aioredis
import redis
from loguru import logger

from pybrook.config import MSG_ID_FIELD, SPECIAL_CHAR
from pybrook.consumers.base import (
    AsyncStreamConsumer,
    BaseStreamConsumer,
    GearsStreamConsumer,
    SyncStreamConsumer,
)
from pybrook.encoding import decode_value, encode_value


class BaseSplitter(BaseStreamConsumer):
    def __init__(self,
                 *,
                 redis_url: str,
                 consumer_group_name: str,
                 input_streams: Iterable[str],
                 object_id_field: str,
                 namespace: str,
                 read_chunk_length: int = 100,
                 **kwargs):
        self.namespace: str = namespace
        self.object_id_field: str = object_id_field
        super().__init__(redis_url=redis_url,
                         consumer_group_name=consumer_group_name,
                         input_streams=input_streams,
                         read_chunk_length=read_chunk_length,
                         **kwargs)

    def split_msg(self, message: Dict[str, str], *, obj_id: str,
                  obj_msg_id: str):
        message_id = f'{obj_id}{SPECIAL_CHAR}{obj_msg_id}'
        return {
            f'{SPECIAL_CHAR}{self.namespace}{SPECIAL_CHAR}split': {
                MSG_ID_FIELD: encode_value(message_id),
                **message
            }
        }

    def get_obj_msg_id_key(self, obj_id: str):
        return f'{SPECIAL_CHAR}id{SPECIAL_CHAR}{obj_id}'


class AsyncSplitter(AsyncStreamConsumer, BaseSplitter):
    async def process_message_async(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: aioredis.Redis,
            pipeline: aioredis.client.Pipeline) -> Dict[str, Dict[str, str]]:
        obj_id = decode_value(message[self.object_id_field])
        obj_msg_id = await redis_conn.incr(self.get_obj_msg_id_key(obj_id))
        return self.split_msg(message, obj_id=obj_id, obj_msg_id=obj_msg_id)


class SyncSplitter(SyncStreamConsumer, BaseSplitter):
    def process_message_sync(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: redis.Redis,
            pipeline: redis.client.Pipeline) -> Dict[str, Dict[str, str]]:
        obj_id = decode_value(message[self.object_id_field])
        obj_msg_id = str(redis_conn.incr(self.get_obj_msg_id_key(obj_id)))
        return self.split_msg(message, obj_id=obj_id, obj_msg_id=obj_msg_id)


class GearsSplitter(GearsStreamConsumer, BaseSplitter):
    def register_builder(self, pipeline: redis.client.Pipeline):
        cmd = f'''
from itertools import chain
from json import loads, dumps

def process_message(msg):
    message = msg["value"]
    obj_id = loads(message["{self.object_id_field}"])
    msg_id_key = f'{self.get_obj_msg_id_key("{obj_id}")}'
    obj_msg_id = execute("INCR", msg_id_key)
    out_stream = '{SPECIAL_CHAR}{self.namespace}{SPECIAL_CHAR}split'
    message["{MSG_ID_FIELD}"] = f'"{{obj_id}}:{{obj_msg_id}}"'
    execute("XADD", out_stream, '*', *chain(*message.items()))

for s in {self.input_streams}:
    GearsBuilder("StreamReader").foreach(process_message).register(s, trimStream=False, mode="sync")'''
        out = pipeline.execute_command('RG.PYEXECUTE', cmd)
        logger.info(f'Registered Redis Gears Reader: \n{cmd}\n{out}')


class Splitter(GearsSplitter, AsyncSplitter, SyncSplitter,
               BaseSplitter):  # noqa: WPS215
    """Splitter."""
