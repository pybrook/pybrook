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

import inspect
import pickle  # noqa: S403
from textwrap import dedent
from typing import Any, Callable, Dict, Iterable

import redis.asyncio as aioredis
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
    def register_readers(self, execute: Callable[..., Any],
                         gears_builder: Any):  # pragma: no cover
        import json
        from itertools import chain

        def process_message(msg):  # noqa: WPS430
            message = msg["value"]
            obj_id = json.loads(message[self.object_id_field])
            msg_id_key = f'{SPECIAL_CHAR}id{SPECIAL_CHAR}{obj_id}'
            obj_msg_id = execute("INCR", msg_id_key)
            out_stream = f'{SPECIAL_CHAR}{self.namespace}{SPECIAL_CHAR}split'
            message[MSG_ID_FIELD] = f'"{obj_id}:{obj_msg_id}"'
            execute("XADD", out_stream, '*', *chain(*message.items()))

        for s in self._input_streams:
            gears_builder("StreamReader").foreach(process_message).register(
                s, trimStream=False, mode="sync", batch=1000, duration=1)

    def register_builder(self, conn: redis.Redis):
        scope = {
            name: value
            for name, value in globals().items()
            if type(value) in {int, list, str, float}  # noqa: WPS516
            and not name.startswith('__')
        }
        context = {**scope, 'ctx': self.__dict__}
        registration_fun_code = dedent(inspect.getsource(
            self.register_readers)).split('@staticmethod', maxsplit=1)[-1]
        context_dumps = str(pickle.dumps(context))
        cmd = f'import pickle\nfrom typing import *\n{registration_fun_code}\n' \
              f'locals().update(pickle.loads({context_dumps}))\n' \
              f'register_readers(self=type("{self.__class__.__name__}", (), ctx), execute=execute, gears_builder=GearsBuilder)'''
        print(cmd)
        out = conn.execute_command('RG.PYEXECUTE', cmd)
        logger.info(f'Registered Redis Gears Reader: \n{cmd}\n{out}')


class Splitter(  # noqa: WPS215
        GearsSplitter, AsyncSplitter, SyncSplitter, BaseSplitter):
    """Splitter."""
