import asyncio
import secrets
from typing import Dict, Iterable, Mapping, Tuple

import aioredis
import redis
from loguru import logger

from pybrook.config import INTERNAL_FIELD_PREFIX, MSG_ID_FIELD, OBJECT_ID_FIELD


class StreamConsumer:
    def __init__(self, *, redis_url: str, group_name: str,
                 input_streams: Iterable[str]):
        self._group_name = group_name
        self._redis_url = redis_url
        self._active = False
        self.input_streams = input_streams
        self.consumer_name = secrets.token_urlsafe(64)

    @property
    def input_streams(self) -> Tuple[str]:
        return self._input_streams

    @input_streams.setter
    def input_streams(self, streams: Iterable[str]):
        self._input_streams = tuple(streams)

    async def process_message_async(
            self, stream: bytes, message: Dict[bytes, bytes], *,
            redis_conn: aioredis.Redis) -> Dict[str, Dict[str, str]]:
        raise NotImplementedError

    def process_message_sync(
            self, stream: bytes, message: Dict[bytes, bytes], *,
            redis_conn: aioredis.Redis) -> Dict[str, Dict[str, str]]:
        logger.warning(
            f'Sync version of process_message for {self.__name__} not implemented.'
        )
        return asyncio.run(self.process_message_async(stream, message))

    async def create_groups_async(self):
        redis_conn = await aioredis.from_url(self._redis_url)
        for stream in self.input_streams:
            try:
                await redis_conn.xgroup_create(stream,
                                               self._group_name,
                                               mkstream=True)
            except aioredis.ResponseError as e:
                if 'BUSYGROUP' not in str(e):
                    raise e

    def create_groups_sync(self):
        redis_conn = redis.from_url(self._redis_url)
        for stream in self.input_streams:
            try:
                redis_conn.xgroup_create(stream,
                                         self._group_name,
                                         mkstream=True)
            except aioredis.ResponseError as e:
                if 'BUSYGROUP' not in str(e):
                    raise e

    @property
    def _xreadgroup_params(self) -> Mapping:
        return dict(streams={s: '>'
                             for s in self.input_streams},
                    groupname=self._group_name,
                    consumername=self.consumer_name,
                    count=100,
                    block=10)

    async def run_async(self):
        redis_conn = await aioredis.from_url(self._redis_url)
        self._active = True
        while self._active:
            response = await redis_conn.xreadgroup(**self._xreadgroup_params)
            for stream, messages in response:
                for m_id, payload in messages:
                    result = await self.process_message_async(
                        stream, payload, redis_conn=redis_conn)
                    async with redis_conn.pipeline() as p:
                        for out_stream, out_msg in result.items():
                            await p.xadd(out_stream, out_msg)
                        await p.execute()

    def run_sync(self):
        redis_conn = redis.from_url(self._redis_url)
        self._active = True
        while self._active:
            response = redis_conn.xreadgroup(**self._xreadgroup_params)
            for stream, messages in response:
                for m_id, payload in messages:
                    result = self.process_message_sync(stream,
                                                       payload,
                                                       redis_conn=redis_conn)
                    with redis_conn.pipeline() as p:
                        for out_stream, out_msg in result.items():
                            p.xadd(out_stream, out_msg)
                        p.execute()


class Splitter(StreamConsumer):
    async def process_message_async(
            self, stream: bytes, message: Dict[bytes, bytes], *,
            redis_conn: aioredis.Redis) -> Dict[str, Dict[str, str]]:
        obj_id = message.pop(OBJECT_ID_FIELD)
        obj_msg_id = await redis_conn.incr(
            f'{INTERNAL_FIELD_PREFIX}id@{obj_id}')
        message_id = f'{obj_id}:{obj_msg_id}'
        return {
            f'{INTERNAL_FIELD_PREFIX}{k}': {
                MSG_ID_FIELD: message_id,
                k: v
            }
            for k, v in message.items()
        }

    def process_message_sync(self, stream, message, *,
                             redis_conn: aioredis.Redis):
        obj_id = message.pop(OBJECT_ID_FIELD)
        obj_msg_id = str(
            redis_conn.incr(f'{INTERNAL_FIELD_PREFIX}id@{obj_id}'))
        message_id = f'{obj_id}:{obj_msg_id}'
        return {
            f'{INTERNAL_FIELD_PREFIX}{k}': {
                MSG_ID_FIELD: message_id,
                k: v
            }
            for k, v in message.items()
        }


class DependencyResolver(StreamConsumer):
    async def process_message_async(
            self, stream: bytes, message: Dict[bytes, bytes], *,
            redis_conn: aioredis.Redis) -> Dict[str, Dict[str, str]]:
        pass

    async def process_message_sync(
            self, stream: bytes, message: Dict[bytes, bytes], *,
            redis_conn: aioredis.Redis) -> Dict[str, Dict[str, str]]:
        pass
