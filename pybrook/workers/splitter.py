import asyncio
import secrets
from typing import List, Iterable, Tuple, Dict

import aioredis
from loguru import logger


class StreamConsumer:
    def __init__(self, redis_url: str, group: str, input_streams: Iterable[str]):
        self._group = group
        self._redis_url = redis_url
        self._active = False
        self._groups_created = False
        self.input_streams = input_streams

    @property
    def input_streams(self) -> Tuple[str]:
        return self._input_streams

    @input_streams.setter
    def input_streams(self, streams: Iterable[str]):
        self._input_streams = tuple(streams)
        self._groups_created = False

    async def process_message(self, stream, message) -> Dict[str, Dict[str, str]]:
        raise NotImplementedError

    async def create_groups(self):
        redis = await aioredis.from_url(self._redis_url)
        for stream in self.input_streams:
            try:
                await redis.xgroup_create(stream, self._group, mkstream=True)
            except aioredis.ResponseError as e:
                if 'BUSYGROUP' not in str(e):
                    raise e
        self._groups_created = True

    async def run(self):
        redis: aioredis.Redis = await aioredis.from_url(self._redis_url)
        if not self._groups_created:
            await self.create_groups()
        self._active = True
        consumer_name = secrets.token_urlsafe(64)
        while self._active:
            response = await redis.xreadgroup(self._group, consumer_name, {s: '>' for s in self.input_streams},
                                              count=100,
                                              block=10)
            for stream, messages in response:
                logger.error(messages)
                for m_id, payload in messages:
                    result = await self.process_message(stream, payload)
                    for out_stream, out_msg in result.items():
                        logger.error('write')
                        await redis.xadd(out_stream, out_msg)


class Splitter(StreamConsumer):

    async def process_message(self, stream, message):
        print(message)
