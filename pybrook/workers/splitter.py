import asyncio
import secrets
from typing import List

import aioredis


class StreamConsumer:
    def __init__(self, group: str, redis: aioredis.Redis):
        self._group = group
        self._redis = redis
        self._active = False

    def get_input_streams(self) -> List[str]:
        raise NotImplementedError

    def spawn(self):
        asyncio.run(self.run())

    async def process_message(self, message):
        raise NotImplementedError

    async def run(self):
        self._active = True
        consumer_name = secrets.token_urlsafe(64)
        for stream in self.get_input_streams():
            try:
                await self._redis.xgroup_create(stream, self._group, mkstream=True)
            except aioredis.ResponseError as e:
                if 'BUSYGROUP' not in str(e):
                    raise e
        while self._active:
            message = await self._redis.xreadgroup(self._group, consumer_name, {s: '>' for s in self.get_input_streams()}, block=0)
            await self.process_message(message)


class Splitter(StreamConsumer):

    def get_input_streams(self) -> List[str]:
        return ['test_reports']

    async def process_message(self, message):
        print(message)