import secrets
import signal
from typing import Dict, Iterable, Mapping, Tuple

import aioredis
import redis
from loguru import logger

CONSUMER_NAME_LENGTH = 64


class StreamConsumer:
    def __init__(self,
                 *,
                 redis_url: str,
                 consumer_group_name: str,
                 input_streams: Iterable[str],
                 read_chunk_length: int = 1):
        self._consumer_group_name = consumer_group_name
        self._redis_url = redis_url
        self._active = False
        self._read_chunk_length = read_chunk_length
        self.input_streams = tuple(input_streams)

    def __repr__(self):
        return f'<{self.__class__.__name__} input_streams={self.input_streams}>'

    @property
    def input_streams(self) -> Tuple[str, ...]:
        return tuple(self._input_streams)

    @input_streams.setter
    def input_streams(self, streams: Tuple[str, ...]):
        self._input_streams = tuple(streams)

    async def process_message_async(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: aioredis.Redis,
            pipeline: aioredis.client.Pipeline) -> Dict[str, Dict[str, str]]:
        raise NotImplementedError(
            f'Async version of process_message for {type(self).__name__} not implemented.'
        )

    def process_message_sync(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: redis.Redis,
            pipeline: redis.client.Pipeline) -> Dict[str, Dict[str, str]]:
        raise NotImplementedError(
            f'Sync version of process_message for {type(self).__name__} not implemented.'
        )

    async def create_groups_async(self):
        redis_conn: aioredis.Redis = await aioredis.from_url(
            self._redis_url, encoding='utf-8', decode_responses=True)
        for stream in self.input_streams:
            try:
                await redis_conn.xgroup_create(stream,
                                               self._consumer_group_name,
                                               id=0,
                                               mkstream=True)
            except aioredis.ResponseError as e:
                if 'BUSYGROUP' not in str(e):
                    raise e

    def create_groups_sync(self):
        redis_conn = redis.from_url(self._redis_url,
                                    encoding='utf-8',
                                    decode_responses=True)
        for stream in self.input_streams:
            try:
                redis_conn.xgroup_create(stream,
                                         self._consumer_group_name,
                                         id=0,
                                         mkstream=True)
            except redis.ResponseError as e:
                if 'BUSYGROUP' not in str(e):
                    raise e

    def stop(self, signum=None, frame=None):
        self._active = False

    @property
    def active(self):
        return self._active

    @active.setter
    def active(self, value: bool):
        self._active = value

    async def run_async(self):  # noqa: WPS217
        signal.signal(signal.SIGTERM, self.stop)
        redis_conn: aioredis.Redis = await aioredis.from_url(
            self._redis_url, encoding='utf-8', decode_responses=True)
        self.active = True
        xreadgroup_params = self._xreadgroup_params
        while self.active:
            response = await redis_conn.xreadgroup(**xreadgroup_params)
            for stream, messages in response:
                for msg_id, payload in messages:
                    async with redis_conn.pipeline() as p:
                        result = await self.process_message_async(
                            stream, payload, redis_conn=redis_conn, pipeline=p)
                        for out_stream, out_msg in result.items():
                            p.xadd(out_stream, out_msg)
                        p.xack(stream, self._consumer_group_name, msg_id)
                        try:
                            await p.execute()
                        except aioredis.WatchError:
                            await redis_conn.xack(stream,
                                                  self._consumer_group_name,
                                                  msg_id)
        await redis_conn.close()
        await redis_conn.connection_pool.disconnect()

    def run_sync(self):
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)
        redis_conn: redis.Redis = redis.from_url(self._redis_url,
                                                 encoding='utf-8',
                                                 decode_responses=True)
        self._active = True
        xreadgroup_params = self._xreadgroup_params
        while self.active:
            response = redis_conn.xreadgroup(**xreadgroup_params)
            for stream, messages in response:
                for msg_id, payload in messages:
                    with redis_conn.pipeline() as p:
                        result = self.process_message_sync(
                            stream, payload, redis_conn=redis_conn, pipeline=p)
                        for out_stream, out_msg in result.items():
                            p.xadd(out_stream, out_msg)
                        p.xack(stream, self._consumer_group_name, msg_id)
                        try:
                            p.execute()
                        except redis.WatchError:
                            redis_conn.xack(stream, self._consumer_group_name,
                                            msg_id)
        redis_conn.close()
        redis_conn.connection_pool.disconnect()

    @property
    def _xreadgroup_params(self) -> Mapping:
        consumer_name = secrets.token_urlsafe(CONSUMER_NAME_LENGTH)
        return {
            'streams': {s: '>'
                        for s in self.input_streams},
            'groupname': self._consumer_group_name,
            'consumername': consumer_name,
            'count': self._read_chunk_length,
            'block': 100
        }
