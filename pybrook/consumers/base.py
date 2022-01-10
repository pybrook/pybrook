import asyncio
import secrets
import signal
from enum import Enum
from typing import Dict, Iterable, Set, Tuple, MutableMapping

import aioredis
import redis
from loguru import logger

CONSUMER_NAME_LENGTH = 64


class ConsumerImpl(Enum):
    GEARS = 'GEARS'
    ASYNC = 'ASYNC'
    SYNC = 'SYNC'


class BaseStreamConsumer:
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

    @property
    def supported_impl(self) -> Set[ConsumerImpl]:
        return set()  # pragma: nocover

    def __repr__(self):
        return f'<{self.__class__.__name__} input_streams={self.input_streams}>'

    @property
    def input_streams(self) -> Tuple[str, ...]:
        return tuple(self._input_streams)

    @input_streams.setter
    def input_streams(self, streams: Tuple[str, ...]):
        self._input_streams = tuple(streams)

    def register_consumer(self):
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
                    raise e  # pragma: nocover

    def stop(self, signum=None, frame=None):
        logger.info(f'Closing {self}')
        self.active = False

    @property
    def active(self):
        return self._active

    @active.setter
    def active(self, value: bool):
        self._active = value

    @property
    def _xreadgroup_params(self) -> MutableMapping:
        consumer_name = secrets.token_urlsafe(CONSUMER_NAME_LENGTH)
        return {
            'streams': {s: '>'
                        for s in self.input_streams},
            'groupname': self._consumer_group_name,
            'consumername': consumer_name,
            'count': self._read_chunk_length,
            'block': 1000
        }


class SyncStreamConsumer(BaseStreamConsumer):
    def process_message_sync(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: redis.Redis,
            pipeline: redis.client.Pipeline) -> Dict[str, Dict[str, str]]:
        raise NotImplementedError(  # pragma: nocover
            f'Sync version of process_message for {type(self).__name__} not implemented.'
        )

    @property
    def supported_impl(self) -> Set[ConsumerImpl]:
        return super().supported_impl | {ConsumerImpl.SYNC}

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
                        except redis.WatchError:  # pragma: nocover
                            redis_conn.xack(stream, self._consumer_group_name,
                                            msg_id)
        redis_conn.close()
        redis_conn.connection_pool.disconnect()


class AsyncStreamConsumer(BaseStreamConsumer):
    async def process_message_async(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: aioredis.Redis,
            pipeline: aioredis.client.Pipeline) -> Dict[str, Dict[str, str]]:
        raise NotImplementedError(  # pragma: nocover
            f'Async version of process_message for {type(self).__name__} not implemented.'
        )

    @property
    def supported_impl(self) -> Set[ConsumerImpl]:
        return super().supported_impl | {ConsumerImpl.ASYNC}

    async def run_async(self):  # noqa: WPS217
        signal.signal(signal.SIGTERM, self.stop)
        signal.signal(signal.SIGINT, self.stop)
        redis_conn: aioredis.Redis = await aioredis.from_url(
            self._redis_url, encoding='utf-8', decode_responses=True)
        self.active = True
        xreadgroup_params = self._xreadgroup_params
        while self.active:
            response = await redis_conn.xreadgroup(**xreadgroup_params)
            for stream, messages in response:
                tasks = []
                for msg_id, payload in messages:
                    # TODO: Add a configurable limit
                    tasks.append(asyncio.create_task(
                        self._handle_message(stream, msg_id, payload,
                                             redis_conn)))
                for num, task in enumerate(asyncio.as_completed(tasks)):
                    await task
                    if num > self._read_chunk_length / 2:
                        num = len((await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED))[0])
                        xreadgroup_params['count'] = self._read_chunk_length - len(tasks) + num
                        break
        await redis_conn.close()
        await redis_conn.connection_pool.disconnect()

    async def _handle_message(self, stream: str, msg_id: str,
                              payload: Dict[str,
                                            str], redis_conn: aioredis.Redis):
        async with redis_conn.pipeline() as p:
            result = await self.process_message_async(stream,
                                                      payload,
                                                      redis_conn=redis_conn,
                                                      pipeline=p)
            for out_stream, out_msg in result.items():
                p.xadd(out_stream, out_msg)  # type: ignore
            p.xack(stream, self._consumer_group_name, msg_id)
            try:
                await p.execute()
            except aioredis.WatchError:  # pragma: nocover
                await redis_conn.xack(stream, self._consumer_group_name,
                                      msg_id)


class GearsStreamConsumer(BaseStreamConsumer):
    @property
    def supported_impl(self) -> Set[ConsumerImpl]:
        return super().supported_impl | {ConsumerImpl.GEARS}

    def register_builder(self, pipeline: redis.client.Pipeline):
        raise NotImplementedError  # pragma: nocover
