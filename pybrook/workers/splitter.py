import asyncio
import multiprocessing
import secrets
import signal
from typing import Any, Callable, Dict, Iterable, Mapping, Tuple

import aioredis
import redis
from loguru import logger

from pybrook.config import INTERNAL_FIELD_PREFIX, MSG_ID_FIELD, OBJECT_ID_FIELD

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
        self.consumer_name = secrets.token_urlsafe(CONSUMER_NAME_LENGTH)

    @property
    def input_streams(self) -> Tuple[str, ...]:
        return tuple(self._input_streams)

    @input_streams.setter
    def input_streams(self, streams: Tuple[str, ...]):
        self._input_streams = tuple(streams)

    async def process_message_async(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: aioredis.Redis) -> Dict[str, Dict[str, str]]:
        raise NotImplementedError(
            f'Async version of process_message for {type(self).__name__} not implemented.'
        )

    def process_message_sync(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: aioredis.Redis) -> Dict[str, Dict[str, str]]:
        raise NotImplementedError(
            f'Sync version of process_message for {type(self).__name__} not implemented.'
        )

    async def create_groups_async(self):
        redis_conn: aioredis.Redis = await aioredis.from_url(self._redis_url)
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
        redis_conn = redis.from_url(self._redis_url)
        for stream in self.input_streams:
            try:
                redis_conn.xgroup_create(stream,
                                         self._consumer_group_name,
                                         id=0,
                                         mkstream=True)
            except redis.ResponseError as e:
                if 'BUSYGROUP' not in str(e):
                    raise e

    def stop(self, signum, frame):
        self._active = False

    @property
    def active(self):
        return self._active

    @active.setter
    def active(self, value: bool):
        self._active = value

    async def run_async(self):  # noqa: WPS217
        signal.signal(signal.SIGTERM, self.stop)
        redis_conn: aioredis.Redis = await aioredis.from_url(self._redis_url)
        self.active = True
        while self.active:
            response = await redis_conn.xreadgroup(**self._xreadgroup_params)
            for stream, messages in response:
                for _, payload in messages:
                    result = await self.process_message_async(
                        stream, payload, redis_conn=redis_conn)
                    async with redis_conn.pipeline() as p:
                        for out_stream, out_msg in result.items():
                            await p.xadd(out_stream, out_msg)
                        await p.execute()
        await redis_conn.close()
        await redis_conn.connection_pool.disconnect()

    def run_sync(self):
        signal.signal(signal.SIGTERM, self.stop)
        redis_conn: redis.Redis = redis.from_url(self._redis_url)
        self._active = True
        while self.active:
            response = redis_conn.xreadgroup(**self._xreadgroup_params)
            for stream, messages in response:
                for _, payload in messages:
                    result = self.process_message_sync(stream,
                                                       payload,
                                                       redis_conn=redis_conn)
                    with redis_conn.pipeline() as p:
                        for out_stream, out_msg in result.items():
                            p.xadd(out_stream, out_msg)
                        p.execute()
        redis_conn.close()
        redis_conn.connection_pool.disconnect()

    @property
    def _xreadgroup_params(self) -> Mapping:
        return {
            'streams': {s: '>'
                        for s in self.input_streams},
            'groupname': self._consumer_group_name,
            'consumername': self.consumer_name,
            'count': self._read_chunk_length,
            'block': 10
        }


class Splitter(StreamConsumer):
    async def process_message_async(
            self, stream_name: str, message: Dict[str, str], *,
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

    def process_message_sync(self, stream_name, message, *,
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
    def __init__(self,
                 *,
                 redis_url: str,
                 resolver_name: str,
                 dependency_names: Iterable[str],
                 read_chunk_length: int = 1):
        self._dependency_names = tuple(set(dependency_names))
        self._num_dependencies = len(dependency_names)
        self._resolver_name = resolver_name
        input_streams = tuple(f'{INTERNAL_FIELD_PREFIX}{name}'
                              for name in self._dependency_names)
        super().__init__(redis_url=redis_url,
                         consumer_group_name=resolver_name,
                         input_streams=input_streams,
                         read_chunk_length=read_chunk_length)

    @property
    def output_stream_key(self):
        return f'{INTERNAL_FIELD_PREFIX}{self._resolver_name}_deps'

    def dependency_map_key(self, message_id: str):
        return f'{self.output_stream_key}{INTERNAL_FIELD_PREFIX}{message_id}'

    async def process_message_async(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: aioredis.Redis) -> Dict[str, Dict[str, str]]:
        ...

    def process_message_sync(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: aioredis.Redis) -> Dict[str, Dict[str, str]]:
        field_name = stream_name[1:]
        field_value = message[field_name]
        message_id = message[MSG_ID_FIELD]
        dep_key = self.dependency_map_key(message_id)
        redis_conn.hset(dep_key, field_name, field_value)
        if redis_conn.hlen(dep_key) == self._num_dependencies:
            return {
                self.output_stream_key: {
                    MSG_ID_FIELD: message_id,
                    **redis_conn.hgetall(dep_key)
                }
            }
        return {}


DEFAULT_PROCESSES_NUM = multiprocessing.cpu_count()


class Worker:
    def __init__(self, consumer: StreamConsumer):
        self._consumer = consumer

    def run_sync(self, *, processes_num: int = DEFAULT_PROCESSES_NUM):
        return self._spawn_sync(processes_num=processes_num)

    def run_async(self,
                  *,
                  processes_num: int = DEFAULT_PROCESSES_NUM,
                  coroutines_num: int = 8):
        return self._spawn_async(processes_num=processes_num,
                                 coroutines_num=coroutines_num)

    def _spawn_sync(self, processes_num: int):
        return self._spawn(target=self._consumer.run_sync,
                           processes_num=processes_num)

    def _async_wrapper(self, coroutines_num: int):
        policy = asyncio.get_event_loop_policy()
        asyncio.set_event_loop(policy.new_event_loop())
        coroutines = [
            self._consumer.run_async() for _ in range(coroutines_num)
        ]
        asyncio.get_event_loop().run_until_complete(
            asyncio.gather(*coroutines))

    def _spawn_async(self, *, processes_num: int, coroutines_num: int):
        return self._spawn(target=self._async_wrapper,
                           args=(coroutines_num, ),
                           processes_num=processes_num)

    def _spawn(
            self,
            *,
            target: Callable,
            processes_num: int,
            args: Tuple[Any, ...] = (),
    ) -> Iterable[multiprocessing.Process]:
        processes = []
        self._consumer.create_groups_sync()

        for _ in range(processes_num):
            proc = multiprocessing.Process(target=target, args=args)
            proc.start()
            processes.append(proc)
        logger.info(
            f'Spawned {processes_num} processes for {type(self._consumer).__name__}'
        )
        return processes
