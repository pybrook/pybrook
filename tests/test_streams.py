import asyncio
import multiprocessing
import random
import signal
import threading
from multiprocessing import Process
from time import sleep, time
from typing import Dict, List

import aioredis
import pytest
import redis

from pybrook.config import OBJECT_ID_FIELD, MSG_ID_FIELD
from pybrook.workers import Splitter
from pybrook.workers.splitter import DependencyResolver, StreamConsumer, Worker

TEST_REDIS_URI = 'redis://localhost/13?decode_responses=1'


@pytest.fixture(autouse=True)
def replace_process_with_thread(monkeypatch):
    monkeypatch.setattr(multiprocessing, 'Process', threading.Thread)
    from time import time
    t = time()
    monkeypatch.setattr(
        StreamConsumer, 'active',
        property(fget=lambda s: time() < t + 1, fset=lambda s, v: None))
    monkeypatch.setattr(signal, 'signal', lambda *args: None)


@pytest.fixture
@pytest.mark.asyncio
async def redis_async():
    redis_async: aioredis.Redis = await aioredis.from_url(
        TEST_REDIS_URI, decode_responses=True)
    yield redis_async
    await redis_async.flushdb()
    await redis_async.close()
    await redis_async.connection_pool.disconnect()


@pytest.fixture
def redis_sync():
    redis_sync: redis.Redis = redis.from_url(TEST_REDIS_URI,
                                             decode_responses=True)
    yield redis_sync
    redis_sync.flushdb()
    redis_sync.close()


@pytest.fixture()
def test_input_perf(redis_sync) -> List[Dict[str, str]]:
    data = []
    with redis_sync.pipeline() as p:
        for i in range(3000):
            item = {OBJECT_ID_FIELD: 'Vehicle 1', 'a': f'{i}', 'b': f'{i + 1}'}
            p.xadd('test_input', item)
            data.append(item)
        p.execute()
    return data


@pytest.fixture()
def test_input(redis_sync) -> List[Dict[str, str]]:
    data = []
    with redis_sync.pipeline() as p:
        for i in range(10):
            item = {OBJECT_ID_FIELD: 'Vehicle 1', 'a': f'{i}', 'b': f'{i + 1}'}
            p.xadd('test_input', item)
            data.append(item)
        p.execute()
    return data


@pytest.fixture()
def test_dependency(redis_sync) -> List[Dict[str, str]]:
    with redis_sync.pipeline() as p:
        for i in range(10):
            p.xadd('@a', {'@_msg_id': f'Vehicle 1:{i}', 'a': str(i)})
            p.xadd('@b', {'@_msg_id': f'Vehicle 1:{i}', 'b': str(i)})
        p.execute()


@pytest.mark.parametrize('mode', ('sync', 'async'))
def test_worker(test_input, redis_sync, mode):
    messages = []
    random.seed(16)

    class TestConsumer(StreamConsumer):
        def process_message_sync(
                self, stream_name: bytes, message: Dict[str, str], *,
                redis_conn: aioredis.Redis) -> Dict[str, Dict[str, str]]:
            # simulate out of order execution
            sleep(random.choice([0, 0.5]))
            messages.append(message)
            return {}

        async def process_message_async(
                self, stream_name: bytes, message: Dict[str, str], *,
                redis_conn: aioredis.Redis) -> Dict[str, Dict[str, str]]:
            # simulate out of order execution
            await asyncio.sleep(random.choice([0, 0.5]))
            messages.append(message)
            return {}

    worker = Worker(
        TestConsumer(input_streams=['test_input'],
                     read_chunk_length=1,
                     consumer_group_name='test_consumer',
                     redis_url=TEST_REDIS_URI))
    if mode == 'async':
        processes = worker.run_async()
    elif mode == 'sync':
        processes = worker.run_sync()
    else:
        raise NotImplementedError

    for p in processes:
        p.join()
    messages = sorted(messages, key=lambda o: o['a'])
    assert messages == test_input


@pytest.mark.asyncio
async def test_splitter_async(redis_async: aioredis.Redis, test_input):
    splitter = Splitter(consumer_group_name='splitter',
                        redis_url=TEST_REDIS_URI,
                        input_streams=['test_input'])
    await splitter.create_groups_async()
    tasks = []
    for _ in range(8):
        task = asyncio.create_task(splitter.run_async())
        tasks.append(task)

    await asyncio.wait(tasks)
    message = (await redis_async.xread(streams={'@a': '0-0'}, count=1))[0]

    assert len(test_input) == 10
    assert (await redis_async.xlen('@a')) == len(test_input)
    assert message[0] == '@a'
    assert message[1][0][1] == {'@_msg_id': 'Vehicle 1:1', 'a': '0'}

    message = (await redis_async.xread(streams={'@b': '0-0'}, count=1))[0]
    assert message[0] == '@b'
    assert message[1][0][1] == {'@_msg_id': 'Vehicle 1:1', 'b': '1'}


def test_splitter_sync(redis_sync: redis.Redis, test_input):
    splitter = Splitter(consumer_group_name='splitter',
                        redis_url=TEST_REDIS_URI,
                        input_streams=['test_input'])
    splitter.create_groups_sync()

    ps = []
    for i in range(16):
        proc = Process(target=splitter.run_sync)
        ps.append(proc)
        proc.start()
    for p in ps:
        p.join()

    assert redis_sync.xlen('@a') == len(test_input)
    message = redis_sync.xread(streams={'@a': '0-0'}, count=1)[0]
    assert message[0] == '@a'
    assert message[1][0][1] == {'@_msg_id': 'Vehicle 1:1', 'a': '0'}

    message = redis_sync.xread(streams={'@b': '0-0'}, count=1)[0]
    assert message[0] == '@b'
    assert message[1][0][1] == {'@_msg_id': 'Vehicle 1:1', 'b': '1'}


def test_dependency_resolver_sync(redis_sync: redis.Redis, test_dependency):
    resolver = DependencyResolver(resolver_name='ab_resolver',
                                  dependency_names=['a', 'b'],
                                  redis_url=TEST_REDIS_URI)
    resolver.create_groups_sync()
    ps = []
    for i in range(16):
        proc = Process(target=resolver.run_sync)
        ps.append(proc)
        proc.start()

    for p in ps:
        p.join()

    assert redis_sync.xlen(resolver.output_stream_key) == 10
    out_data = redis_sync.xread({resolver.output_stream_key: '0'})[0][1]
    for _, message in out_data:
        assert message[MSG_ID_FIELD][-1] == message['a'] == message['b']


def test_perf(test_input_perf, redis_sync):
    splitter = Splitter(consumer_group_name='splitter',
                        redis_url=TEST_REDIS_URI,
                        read_chunk_length=100,
                        input_streams=['test_input'])
    resolver = DependencyResolver(resolver_name='ab_resolver',
                                  dependency_names=['a', 'b'],
                                  read_chunk_length=100,
                                  redis_url=TEST_REDIS_URI)

    t = time()
    splitter_procs = Worker(splitter).run_sync(processes_num=4)
    resolver_procs = Worker(resolver).run_sync(processes_num=4)
    for p in splitter_procs + resolver_procs:
        p.join()
    print(time() - t)
    assert [redis_sync.xlen('@a'),
            redis_sync.xlen('@b'),
            redis_sync.xlen(resolver.output_stream_key)] == [3000] * 3