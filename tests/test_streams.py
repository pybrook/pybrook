import asyncio
from multiprocessing import Process
from time import sleep

import aioredis
import redis
import pytest

from pybrook.config import OBJECT_ID_FIELD
from pybrook.workers import Splitter

TEST_REDIS_URI = 'redis://localhost/13?decode_responses=1'


@pytest.fixture
@pytest.mark.asyncio
async def redis_async():
    redis_async: aioredis.Redis = await aioredis.from_url(TEST_REDIS_URI, decode_responses=True)
    yield redis_async
    await redis_async.flushdb()
    await redis_async.close()
    await redis_async.connection_pool.disconnect()


@pytest.fixture
def redis_sync():
    redis_sync: redis.Redis = redis.from_url(TEST_REDIS_URI, decode_responses=True)
    yield redis_sync
    redis_sync.flushdb()
    redis_sync.close()
    redis_sync.connection_pool.disconnect()


@pytest.mark.asyncio
async def test_splitter(redis_async: aioredis.Redis):
    splitter = Splitter(group_name='splitter', redis_url=TEST_REDIS_URI, input_streams=['test_input'])
    await splitter.create_groups_async()
    async with redis_async.pipeline() as p:
        for i in range(20000):
            await p.xadd('test_input', {OBJECT_ID_FIELD: 'Vehicle 1', 'a': f'{i}', 'b': f'{i + 1}'})
        await p.execute()
    tasks = []
    for _ in range(32):
        task = asyncio.create_task(splitter.run_async())
        tasks.append(task)

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(asyncio.wait(tasks), timeout=1)
    message = (await redis_async.xread(streams={'@a': '0-0'}, count=1))[0]

    assert (await redis_async.xlen('@a')) == 20000
    assert message[0] == '@a'
    assert message[1][0][1] == {'@_msg_id': 'Vehicle 1:1', 'a': '0'}

    message = (await redis_async.xread(streams={'@b': '0-0'}, count=1))[0]
    assert message[0] == '@b'
    assert message[1][0][1] == {'@_msg_id': 'Vehicle 1:1', 'b': '1'}


def test_splitter_sync(redis_sync: redis.Redis):
    splitter = Splitter(group_name='splitter', redis_url=TEST_REDIS_URI, input_streams=['test_input'])
    splitter.create_groups_sync()
    with redis_sync.pipeline() as p:
        for i in range(60000):
            p.xadd('test_input', {OBJECT_ID_FIELD: 'Vehicle 1', 'a': f'{i}', 'b': f'{i + 1}'})
        p.execute()

    ps = []
    for i in range(16):
        proc = Process(target=splitter.run_sync)
        proc.start()
        ps.append(proc)
    sleep(1)
    for p in ps:
        p.kill()

    assert redis_sync.xlen('@a') == 60000
    message = redis_sync.xread(streams={'@a': '0-0'}, count=1)[0]
    assert message[0] == '@a'
    assert message[1][0][1] == {'@_msg_id': 'Vehicle 1:1', 'a': '0'}

    message = redis_sync.xread(streams={'@b': '0-0'}, count=1)[0]
    assert message[0] == '@b'
    assert message[1][0][1] == {'@_msg_id': 'Vehicle 1:1', 'b': '1'}


