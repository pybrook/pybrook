import multiprocessing
import signal
import threading

import aioredis
import pytest
import redis

from pybrook.consumers.base import BaseStreamConsumer
from pybrook.consumers.worker import WorkerManager


@pytest.fixture
def mock_processes(monkeypatch):
    monkeypatch.setattr(multiprocessing, 'Process', threading.Thread)
    monkeypatch.setattr(signal, 'signal', lambda *args, **kwargs: None)


TEST_REDIS_URI = 'redis://localhost/'


@pytest.fixture
@pytest.mark.asyncio
async def redis_async():
    redis_async: aioredis.Redis = await aioredis.from_url(
        TEST_REDIS_URI, decode_responses=True)
    await redis_async.flushdb()
    yield redis_async
    await redis_async.flushdb()
    await redis_async.close()
    await redis_async.connection_pool.disconnect()


@pytest.fixture
def redis_sync():
    redis_sync: redis.Redis = redis.from_url(TEST_REDIS_URI,
                                             decode_responses=True)
    redis_sync.flushdb()
    yield redis_sync
    redis_sync.close()


@pytest.fixture
def limit_time(monkeypatch):
    from time import time
    t = time()
    state = {'active': True}
    monkeypatch.setattr(
        BaseStreamConsumer, 'active',
        property(fget=lambda s: time() < t + 1 and state['active'],
                 fset=lambda s, v: None))

    def term(*args, **kwargs):
        state['active'] = False

    monkeypatch.setattr(WorkerManager, 'terminate', term)
    yield state
    state['active'] = False
