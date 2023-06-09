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

import asyncio
import multiprocessing
import random
import signal
import threading
from time import sleep
from typing import Dict, List

import redis.asyncio as aioredis
import pytest
import redis
from loguru import logger

from pybrook.config import MSG_ID_FIELD
from pybrook.consumers.base import AsyncStreamConsumer, ConsumerImpl, SyncStreamConsumer
from pybrook.consumers.dependency_resolver import DependencyResolver
from pybrook.consumers.splitter import AsyncSplitter, Splitter, SyncSplitter
from pybrook.consumers.worker import Worker
from pybrook.encoding import decode_value
from tests.conftest import TEST_REDIS_URI


def write_test_reports(redis_sync: redis.Redis,
                       num: int) -> List[Dict[str, str]]:
    """Generates input reports for testing."""
    data = []
    with redis_sync.pipeline() as p:
        for i in range(num):
            item = {'vehicle_id': '"Vehicle 1"', 'a': f'{i}', 'b': f'{i + 1}'}
            p.xadd('test_input', item)
            data.append(item)
        p.execute()
    return data


@pytest.fixture()
def test_input_perf(redis_sync) -> List[Dict[str, str]]:
    return write_test_reports(redis_sync, 100000)


@pytest.fixture()
def test_input(redis_sync) -> List[Dict[str, str]]:
    return write_test_reports(redis_sync, 10)


@pytest.fixture()
def test_dependency(redis_sync) -> List[Dict[str, str]]:
    with redis_sync.pipeline() as p:
        for i in range(100):
            p.xadd(':a', {':_msg_id': f'"Vehicle 1:{i}"', 'a': str(i)})
            p.xadd(':b', {':_msg_id': f'"Vehicle 1:{i}"', 'b': str(i)})
        p.execute()


@pytest.mark.parametrize('mode', ('sync', 'async'))
def test_worker(test_input, redis_sync, mode, limit_time, mock_processes):
    messages = multiprocessing.Manager().list()
    random.seed(16)

    class TestConsumer(SyncStreamConsumer, AsyncStreamConsumer):
        def process_message_sync(
                self, stream_name: bytes, message: Dict[str, str], *,
                redis_conn: aioredis.Redis,
                pipeline: redis.client.Pipeline) -> Dict[str, Dict[str, str]]:
            # simulate out of order execution
            sleep(random.choice([0, 0.5]))
            messages.append(message)
            return {}

        async def process_message_async(
                self, stream_name: bytes, message: Dict[str, str], *,
                redis_conn: aioredis.Redis, pipeline: aioredis.client.Pipeline
        ) -> Dict[str, Dict[str, str]]:
            # simulate out of order execution
            await asyncio.sleep(random.choice([0, 0.5]))
            messages.append(message)
            return {}

    worker = Worker(
        TestConsumer(input_streams=['test_input'],
                     read_chunk_length=1,
                     read_messages_since=0,
                     consumer_group_name='test_consumer',
                     redis_url=TEST_REDIS_URI))
    processes = worker.run()

    for p in processes:
        p.join()
    messages = sorted(messages, key=lambda o: o['a'])
    assert messages == test_input


@pytest.mark.asyncio
async def test_splitter_async(redis_async: aioredis.Redis, test_input,
                              mock_processes, limit_time):
    splitter = AsyncSplitter(consumer_group_name='splitter',
                             object_id_field='vehicle_id',
                             namespace='test',
                             read_messages_since='0',
                             redis_url=TEST_REDIS_URI,
                             input_streams=['test_input'])
    splitter.register_consumer()
    tasks = []
    for _ in range(8):
        task = asyncio.create_task(splitter.run_async())
        tasks.append(task)

    await asyncio.wait(tasks)
    message = (await redis_async.xread(streams={':test:split': '0-0'},
                                       count=1))[0]

    assert (await redis_async.xlen(':test:split')) == len(test_input)
    assert message[0] == ':test:split'
    assert message[1][0][1] == {
        ':_msg_id': '"Vehicle 1:1"',
        'a': '0',
        'b': '1',
        'vehicle_id': '"Vehicle 1"'
    }


def test_splitter_sync(redis_sync: redis.Redis, test_input, limit_time,
                       mock_processes):
    splitter = SyncSplitter(consumer_group_name='splitter',
                            namespace='test',
                            read_messages_since=0,
                            object_id_field='vehicle_id',
                            redis_url=TEST_REDIS_URI,
                            input_streams=['test_input'])
    splitter.register_consumer()

    ps = []
    for i in range(16):
        proc = threading.Thread(target=splitter.run_sync)
        proc.start()
        ps.append(proc)
    for p in ps:
        p.join()

    assert redis_sync.xlen(':test:split') == len(test_input)
    message = redis_sync.xread(streams={':test:split': '0-0'}, count=1)[0]
    assert message[0] == ':test:split'
    assert message[1][0][1] == {
        ':_msg_id': '"Vehicle 1:1"',
        'a': '0',
        'b': '1',
        'vehicle_id': '"Vehicle 1"'
    }


def test_dependency_resolver_sync(redis_sync: redis.Redis, test_dependency,
                                  limit_time, mock_processes):
    resolver = DependencyResolver(resolver_name='ab_resolver',
                                  dependencies=[
                                      DependencyResolver.Dep(src_stream=':a',
                                                             src_key='a',
                                                             dst_key='a'),
                                      DependencyResolver.Dep(src_stream=':b',
                                                             src_key='b',
                                                             dst_key='b'),
                                  ],
                                  read_messages_since=0,
                                  redis_url=TEST_REDIS_URI)
    resolver.register_consumer()
    ps = []
    for i in range(16):
        proc = threading.Thread(target=resolver.run_sync)
        ps.append(proc)
        proc.start()

    for p in ps:
        p.join()

    assert redis_sync.xlen(resolver.output_stream_name) == 100
    out_data = redis_sync.xread({resolver.output_stream_name: '0'})[0][1]
    for _, message in out_data:
        assert decode_value(message[MSG_ID_FIELD]).split(
            ':')[-1] == message['a'] == message['b']


def test_perf(test_input_perf, redis_sync):
    splitter = Splitter(consumer_group_name='splitter',
                        redis_url=TEST_REDIS_URI,
                        object_id_field='vehicle_id',
                        namespace='test_perf',
                        read_chunk_length=10,
                        read_messages_since=0,
                        input_streams=['test_input'])
    repr(splitter)
    resolver = DependencyResolver(
        resolver_name='ab_resolver',
        dependencies=[
            DependencyResolver.Dep(src_stream=':test_perf:split',
                                   src_key='a',
                                   dst_key='a'),
            DependencyResolver.Dep(src_stream=':test_perf:split',
                                   src_key='b',
                                   dst_key='b'),
        ],
        read_chunk_length=10,
        redis_url=TEST_REDIS_URI)
    repr(resolver)
    splitter_procs = Worker(splitter).run(processes_num=3)
    resolver_procs = Worker(resolver).run(processes_num=8)
    splitter.register_consumer()  # should do nothing
    assert splitter.supported_impl == {
        ConsumerImpl.GEARS, ConsumerImpl.ASYNC, ConsumerImpl.SYNC
    }
    sleep(4)
    for p in splitter_procs + resolver_procs:
        p.terminate()

    msgids = set()
    assert redis_sync.xlen(resolver.output_stream_name) != 0
    for m_id, m_payload in (redis_sync.xread(
        {resolver.output_stream_name: '0-0'}))[0][1]:
        assert m_payload[':_msg_id'] not in msgids, 'Race condition check'
        msgids.add(m_payload[':_msg_id'])
    logger.warning([
        redis_sync.xlen(':test_perf:split'),
        redis_sync.xlen(resolver.output_stream_name)
    ])
