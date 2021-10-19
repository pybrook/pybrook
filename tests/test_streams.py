import asyncio
import multiprocessing
from time import time, sleep
import redis
from pybrook.workers.splitter import StreamConsumer
from loguru import logger

TEST_REDIS_URI = 'redis://localhost/13'


def test_stream_consumer():
    redis_conn = redis.from_url(TEST_REDIS_URI)
    redis_conn.flushdb()

    async def process_message(stream, message):
        return {'test_output': message}

    def insert():
        for i in range(500000):
            redis_conn.xadd('test_input', {'a': 1, 'b': 2, '_obj_id': 5})

    consumer = StreamConsumer(TEST_REDIS_URI, 'test_1', input_streams=['test_input'])

    asyncio.run(consumer.create_groups())
    insert()

    def run_consumer():
        policy = asyncio.get_event_loop_policy()
        policy.set_event_loop(policy.new_event_loop())
        loop = asyncio.get_event_loop()
        consumer.process_message = process_message

        async def spawn():
            tasks = [asyncio.create_task(consumer.run()) for _ in range(16)]
            await asyncio.wait(tasks, loop=policy.get_event_loop())
        loop.run_until_complete(spawn())

    t = time()
    for _ in range(8):
        multiprocessing.Process(target=run_consumer).start()
    sleep(10)
    assert time() - t == 0, redis_conn.xlen('test_output')
