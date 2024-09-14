import multiprocessing.pool
from itertools import chain
from time import sleep, time

import redis

from pybrook.config import MSG_ID_FIELD
from pybrook.redis_plugin_integration import (
    BrookConfig,
    Dependency,
    DependencyField,
    DependencyResolver,
)
from tests.conftest import TEST_REDIS_URI

conn = None


def init_pool():
    global conn
    conn = redis.from_url(TEST_REDIS_URI, decode_responses=True)


m = multiprocessing.Manager()
msg_id_to_send_time = m.dict()


def xadd(args):
    stream, msg = args
    msg_id_to_send_time[msg[MSG_ID_FIELD]] = time()
    conn.xadd(stream, msg)


def test_resolver(redis_sync: redis.Redis):
    redis_sync.execute_command(
        "PB.SETCONFIG",
        BrookConfig(
            dependency_resolvers={
                "out_dr":
                DependencyResolver(
                    output_stream_key="out",
                    inputs=[
                        Dependency(
                            stream_key="stream_a",
                            fields=[DependencyField(src="x", dst="x_dest")],
                        ),
                        Dependency(
                            stream_key="stream_b",
                            fields=[DependencyField(src="y", dst="y_dest")],
                        ),
                    ],
                )
            }).json(),
    )
    msg_id_to_send_time["\"1:0\""] = time()
    redis_sync.xadd("stream_a", {MSG_ID_FIELD: '"1:0"', "x": "1"})
    redis_sync.xadd("stream_b", {MSG_ID_FIELD: '"1:0"', "y": "2"})

    REQUESTS = 50000
    args = list(
        chain(*((("stream_a", {
            MSG_ID_FIELD: f'"1:{x}"',
            "x": "1"
        }), ("stream_b", {
            MSG_ID_FIELD: f'"1:{x}"',
            "y": "2"
        })) for x in range(1, REQUESTS))))
    assert [m for _, m in redis_sync.xrevrange("out", "+", "-")] == [{
        "@pb@msg_id":
        '"1:0"',
        "x_dest":
        "1",
        "y_dest":
        "2"
    }]

    with multiprocessing.Pool(processes=16, initializer=init_pool) as pool:
        t = time()
        pool.map(xadd, args)
    assert redis_sync.xlen("out") == REQUESTS
    end_time = time()
    print("RPS achieved: ", REQUESTS / (end_time - t))
    print(
        "Latency: ",
        sum(
            int(tr.split("-")[0])
            - msg_id_to_send_time[msg[MSG_ID_FIELD]] * 1000
            for tr, msg in redis_sync.xrange("out", "-", "+")) / REQUESTS)
