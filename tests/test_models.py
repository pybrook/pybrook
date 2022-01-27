from threading import Thread

from starlette.testclient import TestClient

from tests.conftest import TEST_REDIS_URI


def test_buses_example():
    from pybrook.examples.buses import brook


def test_demo_example(limit_time, redis_sync, mock_processes):
    from pybrook.examples.demo import brook
    brook.redis_url = TEST_REDIS_URI
    t = Thread(target=brook.run)
    try:
        t.start()
        t.join(0.1)
        assert ':ztm-report:split' in redis_sync.keys('*')
        with TestClient(app=brook.app, base_url='https://localhost') as client:
            res = client.post('/ztm-report',
                              json={
                                  "time": "2022-01-06T20:40:25",
                                  "lat": 52.2061306,
                                  "lon": 21.0004175,
                                  "brigade": "2",
                                  "vehicle_number": "1000",
                                  "line": "119"
                              })
            assert res.status_code == 200
        brook.terminate()
        t.join()
        print(redis_sync.execute_command('RG.DUMPREGISTRATIONS'))
        assert redis_sync.xlen(':ztm-report:split') == 1
        assert redis_sync.xlen(':location-report') == 1
    finally:
        t.join()
