import asyncio
import multiprocessing
from collections import defaultdict
from typing import Any, Callable, Dict, Iterable, List, Mapping, Tuple, Union

from loguru import logger
from redis import Redis

from pybrook.consumers.base import (
    AsyncStreamConsumer,
    BaseStreamConsumer,
    GearsStreamConsumer,
    SyncStreamConsumer,
)

DEFAULT_PROCESSES_NUM = multiprocessing.cpu_count()


class Worker:
    def __init__(self, consumer: Union[SyncStreamConsumer,
                                       AsyncStreamConsumer]):
        self._consumer = consumer

    def run_sync(self, *, processes_num: int = DEFAULT_PROCESSES_NUM):
        return self._spawn_sync(processes_num=processes_num)

    def run_async(self,
                  *,
                  processes_num: int = DEFAULT_PROCESSES_NUM,
                  coroutines_num: int = 8):
        return self._spawn_async(processes_num=processes_num,
                                 coroutines_num=coroutines_num)

    def _spawn_sync(self,
                    processes_num: int) -> Iterable[multiprocessing.Process]:
        return self._spawn(
            target=self._consumer.run_sync,  # type: ignore
            processes_num=processes_num)

    def _async_wrapper(self, coroutines_num: int):
        policy = asyncio.get_event_loop_policy()
        asyncio.set_event_loop(policy.new_event_loop())
        coroutines = [
            self._consumer.run_async()  # type: ignore
            for _ in range(coroutines_num)
        ]
        try:
            asyncio.get_event_loop().run_until_complete(
                asyncio.gather(*coroutines))
        except KeyboardInterrupt:
            pass

    def _spawn_async(self, *, processes_num: int,
                     coroutines_num: int) -> Iterable[multiprocessing.Process]:
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
        self._consumer.register_consumer()

        for _ in range(processes_num):
            proc = multiprocessing.Process(target=target, args=args)
            proc.start()
            processes.append(proc)
        logger.info(
            f'Spawned {processes_num} processes for {type(self._consumer).__name__}'
        )
        return processes


class WorkerManager:
    def __init__(self, consumers: Iterable[BaseStreamConsumer]):
        self.consumers = consumers
        self.processes = []

    def terminate(self):
        for p in self.processes:
            p.terminate()

    def run(self):
        if self.processes:
            raise RuntimeError('Already running!')
        gears_consumers: Mapping[str,
                                 List[GearsStreamConsumer]] = defaultdict(list)
        for c in self.consumers:
            if isinstance(c, GearsStreamConsumer):
                gears_consumers[c._redis_url].append(c)
                continue
            logger.info(f'Spawning worker for {c}...')
            w = Worker(c)
            if isinstance(c, SyncStreamConsumer):
                procs = w.run_sync(processes_num=8)
            elif isinstance(c, AsyncStreamConsumer):
                procs = w.run_async(processes_num=8, coroutines_num=8)
            else:
                raise NotImplementedError(c)
            self.processes.extend(procs)
        for redis_url, consumers in dict(gears_consumers).items():
            redis = Redis.from_url(redis_url,
                                   decode_responses=True,
                                   encoding='utf-8')
            with redis.pipeline() as p:
                p.watch('RG.REGISTERLOCK')
                if p.exists('RG.REGISTERLOCK'):
                    raise RuntimeError(
                        'Try again later, RG registration is locked, possibly by another instance'
                    )
                p.multi()
                p.set('RG.REGISTERLOCK', '1')
                p.expire('RG.REGISTERLOCK', '5')
                p.execute(raise_on_error=True)
            ids = [r[1] for r in redis.execute_command('RG.DUMPREGISTRATIONS')]
            for i in ids:
                redis.execute_command('RG.UNREGISTER', i)
            for c in consumers:
                c.register_builder(redis)
            redis.delete('RG.REGISTERLOCK')
        for proc in self.processes:
            try:
                proc.join()
            except KeyboardInterrupt:
                print('\nShutting down.')
        self.processes = []
