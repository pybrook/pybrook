import asyncio
import multiprocessing
from typing import Any, Callable, Iterable, Tuple

from loguru import logger

from pybrook.consumers import Splitter
from pybrook.consumers.base import StreamConsumer

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

    def _spawn_sync(self,
                    processes_num: int) -> Iterable[multiprocessing.Process]:
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
        self._consumer.create_groups_sync()

        for _ in range(processes_num):
            proc = multiprocessing.Process(target=target, args=args)
            proc.start()
            processes.append(proc)
        logger.info(
            f'Spawned {processes_num} processes for {type(self._consumer).__name__}'
        )
        return processes


class WorkerManager:
    def __init__(self, consumers: Iterable[StreamConsumer]):
        self.consumers = consumers

    def run(self):
        processes = []
        for c in self.consumers:
            logger.info(f'Spawning worker for {c}...')
            w = Worker(c)
            if not c.use_async:
                procs = w.run_sync(
                    processes_num=4 if isinstance(c, Splitter) else 8)
            else:
                procs = w.run_async(processes_num=8, coroutines_num=8)
            processes.extend(procs)
        for p in processes:
            p.join()
