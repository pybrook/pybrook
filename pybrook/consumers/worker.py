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

import asyncio
import dataclasses
import multiprocessing
import signal
from typing import Any, Callable, Dict, Iterable, List, Set, Tuple

import redis
import uvloop
from loguru import logger

from pybrook.config import DEFAULT_WORKERS
from pybrook.consumers.base import (
    AsyncStreamConsumer,
    BaseStreamConsumer,
    GearsStreamConsumer,
    SyncStreamConsumer,
)

DEFAULT_PROCESSES_NUM = multiprocessing.cpu_count()


class Worker:
    def __init__(self, consumer: BaseStreamConsumer):
        self._consumer = consumer

    def run(self, *, processes_num: int = DEFAULT_PROCESSES_NUM):
        if isinstance(self._consumer, SyncStreamConsumer):
            return self._spawn_sync(processes_num=processes_num)
        elif isinstance(self._consumer, AsyncStreamConsumer):
            return self._spawn_async(processes_num=processes_num)
        raise NotImplementedError(self._consumer)

    def _spawn_sync(self,
                    processes_num: int) -> Iterable[multiprocessing.Process]:
        return self._spawn(
            target=self._consumer.run_sync,  # type: ignore
            processes_num=processes_num)

    def _async_wrapper(self):
        policy = uvloop.EventLoopPolicy()
        asyncio.set_event_loop_policy(policy)
        asyncio.set_event_loop(policy.new_event_loop())
        try:
            asyncio.get_event_loop().run_until_complete(
                self._consumer.run_async())
        except KeyboardInterrupt:
            ...
        except asyncio.CancelledError:
            ...  # This is fine, shouldn't break anything

    def _spawn_async(self, *,
                     processes_num: int) -> Iterable[multiprocessing.Process]:
        return self._spawn(target=self._async_wrapper,
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


@dataclasses.dataclass
class ConsumerConfig:
    workers: int = DEFAULT_WORKERS


class WorkerManager:
    def __init__(self,
                 consumers: Iterable[BaseStreamConsumer],
                 config: Dict[str, ConsumerConfig] = None,
                 enable_gears: bool = True):
        self.consumers = consumers
        self.config = config or {}
        self.redis_urls: Set[str] = {c.redis_url for c in consumers}
        self.gears_consumers: List[GearsStreamConsumer] = [
            c for c in consumers if enable_gears and isinstance(c, GearsStreamConsumer)
        ]
        self.regular_consumers: List[BaseStreamConsumer] = [
            c for c in consumers if not enable_gears or not isinstance(c, GearsStreamConsumer)
        ]
        self.processes: List[multiprocessing.Process] = []
        self._kill_on_terminate = False

    def terminate(self):
        if self._kill_on_terminate:
            for p in self.processes:
                p.kill()
            return
        for p in self.processes:  # noqa: WPS440
            p.terminate()
        self._kill_on_terminate = True

    def run(self):
        if self.processes:
            raise RuntimeError('Already running!')
        signal.signal(signal.SIGINT, lambda *args: self.terminate)
        signal.signal(signal.SIGTERM, lambda *args: self.terminate)

        self.spawn_workers()

        for redis_url in self.redis_urls:
            redis_conn = redis.from_url(redis_url,
                                        decode_responses=True,
                                        encoding='utf-8')
            self.acquire_gears_registration_lock(redis_conn)
            ids = [
                r[1]
                for r in redis_conn.execute_command('RG.DUMPREGISTRATIONS')
            ]
            for i in ids:
                redis_conn.execute_command('RG.UNREGISTER', i)
            for consumer in self.gears_consumers:
                consumer.register_builder(redis_conn)
            redis_conn.delete('RG.REGISTERLOCK')
        for proc in self.processes:
            try:
                proc.join()
            except KeyboardInterrupt:
                ...
        self.processes = []

    def spawn_workers(self):
        for c in self.regular_consumers:
            consumer_config = self.config.get(c.consumer_group_name,
                                              ConsumerConfig())
            logger.info(f'Spawning worker for {c}...')
            w = Worker(c)
            procs = w.run(processes_num=consumer_config.workers)
            self.processes.extend(procs)

    def acquire_gears_registration_lock(self, redis_conn: redis.Redis):
        with redis_conn.pipeline() as p:
            p.watch('RG.REGISTERLOCK')
            if p.exists('RG.REGISTERLOCK'):
                raise RuntimeError(
                    'Try again later, RG registration is locked, possibly by another instance'
                )
            p.multi()
            p.set('RG.REGISTERLOCK', '1')
            p.expire('RG.REGISTERLOCK', 5)
            p.execute(raise_on_error=True)
