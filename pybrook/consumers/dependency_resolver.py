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

import dataclasses
from itertools import chain
from typing import Dict, List

import redis

from pybrook.config import MSG_ID_FIELD, SPECIAL_CHAR
from pybrook.consumers.base import SyncStreamConsumer
from pybrook.encoding import decode_value, encode_value


class DependencyResolver(SyncStreamConsumer):
    @dataclasses.dataclass
    class Dep:  # noqa: WPS431
        src_stream: str
        src_key: str
        dst_key: str

    @dataclasses.dataclass
    class HistoricalDep:  # noqa: WPS431
        src_stream: str
        src_key: str
        dst_key: str
        history_length: int = 1

    def __init__(self,
                 *,
                 redis_url: str,
                 resolver_name: str,
                 output_stream_name: str = None,
                 dependencies: List[Dep],
                 historical_dependencies: List[HistoricalDep] = None,
                 read_chunk_length: int = 100,
                 **kwargs):
        """

        Args:
            redis_url: Redis server URL
            resolver_name: Name of the resolver, used for the consumer group
            output_stream_name: name of the output stream
            historical_dependencies:
                A list of historical dependencies to load when all regular dependencies are available.
            dependencies:
                Keys are source streams, values are dependency names used as keys in the output stream.
                Source streams should contain just the internal msgid and values
                for the specific field.
            read_chunk_length: Redis XACK COUNT arg
            kwargs: other arguments that will be passed to SyncStreamConsumer.__init__
        """
        self._dependencies: List[DependencyResolver.Dep] = dependencies
        self._historical_dependencies: List[
            DependencyResolver.HistoricalDep] = historical_dependencies or []
        self._num_dependencies = len(dependencies)
        consumer_group_name = f'{resolver_name}{SPECIAL_CHAR}dr'
        if not output_stream_name:
            output_stream_name = f'{SPECIAL_CHAR}{consumer_group_name}{SPECIAL_CHAR}deps'
        self.output_stream_name: str = output_stream_name
        input_streams = list({
            s.src_stream  # type: ignore
            for s in chain(dependencies, self._historical_dependencies)
        })
        super().__init__(redis_url=redis_url,
                         consumer_group_name=consumer_group_name,
                         input_streams=input_streams,
                         read_chunk_length=read_chunk_length,
                         **kwargs)

    def __repr__(self):
        return f'<{self.__class__.__name__} output_stream_name=\'{self.output_stream_name}\'' \
               f' input_streams={self.input_streams}, dependencies={self._dependencies}>'

    def dependency_map_key(self, message_id: str):
        return f'{SPECIAL_CHAR}depmap{self.output_stream_name}{SPECIAL_CHAR}{message_id}'

    def send_historical_deps(self, message_id: str, message: Dict[str, str],
                             pipeline: redis.client.Pipeline):
        historical_deps = [(k.dst_key, message[k.src_key], k.history_length)
                           for k in self._historical_dependencies
                           if k.src_key in message]
        if historical_deps:
            vehicle_id, vehicle_message_id = message_id.rsplit(SPECIAL_CHAR,
                                                               maxsplit=1)
            dependency_map_key_base = self.dependency_map_key(vehicle_id
                                                              + SPECIAL_CHAR)
            for dst_key, value, history_length in historical_deps:
                id_in_deps = history_length
                future_vehicle_message_id = int(vehicle_message_id)
                while id_in_deps > 0:
                    id_in_deps -= 1
                    future_vehicle_message_id += 1
                    pipeline.hset(
                        f'{dependency_map_key_base}{future_vehicle_message_id}',
                        f'{dst_key}{SPECIAL_CHAR}{id_in_deps}', value)

    def process_message_sync(
            self, stream_name: str, message: Dict[str, str], *,
            redis_conn: redis.Redis,
            pipeline: redis.client.Pipeline) -> Dict[str, Dict[str, str]]:
        message_id_enc = message.pop(MSG_ID_FIELD)
        message_id = decode_value(message_id_enc)
        dep_key = self.dependency_map_key(message_id)
        incr_key = dep_key + f'{SPECIAL_CHAR}incr'
        new_deps = {
            k.dst_key: message[k.src_key]
            for k in self._dependencies
            if k.src_key in message
        }
        incr_num = 0
        if new_deps:
            with redis_conn.pipeline() as p:
                p.multi()
                p.hset(dep_key, mapping=new_deps)  # type: ignore
                p.incrby(incr_key, len(new_deps))
                _, incr_num = p.execute()
        if self._historical_dependencies:
            self.send_historical_deps(message_id, message, pipeline)
        if incr_num == self._num_dependencies:
            dependencies = redis_conn.hgetall(dep_key)
            for h in self._historical_dependencies:
                dependencies[h.dst_key] = []
                for i in range(h.history_length):
                    dep_value = dependencies.pop(
                        f'{h.dst_key}{SPECIAL_CHAR}{i}', None)
                    dependencies[h.dst_key].append(
                        decode_value(dep_value
                                     ) if dep_value is not None else dep_value)
                dependencies[h.dst_key] = encode_value(dependencies[h.dst_key])
            # tutaj była transakcja
            pipeline.delete(dep_key, incr_key)
            return {
                self.output_stream_name: {
                    MSG_ID_FIELD: message_id_enc,
                    **dependencies
                }
            }
        return {}
