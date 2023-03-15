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

from collections import defaultdict

import redis

def timestamp_to_epoch(timestamp: str) -> int:
    return int(timestamp.split('-', maxsplit=1)[0])

def calc_latency(in_stream, out_streams):
    conn: redis.Redis = redis.from_url('redis://redis', decode_responses=1, encoding='utf-8')
    time_to_msg_id = {}
    stream_times = {}
    for stream in out_streams:
        stream_times[stream] = {}
        for timestamp, payload in conn.xrange(stream, '-', '+'):
            epoch = timestamp_to_epoch(timestamp)
            msg_id = payload[':_msg_id']
            if 'time' in payload:
                time_to_msg_id[payload['time']] = msg_id
            stream_times[stream][msg_id] = epoch
    deltas = defaultdict(dict)
    for timestamp, payload in conn.xrange(in_stream, '-', '+'):
        epoch = timestamp_to_epoch(timestamp)
        try:
            msg_id = time_to_msg_id[payload['time']]
        except KeyError:
            continue
        for stream in out_streams:
            if msg_id in stream_times[stream]:
                deltas[stream][msg_id] = stream_times[stream][msg_id] - epoch
    deltas = dict(deltas)
    for stream in out_streams:
        print(stream)
        sorted_deltas = sorted(deltas[stream].values())
        msgs = len(deltas[stream])
        print('Messages: ', msgs)
        print('Median: ', sorted_deltas[msgs//2])
        print('Average latency: ', sum(deltas[stream].values()) / msgs)
        print('90th percentile: ', sorted_deltas[round(msgs*0.9)])


if __name__ == '__main__':
    calc_latency(':ztm-report', [':location-report', ':brigade-report', ':direction-report'])
