from json import loads
from typing import Tuple, List

from locust import FastHttpUser, task, between
from datetime import datetime


def load_data() -> dict:
    return loads(open('ztm_dump.json').read())


data = load_data()
lines = list(sorted(data.keys()))


class VehicleReportUser(FastHttpUser):
    wait_time = between(1, 2)

    def on_start(self):
        self.line = lines.pop(0)
        self.records: List[Tuple[datetime, dict]] = sorted(
            [
                (datetime.fromisoformat(v['time']), v)
                for v in data[self.line]
            ],
            key=lambda v: v[0]
        )
        self.current_record_id = 0
        self.time_offset = None

    def get_record(self):
        if self.time_offset is None:
            self.time_offset = (
                datetime.now() - self.records[0][0]
            )
        now = datetime.now()
        for ((time_a, record_a), (time_b, record_b)) in zip(
            self.records[self.current_record_id:-1],
            self.records[self.current_record_id + 1:]
        ):
            time_a = time_a + self.time_offset
            time_b = time_b + self.time_offset
            if time_b <= now:
                self.current_record_id += 1
            else:
                break
        else:
            self.time_offset = None
            self.current_record_id = 0
            return self.get_record()

        diff = (time_b - time_a).total_seconds()
        pos = (now - time_a).total_seconds() / diff
        lat_diff = record_b['lat'] - record_a['lat']
        lon_diff = record_b['lon'] - record_a['lon']
        cur_lon = record_a['lon'] + lon_diff * pos
        cur_lat = record_a['lat'] + lat_diff * pos
        record = {
            **record_b, 'time': now.isoformat(),
            'lon': cur_lon,
            'lat': cur_lat
        }
        return record

    @task
    def send_vehicle_report(self):
        self.client.post(
            "/ztm-report", json=self.get_record()
        )
