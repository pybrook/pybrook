from datetime import datetime
from json import loads

from locust import FastHttpUser, task

request_id = 0

data = loads(open('records.json').read())
time_ranges = sorted([datetime.fromisoformat(d) for d in data.keys()])

time_diff = None


class VehicleReportUser(FastHttpUser):

    @task
    def send_vehicle_report(self):
        global time_diff
        if not time_diff:
            time_diff = datetime.now() - time_ranges[0]
        if time_ranges[0] + time_diff > datetime.now():
            return
        while not (reports_for_range := data[time_ranges[0].isoformat()]):
            time_ranges.pop(0)

        req = reports_for_range.pop(0)
        req['time'] = (datetime.fromisoformat(req['time']) + time_diff).isoformat()
        print(req['time'])
        self.client.post("/ztm-report", json=req)
