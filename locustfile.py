import random
from datetime import datetime

from locust import HttpUser, task


class VehicleReportUser(HttpUser):

    def on_start(self):
        super().on_start()
        self.request_id = 0

    @task
    def send_vehicle_report(self):
        self.request_id += 1
        randid = random.randint(1, 100)
        self.client.post("/location-report", json={
            "vehicle_id": randid,
            "time": datetime.now().isoformat(),
            "latitude": randid,
            "longitude": randid,
            "temperature": randid,
            "doors_open": bool(randid % 2),
            "speed": randid
        })
