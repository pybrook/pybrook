import random
from datetime import datetime

from locust import FastHttpUser, task

request_id = 0

class VehicleReportUser(FastHttpUser):

    @task
    def send_vehicle_report(self):
        global request_id
        request_id += 1
        self.client.post("/location-report", json={
            "vehicle_id": request_id,
            "time": datetime.now().isoformat(),
            "latitude": request_id,
            "longitude": request_id,
            "temperature": request_id,
            "doors_open": bool(request_id % 2),
            "speed": request_id
        })
