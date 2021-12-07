from locust import HttpUser, task


class HelloWorldUser(HttpUser):
    @task
    def hello_world(self):
        self.client.post("/location-report", json={
            "vehicle_id": 0,
            "time": "2021-12-07T21:57:22.592Z",
            "latitude": 0,
            "longitude": 0,
            "temperature": 0,
            "doors_open": True,
            "speed": 0
        })
