# Introduction

PyBrook - a real-time cloud computing framework for the Internet of Things.
PyBrook enables users to define complex data processing models declaratively using the Python programming language.
The framework also provides a generic web interface that presents the collected data in real-time.

PyBrook aims to make the development of real-time data processing services as easy as possible by utilising powerful 
mechanisms of the Python programming language and modern concepts like hot-reloading or deploying software in Linux Containers.

A simple `docker-compose up` is enough to start playing with the framework.

## Run demo with Docker

It is recommended to use `docker-compose` for learning (you can use the `docker-compose.yml` from the [project repository](https://github.com/pybrook/pybrook/blob/master/docker-compose.yml):

```bash
docker-compose up
```

This command will start all the services, including Redis with Redis Gears enabled.

The following services will be available:

- OpenAPI docs (ReDoc): <http://localhost:8000/redoc> 
- OpenAPI docs (Swagger UI): <http://localhost:8000/docs> 
- PyBrook frontend: <http://localhost:8000/panel> 
- Locust panel for load testing: <http://localhost:8089>

You should probably visit the Locust panel first and start sending some reports.

### Using your own model

The configured model is `pybrook.examples.demo`, but replacing it with your own is very easy.  
First, you have to save your custom model somewhere. 
For now, you can just copy the source of `pybrook.examples.demo` (attached below) and save it as `mymodel.py` in your working directory.

??? example "Source of `pybrook.examples.demo`"

    ```python linenums="1"
    from datetime import datetime
    from math import atan2, degrees
    from typing import Optional, Sequence
    
    from pybrook.models import (
        InReport,
        OutReport,
        PyBrook,
        ReportField,
        dependency,
        historical_dependency,
    )
    
    brook = PyBrook('redis://localhost')
    app = brook.app
    
    
    @brook.input('ztm-report', id_field='vehicle_number')
    class ZTMReport(InReport):
        vehicle_number: int
        time: datetime
        lat: float
        lon: float
        brigade: str
        line: str
    
    
    @brook.output('location-report')
    class LocationReport(OutReport):
        vehicle_number = ReportField(ZTMReport.vehicle_number)
        lat = ReportField(ZTMReport.lat)
        lon = ReportField(ZTMReport.lon)
        line = ReportField(ZTMReport.line)
        time = ReportField(ZTMReport.time)
        brigade = ReportField(ZTMReport.brigade)
    
    
    @brook.artificial_field()
    def direction(lat_history: Sequence[float] = historical_dependency(
        ZTMReport.lat, history_length=1),
                        lon_history: Sequence[float] = historical_dependency(
                            ZTMReport.lon, history_length=1),
                        lat: float = dependency(ZTMReport.lat),
                        lon: float = dependency(ZTMReport.lon)) -> Optional[float]:
        prev_lat, = lat_history
        prev_lon, = lon_history
        if prev_lat and prev_lon:
            return degrees(atan2(lon - prev_lon, lat - prev_lat))
        else:
            return None
    
    
    @brook.output('direction-report')
    class DirectionReport(OutReport):
        direction = ReportField(direction)
    
    
    @brook.artificial_field()
    async def counter(prev_values: Sequence[int] = historical_dependency(
        'counter', history_length=1),
                      time: datetime = dependency(ZTMReport.time)) -> int:
        prev_value, = prev_values
        if prev_value is None:
            prev_value = -1
        prev_value += 1
        return prev_value
    
    
    @brook.output('counter-report')
    class CounterReport(OutReport):
        counter = ReportField(counter)
    
    
    brook.set_meta(latitude_field=LocationReport.lat,
                   longitude_field=LocationReport.lon,
                   time_field=LocationReport.time,
                   group_field=LocationReport.line,
                   direction_field=DirectionReport.direction)
    
    if __name__ == '__main__':
        brook.run()
    ```

After creating `mymodel.py`, you should add it to the `api` and `worker` containers, using a Docker volume.
To make PyBrook use `mymodel` instead of `pybrook.examples.demo`, you should also alter the arguments passed to `gunicorn` and `pybrook`. 
You can simply add it to the default `docker-compose.yml`:

```yaml hl_lines="20 21 10 11 12 13 14 24" linenums="1"
services:
  api:
    image: pybrook:latest
    build:
      context: .
    environment:
      REDIS_URL: redis://redis
    ports:
      - 8000:8000
    volumes:
      - ./mymodel.py:/src/mymodel.py
    command: gunicorn mymodel:app 
          -w 4 -k uvicorn.workers.UvicornWorker 
          -b 0.0.0.0:8000
  worker:
    image: pybrook:latest
    depends_on:
      - api
    environment:
      REDIS_URL: redis://redis
      DEFAULT_WORKERS: 8
    volumes:
      - ./mymodel.py:/src/mymodel.py
    command: pybrook mymodel:brook
  locust:
    image: pybrook:latest
    depends_on:
      - api
    ports:
      - 8089:8089
    command: locust -H http://api:8000
  redis:
    image: redislabs/redisgears:latest
```

Then run `docker-compose up --build` again, to start PyBrook - this time using your own model.

## Setup & Development

You can install the PyBrook from PyPi using `pip`:

```bash
pip install pybrook
```

## Running all services manually, without Docker

To run the `pybrook.examples.demo` model, you have to start all the required services manually:

```bash
# Redis + Redis Gears
docker run --net=host -d redislabs/redisgears
# HTTP API based on pybrook.examples.demo - uvicorn
uvicorn pybrook.examples.demo:app --reload  
# PyBrook workers based on pybrook.examples.demo 
pybrook pybrook.examples.demo:brook   
# Locust - load testing
locust -H http://localhost:8000
```

## Contributing

PyBrook uses [poetry](https://python-poetry.org) for dependency management.
To install all its development dependencies, simply run this command:

```bash
poetry install
```

### Tests

```bash
make test
```

### Code quality

The source code of PyBrook is formatted using yapf and isort.  
To run them with the correct settings, use the following command:

```bash
make format
```

PyBrook uses `mypy` for type checking and `flake8` for linting.
Use the following command to run them with the appropriate settings:

```bash
make lint
```