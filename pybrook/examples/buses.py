import asyncio
import random
from datetime import datetime

from pybrook.models import InReport, OutReport, PyBrook, ReportField, dependency

brook = PyBrook('redis://localhost')
app = brook.app


@brook.input('location-report', id_field='vehicle_id')
class LocationReport(InReport):
    vehicle_id: int
    time: datetime
    latitude: float
    longitude: float
    temperature: int
    doors_open: bool
    speed: float


@brook.artificial_field('course_id')
async def calc_course_id(
        *,
        vehicle_id: int = dependency(LocationReport.vehicle_id),
        latitude: float = dependency(LocationReport.latitude),
        longitude: float = dependency(LocationReport.longitude),
) -> float:
    await asyncio.sleep(1)
    return vehicle_id * latitude * longitude


@brook.output('course-report')
class CourseReport(OutReport):
    course_id = ReportField(calc_course_id)
    time = ReportField(LocationReport.time)
    latitude = ReportField(LocationReport.latitude)
    longitude = ReportField(LocationReport.longitude)
    temperature = ReportField(LocationReport.temperature)
    doors_open = ReportField(LocationReport.doors_open)
    speed = ReportField(LocationReport.speed)


if __name__ == '__main__':
    brook.run()
