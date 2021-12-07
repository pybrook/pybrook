from datetime import datetime

from pybrook.models import InReport, OutReport, PyBrook, ReportField, dependency

brook = PyBrook('redis://localhost')


@brook.input('location-report', id_field='vehicle_id')
class LocationReport(InReport):
    vehicle_id: int
    time: datetime
    latitude: float
    longitude: float
    temperature: int
    doors_open: bool
    speed: float


@brook.artificial_field('speed')
def calc_course_id(
        *,
        vehicle_id: int = dependency(LocationReport.vehicle_id),
        latitude: float = dependency(LocationReport.latitude),
        longitude: float = dependency(LocationReport.longitude),
) -> float:
    ...


@brook.output()
class CourseReport(OutReport):
    # course_id = ReportField(calc_course_id)
    # stop_id = ReportField(calc_course_id)
    latitude = ReportField(LocationReport.latitude)
    longitude = ReportField(LocationReport.longitude)
    temperature = ReportField(LocationReport.temperature)
    doors_open = ReportField(LocationReport.doors_open)
    speed = ReportField(LocationReport.speed)


print(LocationReport.time)
