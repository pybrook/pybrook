import asyncio
from datetime import datetime
from time import sleep
from typing import Optional

import aioredis
import redis
from pydantic import Field

from pybrook.models import Dependency, InReport, OutReport, PyBrook, ReportField

brook = PyBrook('redis://localhost')
app = brook.app


@brook.input('ztm-report', id_field='vehicle_id')
class ZTMReport(InReport):
    vehicle_id: int = Field(alias='vehicle_number')
    time: datetime
    latitude: float = Field(alias='lat')
    longitude: float = Field(alias='lon')
    brigade: str
    line: str = Field(alias='lines')


@brook.output('location-report')
class LocationReport(OutReport):
    vehicle_id = ReportField(ZTMReport.vehicle_id)
    latitude = ReportField(ZTMReport.latitude)
    longitude = ReportField(ZTMReport.longitude)
    line = ReportField(ZTMReport.line)
    time = ReportField(ZTMReport.time)
    brigade = ReportField(ZTMReport.brigade)


@brook.artificial_field('stop3')
async def stop(lat: float = Dependency(ZTMReport.latitude),
               lon: float = Dependency(ZTMReport.longitude),
               redis_conn: aioredis.Redis = Dependency(
                   aioredis.Redis)) -> Optional[str]:
    await asyncio.sleep(10)
    stop_names = await redis_conn.georadius('stops',
                                            lat,
                                            lon,
                                            50,
                                            unit='m',
                                            count=1)  # type: ignore

    return stop_names[0] if stop_names else None


@brook.output('course-report3')
class CourseReport(OutReport):
    stop_name = ReportField(stop)
    lat = ReportField(ZTMReport.latitude)
    lon = ReportField(ZTMReport.longitude)
    time = ReportField(ZTMReport.time)


brook.set_meta(latitude_field=LocationReport.latitude,
               longitude_field=LocationReport.longitude,
               group_field=LocationReport.line,
               time_field=LocationReport.time)

if __name__ == '__main__':
    brook.run()
