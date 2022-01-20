from datetime import datetime
from math import atan2, degrees
from typing import List, Optional, Sequence, Any

from pydantic import Field

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


@brook.artificial_field('direction')
async def direction(lat_history: Sequence[float] = historical_dependency(
    ZTMReport.lat, history_length=1),
                    lon_history: Sequence[float] = historical_dependency(
                        ZTMReport.lon, history_length=1),
                    lat: float = dependency(ZTMReport.lat),
                    lon: float = dependency(
                        ZTMReport.lon)) -> Optional[float]:
    if lat_history[-1] and lon_history[-1]:
        return degrees(atan2(lon - lon_history[-1], lat - lat_history[-1]))
    else:
        return None


@brook.artificial_field('latency1')
def latency1(time: datetime = dependency(ZTMReport.time)) -> datetime:
    return datetime.now()


@brook.artificial_field('latency2')
def latency2(time: datetime = dependency(ZTMReport.time), lat1: datetime = dependency(latency1)) -> datetime:
    return lat1


@brook.artificial_field('latency4')
def latency4(time: datetime = dependency(ZTMReport.time), lat2: datetime = dependency(latency2), direction: Any = dependency(direction)) -> str:
    return str((datetime.now() - lat2, direction))


@brook.output('raport2')
class PosReport(OutReport):
    latency2 = ReportField(latency4)
    lat = ReportField(ZTMReport.lat)
    long = ReportField(ZTMReport.lon)
    direction = ReportField(direction)


brook.set_meta(latitude_field=LocationReport.lat,
               longitude_field=LocationReport.lon,
               group_field=LocationReport.line,
               direction_field=PosReport.direction,
               time_field=LocationReport.time)
