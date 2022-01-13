import asyncio
from datetime import datetime
from math import atan2, pi, degrees
from time import sleep
from typing import List, Optional, Any, Tuple

import aioredis
import redis
from pydantic import Field

from pybrook.models import (
    HistoricalDependency,
    InReport,
    OutReport,
    PyBrook,
    ReportField, Dependency,
)

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


@brook.artificial_field('direction')
async def direction(lat_history: List[float] = HistoricalDependency(ZTMReport.latitude, history_length=1),
               lon_history: List[float] = HistoricalDependency(ZTMReport.longitude, history_length=1),
               lat: float = Dependency(ZTMReport.latitude),
               lon: float = Dependency(ZTMReport.longitude)) -> Optional[float]:
    if lat_history[-1] and lon_history[-1]:
        return degrees(atan2(lon - lon_history[-1], lat - lat_history[-1]))
    else:
        return None


@brook.output('raport2')
class PosReport(OutReport):
    lat = ReportField(ZTMReport.latitude)
    long = ReportField(ZTMReport.longitude)
    direction = ReportField(direction)


brook.set_meta(latitude_field=LocationReport.latitude,
               longitude_field=LocationReport.longitude,
               group_field=LocationReport.line,
               direction_field=PosReport.direction,
               time_field=LocationReport.time)
