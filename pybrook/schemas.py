from typing import List, Optional

from pydantic import BaseModel


class StreamInfo(BaseModel):
    stream_name: str
    websocket_path: str


class FieldInfo(BaseModel):
    stream_name: str
    field_name: str


class PyBrookSchema(BaseModel):
    streams: List[StreamInfo] = []
    latitude_field: Optional[FieldInfo] = None
    longitude_field: Optional[FieldInfo] = None
    group_field: Optional[FieldInfo] = None
