from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from pybrook.config import MSG_ID_FIELD, SPECIAL_CHAR


class StreamInfo(BaseModel):
    stream_name: str
    websocket_path: str
    report_schema: Dict[Any, Any]


class FieldInfo(BaseModel):
    stream_name: str
    field_name: str


class PyBrookSchema(BaseModel):
    streams: List[StreamInfo] = []
    special_char: str = SPECIAL_CHAR
    msg_id_field: str = MSG_ID_FIELD
    latitude_field: Optional[FieldInfo] = None
    longitude_field: Optional[FieldInfo] = None
    time_field: Optional[FieldInfo] = None
    group_field: Optional[FieldInfo] = None
