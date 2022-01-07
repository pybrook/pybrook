from pydantic import BaseModel


class StreamInfo(BaseModel):
    stream_name: str
    websocket_path: str

