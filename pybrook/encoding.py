from typing import Any, Dict

import orjson


def encode_stream_message(data: Dict[str, Any]):
    return {k: encode_value(v) for k, v in data.items()}


def encode_value(v: Any):
    return orjson.dumps(v)


def decode_stream_message(data: Dict[str, str]):
    return {k: decode_value(v) for k, v in data.items()}


def decode_value(v: Any):
    return orjson.loads(v)
