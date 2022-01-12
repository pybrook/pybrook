from typing import Any, Dict

from orjson import orjson


def redisable_encoder(data: Dict[str, Any]):
    return {k: orjson.dumps(v) for k, v in data.items()}


def redisable_decoder(data: Dict[str, str]):
    return {k: redisable_value_decoder(v) for k, v in data.items()}


def redisable_value_decoder(v):
    return orjson.loads(v)
