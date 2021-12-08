import json
from typing import Dict, Union

from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel


def redisable_encoder(model: Union[BaseModel, Dict]):
    encoded_dict = jsonable_encoder(model)
    for k, v in encoded_dict.items():
        if type(v) in (str, bytes, int, float):
            continue
        if type(v) is bool:
            encoded_dict[k] = int(v)
        else:
            # TODO: Handle this when decoding
            encoded_dict[k] = json.dumps(v)
    return encoded_dict
