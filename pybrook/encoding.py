import json
from typing import Dict, Union

from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel


def redisable_encoder(model: Union[BaseModel, Dict]):
    encoded_dict = jsonable_encoder(
        model.dict(by_alias=False) if isinstance(model, BaseModel) else model)
    for k, v in encoded_dict.items():
        if type(v) in {str, bytes, int, float}:  # noqa: WPS516
            continue
        if type(v) is bool:  # noqa: WPS516
            encoded_dict[k] = int(v)
        else:
            # TODO: Handle this when decoding
            encoded_dict[k] = json.dumps(v)
    return encoded_dict
