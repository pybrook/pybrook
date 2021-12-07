import dataclasses
import inspect
import json
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Dict,
    List,
    Type,
    TypeVar,
    Union,
    get_type_hints,
)

import aioredis
import fastapi
import pydantic
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel

from pybrook.config import FIELD_PREFIX
from pybrook.consumers import Splitter
from pybrook.consumers.base import StreamConsumer


class ConsumerGenerator:
    @classmethod
    def gen_consumers(cls, model: 'PyBrook'):
        raise NotImplementedError


class RouteGenerator:
    @classmethod
    def gen_routes(cls, app: FastAPI,
                   redis_dep: aioredis.Redis):
        raise NotImplementedError


class OutReportMeta(type):

    @property
    def model(cls) -> Type[BaseModel]:
        fields: Dict[str, ReportField] = {
            field: member for field, member in inspect.getmembers(cls) if isinstance(member, ReportField)}
        print(fields)
        pydantic_fields = {
            field: (member.source_field.value_type, pydantic.Field())
            for field, member in fields.items()
        }
        if not hasattr(cls, '_model'):
            cls._model = pydantic.create_model(
                cls.__name__ + 'Model',
                **pydantic_fields
            )
        return cls._model


class OutReport(ConsumerGenerator, RouteGenerator, metaclass=OutReportMeta):

    @classmethod
    def gen_routes(cls, app: FastAPI, redis_dep: aioredis.Redis):
        @app.get(f'/{cls.__name__}', response_model=cls.model)
        async def get_report(redis: aioredis.Redis = redis_dep):
            ...

    @classmethod
    def gen_consumers(cls, model: 'PyBrook'):
        ...


class Dependency:
    def __init__(self, src_field: 'SourceField'):
        self.src_field = src_field


def dependency(src_field: Union['SourceField', Any]) -> Any:
    if not isinstance(src_field, SourceField):
        raise RuntimeError(f'{src_field} is not a SourceField')
    return Dependency(src_field)


class HistoricalDependency(Dependency):
    def __init__(self, src_field: Any, num: int):
        super().__init__(src_field)


class SourceField:
    def __init__(self,
                 field_name: str,
                 *,
                 value_type: Type,
                 source_obj: Type['InReport'] = None):
        """
        A base class for fields (InputField, ArtificialField)

        Provides metadata used to generate producers & consumers.

        Args:
            value_type: Field value type.
            field_name: Source field.
            source_obj: InReport that the field belongs to.
        """
        self.field_name = field_name
        self.source_obj = source_obj
        self.value_type = value_type

    def __repr__(self):
        return f'<{self.__class__.__name__} name={self.field_name},' \
               f' report_class={self.source_obj.__name__},' \
               f' value_type={self.value_type.__name__}>'


@dataclasses.dataclass
class InReportOptions:
    id_field: str
    name: str

    @property
    def stream_name(self):
        return f'{FIELD_PREFIX}{self.name}'


class OptionsMixin:
    @property
    def _options(self) -> InReportOptions:
        return self._report_options

    @_options.setter
    def _options(self, report_options: InReportOptions):
        try:
            getattr(self, report_options.id_field)
        except AttributeError:
            raise RuntimeError(
                f'Invalid id_field! {self.__name__} '
                f'has no attribute "{report_options.id_field}".') from None
        self._report_options = report_options


class InReportMeta(OptionsMixin, pydantic.main.ModelMetaclass):
    def __getattr__(cls, item: str) -> SourceField:  # noqa: N805
        if item in cls.__fields__ and not item.startswith('_'):  # type: ignore
            return InputField(cls, cls.__fields__[item])  # type: ignore
        return super().__getattribute__(item)  # noqa: WPS613


class InReport(ConsumerGenerator, RouteGenerator, pydantic.BaseModel, metaclass=InReportMeta):
    @classmethod
    def gen_consumers(cls, model: 'PyBrook'):
        splitter = Splitter(redis_url=model.redis_url,
                            consumer_group_name=cls._options.name,
                            input_streams=cls._options.stream_name)
        model.add_consumer(splitter)

    @classmethod
    def gen_routes(cls, app: FastAPI, redis_dep: aioredis.Redis):
        @app.post(f'/{cls._options.name}',
                  name=f'Add {cls._options.name}')
        async def add_report(report: cls = fastapi.Body(...), redis: aioredis.Redis = redis_dep):  # type: ignore
            print(jsonable_encoder(report.dict()))
            await redis.xadd(cls._options.stream_name, redisable_encoder(report))


def redisable_encoder(model: BaseModel):
    encoded_dict = jsonable_encoder(model)
    for k, v in encoded_dict.items():
        if type(v) in (str, bytes, int, float):
            continue
        if type(v) is bool:
            encoded_dict[k] = int(v)
        else:
            encoded_dict[k] = json.dumps(v)
    return encoded_dict



class ReportField:
    def __init__(self, source_field: Union[SourceField, Any]):
        if not isinstance(source_field, SourceField):
            raise RuntimeError(f'{source_field} is not a SourceField')
        self.source_field: SourceField = source_field


class InputField(SourceField):
    def __init__(self, report_class: Type[InReport],
                 pydantic_field: pydantic.fields.ModelField):
        super().__init__(pydantic_field.name,
                         value_type=pydantic_field.type_,
                         source_obj=report_class)


class ArtificialField(SourceField, ConsumerGenerator):
    def __init__(self, calculate: Callable, name: str = None):
        annotations = get_type_hints(calculate)
        super().__init__(field_name=(name or calculate.__name__),
                         value_type=annotations.pop('return'))
        self.args: inspect.Signature = inspect.signature(calculate)
        self.dependencies = [
            val.default for val in self.args.parameters.values()
        ]
        if not all(isinstance(d, Dependency) for d in self.dependencies):
            raise RuntimeError(
                f'Artificial field "{self.field_name}" has default values'
                f' which do not subclass Dependency.')
        self.calculate = calculate

    def __call__(self, *args, **kwargs):
        return self.calculate(*args, **kwargs)

    @classmethod
    def gen_consumers(cls, model: 'PyBrook'):
        pass


TI = TypeVar('TI', bound=Type[InReport])

TO = TypeVar('TO', bound=Type[OutReport])


class PyBrookApi:
    def __init__(self, model: 'PyBrook'):
        self.fastapi = FastAPI()
        self.model = model

    async def redis_dependency(self) -> AsyncIterator[aioredis.Redis]:
        """Redis FastAPI Dependency"""
        redis = await aioredis.from_url(self.model.redis_url,
                                        encoding='utf-8',
                                        decode_responses=True)
        yield redis
        await redis.close()
        await redis.connection_pool.disconnect()

    def visit(self, generator: RouteGenerator):
        generator.gen_routes(self.fastapi,
                             redis_dep=fastapi.Depends(self.redis_dependency))


class PyBrook:
    def __init__(self, redis_url: str):
        self.inputs: Dict[str, Type[InReport]] = {}
        self.outputs: Dict[str, Type[OutReport]] = {}
        self.artificial_fields: Dict[str, ArtificialField] = {}
        self.consumers: List[StreamConsumer] = []
        self.redis_url: str = redis_url
        self.api: PyBrookApi = PyBrookApi(self)

    @property
    def app(self) -> FastAPI:
        return self.api.fastapi

    def input(
            self,  # noqa: A003
            name: str = None,
            *,
            id_field: str) -> Callable[[TI], TI]:
        def wrapper(cls) -> TI:
            name_safe = name or cls.__name__
            self.inputs[name_safe] = cls
            cls._options = InReportOptions(id_field=id_field, name=name_safe)
            self.visit(cls)
            self.api.visit(cls)
            return cls

        return wrapper

    def output(self, name: str = None) -> Callable[[TO], TO]:
        def wrapper(cls):
            self.outputs[name or cls.__name__] = cls
            self.visit(cls)
            self.api.visit(cls)
            return cls

        return wrapper

    def artificial_field(self,
                         name: str = None
                         ) -> Callable[[Callable], ArtificialField]:
        def wrapper(fun: Callable) -> ArtificialField:
            field = ArtificialField(fun)
            self.artificial_fields[name or fun.__name__] = field
            self.visit(field)
            return field

        return wrapper

    def visit(self, generator: ConsumerGenerator):
        generator.gen_consumers(self)

    def add_consumer(self, consumer: StreamConsumer):
        self.consumers.append(consumer)
