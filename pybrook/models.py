import dataclasses
import inspect
import json
from functools import lru_cache, cached_property
from typing import (
    Any,
    AsyncIterator,
    Callable,
    Dict,
    Generic,
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
from pydantic import BaseModel

from pybrook.config import FIELD_PREFIX
from pybrook.consumers.base import BaseStreamConsumer
from pybrook.consumers.dependency_resolver import DependencyResolver
from pybrook.consumers.field_generator import (
    AsyncFieldGenerator,
    BaseFieldGenerator,
    SyncFieldGenerator,
)
from pybrook.consumers.splitter import GearsSplitter, SyncSplitter
from pybrook.consumers.worker import WorkerManager
from pybrook.utils import redisable_encoder


class ConsumerGenerator:
    @classmethod
    def gen_consumers(cls, model: 'PyBrook'):
        raise NotImplementedError


class RouteGenerator:
    @classmethod
    def gen_routes(cls, app: FastAPI, redis_dep: aioredis.Redis):
        raise NotImplementedError


class Dependency:
    def __init__(self, src_field: 'SourceField'):
        self.src_field = src_field

    def cast(self, value: str):
        return self.src_field.value_type(value)

    def __repr__(self):
        return f'<Dependency src_field={self.src_field}>'


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

    @property
    @lru_cache
    def stream_name(self) -> str:
        if self.source_obj:
            return f'{FIELD_PREFIX}{self.source_obj._options.name}{FIELD_PREFIX}split'
        else:
            return f'{FIELD_PREFIX}artificial{FIELD_PREFIX}{self.field_name}'

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


TOPT = TypeVar('TOPT')


class OptionsMixin(Generic[TOPT]):
    @property
    def _options(self) -> TOPT:
        return self.__options

    @_options.setter
    def _options(self, options: TOPT):
        options = self._validate_options(options)
        self.__options = options

    def _validate_options(self, options: TOPT) -> TOPT:
        raise NotImplementedError


class InReportMeta(OptionsMixin[InReportOptions],
                   pydantic.main.ModelMetaclass):
    _options: InReportOptions

    def _validate_options(self, options: InReportOptions) -> InReportOptions:
        try:
            getattr(self, options.id_field)
        except AttributeError:
            raise RuntimeError(
                f'Invalid id_field! {self.__name__} '
                f'has no attribute "{options.id_field}".') from None
        return options

    def __getattr__(cls, item: str) -> SourceField:  # noqa: N805
        if item in cls.__fields__ and not item.startswith('_'):  # type: ignore
            return InputField(cls, cls.__fields__[item])  # type: ignore
        return super().__getattribute__(item)  # noqa: WPS613


class InReport(ConsumerGenerator,
               RouteGenerator,
               pydantic.BaseModel,
               metaclass=InReportMeta):
    @classmethod
    def gen_consumers(cls, model: 'PyBrook'):
        splitter = GearsSplitter(redis_url=model.redis_url,
                                 object_id_field=cls._options.id_field,
                                 consumer_group_name=cls._options.name,
                                 namespace=cls._options.name,
                                 input_streams=[cls._options.stream_name])
        model.add_consumer(splitter)

    @classmethod
    def gen_routes(cls, app: FastAPI, redis_dep: aioredis.Redis):
        @app.post(f'/{cls._options.name}', name=f'Add {cls._options.name}')
        async def add_report(report: cls = fastapi.Body(...),  # type: ignore
                             redis: aioredis.Redis = redis_dep
                             ):  # type: ignore
            await redis.xadd(cls._options.stream_name,
                             redisable_encoder(report))


@dataclasses.dataclass
class OutReportOptions:
    name: str

    @property
    def stream_name(self):
        return f'{FIELD_PREFIX}{self.name}'


class OutReportMeta(OptionsMixin[OutReportOptions], type):
    def _validate_options(self, options: OutReportOptions) -> OutReportOptions:
        return options

    @property
    @lru_cache
    def report_fields(cls) -> Dict[str, 'ReportField']:
        return {
            field: member
            for field, member in inspect.getmembers(cls)
            if isinstance(member, ReportField)
        }

    @property
    @lru_cache
    def model(cls) -> Type[BaseModel]:
        pydantic_fields = {
            field: (member.source_field.value_type, pydantic.Field())
            for field, member in cls.report_fields.items()
        }
        if not hasattr(cls, '_model'):
            cls._model: Type[BaseModel] = pydantic.create_model(
                cls.__name__ + 'Model',
                **pydantic_fields  # type: ignore
            )
        return cls._model


class OutReport(ConsumerGenerator, RouteGenerator, metaclass=OutReportMeta):
    _options: OutReportOptions
    report_fields: Dict[str, 'ReportField']

    @classmethod
    def gen_routes(cls, app: FastAPI, redis_dep: aioredis.Redis):
        @app.get(f'/{cls._options.name}',
                 response_model=cls.model,
                 name=f'Retrieve {cls._options.name}')
        async def get_report(redis: aioredis.Redis = redis_dep):
            for msg_id, msg_body in (await
                                     redis.xrevrange(cls._options.stream_name
                                                     )):
                return cls.model(**msg_body)
            else:
                return {}

    @classmethod
    def gen_consumers(cls, model: 'PyBrook'):
        dependency_resolver = DependencyResolver(
            redis_url=model.redis_url,
            output_stream_name=cls._options.stream_name,
            dependencies={
                dependency_name: field.source_field.stream_name
                for dependency_name, field in cls.report_fields.items()
            },
            resolver_name=f'{cls._options.name}')
        model.add_consumer(dependency_resolver)


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
        self.is_coro: bool = inspect.iscoroutinefunction(calculate)
        self.dependencies: Dict[str, Dependency] = {
            arg_name: arg.default
            for arg_name, arg in self.args.parameters.items()
        }
        if not all(
                isinstance(d, Dependency) for d in self.dependencies.values()):
            raise RuntimeError(
                f'Artificial field "{self.field_name}" has default values'
                f' which do not subclass Dependency.')
        self.calculate = calculate

    def __call__(self, *args, **kwargs):
        return self.calculate(*args, **kwargs)

    def gen_consumers(self, model: 'PyBrook'):  # type: ignore
        dependency_resolver = DependencyResolver(
            redis_url=model.redis_url,
            output_stream_name=
            f'{FIELD_PREFIX}{self.field_name}{FIELD_PREFIX}deps',
            dependencies={
                dep_name: dep.src_field.stream_name
                for dep_name, dep in self.dependencies.items()
            },
            resolver_name=f'{self.field_name}')
        model.add_consumer(dependency_resolver)

        field_generator_deps = [
            BaseFieldGenerator.Dependency(name=dep_name,
                                          value_type=dep.src_field.value_type)
            for dep_name, dep in self.dependencies.items()
        ]

        generator_class = AsyncFieldGenerator if self.is_coro else SyncFieldGenerator

        field_generator = generator_class(
            redis_url=model.redis_url,
            dependency_stream=dependency_resolver.output_stream_name,
            field_name=self.field_name,
            generator=self.calculate,
            dependencies=field_generator_deps)

        model.add_consumer(field_generator)


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
        self.consumers: List[BaseStreamConsumer] = []
        self.redis_url: str = redis_url
        self.api: PyBrookApi = PyBrookApi(self)

    @property
    def app(self) -> FastAPI:
        return self.api.fastapi

    def run(self):
        WorkerManager(self.consumers).run()

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
            name_safe = name or cls.__name__
            self.outputs[name_safe] = cls
            cls._options = OutReportOptions(name=name_safe)
            self.visit(cls)
            self.api.visit(cls)
            return cls

        return wrapper

    def artificial_field(self,
                         name: str = None
                         ) -> Callable[[Callable], ArtificialField]:
        def wrapper(fun: Callable) -> ArtificialField:
            field = ArtificialField(fun, name=name)
            self.artificial_fields[name or fun.__name__] = field
            self.visit(field)
            return field

        return wrapper

    def visit(self, generator: ConsumerGenerator):
        generator.gen_consumers(self)

    def add_consumer(self, consumer: BaseStreamConsumer):
        print(consumer)
        self.consumers.append(consumer)
