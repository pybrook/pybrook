import asyncio
import dataclasses
import inspect
import signal
from time import time
from typing import (  # noqa: WPS235
    Any, AsyncIterator, Callable, Dict, Generic, List, Optional, Type, TypeVar,
    Union, get_type_hints,
)

import aioredis
import fastapi
import pydantic
import redis
from loguru import logger
from starlette.middleware.cors import CORSMiddleware
from websockets.exceptions import ConnectionClosedOK

from pybrook.config import MSG_ID_FIELD, SPECIAL_CHAR
from pybrook.consumers.base import BaseStreamConsumer
from pybrook.consumers.dependency_resolver import DependencyResolver
from pybrook.consumers.field_generator import (
    AsyncFieldGenerator,
    BaseFieldGenerator,
    SyncFieldGenerator,
)
from pybrook.consumers.splitter import GearsSplitter
from pybrook.consumers.worker import WorkerManager
from pybrook.encoding import redisable_decoder, redisable_encoder
from pybrook.schemas import FieldInfo, PyBrookSchema, StreamInfo


class ConsumerGenerator:
    @classmethod
    def gen_consumers(cls, model: 'PyBrook'):
        raise NotImplementedError


class RouteGenerator:
    @classmethod
    def gen_routes(cls, api: 'PyBrookApi', redis_dep: aioredis.Redis):
        raise NotImplementedError


class Dependency:
    def __init__(self, src: Union['SourceField', aioredis.Redis, redis.Redis]):
        self.is_aioredis = type(src) == type and issubclass(
            src, aioredis.Redis)
        self.is_redis = type(src) == type and issubclass(src, redis.Redis)
        if isinstance(src, SourceField):
            self.src_field = src
        elif self.is_aioredis or self.is_redis:
            self.src_field = None
        else:
            raise ValueError(
                f'{src} is not an instance of SourceField or a Redis class')

    def cast(self, value: str):
        return self.src_field.value_type(value)

    def __repr__(self):
        if self.is_aioredis:
            return '<Dependency aioredis>'
        if self.is_redis:
            return '<Dependency redis>'
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
        self.field_name: str = field_name
        self.source_obj: Optional[Type[InReport]] = source_obj
        self.value_type: Type = value_type

    @property  # type: ignore
    def stream_name(self) -> str:
        if self.source_obj:
            return f'{SPECIAL_CHAR}{self.source_obj._options.name}{SPECIAL_CHAR}split'
        else:
            return f'{SPECIAL_CHAR}artificial{SPECIAL_CHAR}{self.field_name}'

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
        return f'{SPECIAL_CHAR}{self.name}'


TOPT = TypeVar('TOPT')


class OptionsMixin(Generic[TOPT]):
    @property
    def _options(self) -> TOPT:
        return self.__options

    @_options.setter
    def _options(self, options: TOPT):
        options = self._validate_options(options)
        self.__options = options  # noqa: WPS112

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
    def gen_routes(cls, api: 'PyBrookApi', redis_dep: aioredis.Redis):
        @api.fastapi.post(f'/{cls._options.name}',
                          name=f'Add {cls._options.name}')
        async def add_report(
                report: cls = fastapi.Body(...),  # type: ignore
                redis: aioredis.Redis = redis_dep):  # type: ignore
            await redis.xadd(cls._options.stream_name,
                             redisable_encoder(report.dict(by_alias=False)))


@dataclasses.dataclass
class OutReportOptions:
    name: str

    @property
    def stream_name(self):
        return f'{SPECIAL_CHAR}{self.name}'


class OutReportMeta(OptionsMixin[OutReportOptions], type):
    def _validate_options(self, options: OutReportOptions) -> OutReportOptions:
        return options

    @property  # type: ignore
    def report_fields(cls) -> List['ReportField']:
        return [
            m[1] for m in inspect.getmembers(cls)
            if isinstance(m[1], ReportField)
        ]

    @property
    def stream_name(cls):
        return cls._options.stream_name

    @property  # type: ignore
    def model(cls) -> Type[pydantic.BaseModel]:
        pydantic_fields = {
            report_field.destination_field_name:
            (report_field.source_field.value_type, pydantic.Field())
            for report_field in cls.report_fields  # type: ignore
        }
        pydantic_fields[MSG_ID_FIELD] = (
            str,
            pydantic.Field(
                title='Message ID',
                description=
                f'Message id - {{object ID}}{SPECIAL_CHAR}{{msg index for object}}'
            ))
        if not hasattr(cls, '_model'):
            cls._model: Type[pydantic.BaseModel] = pydantic.create_model(
                cls.__name__ + 'Model',
                **pydantic_fields  # type: ignore
            )
        return cls._model


class OutReport(ConsumerGenerator, RouteGenerator, metaclass=OutReportMeta):
    _options: OutReportOptions
    report_fields: List['ReportField']

    @classmethod
    def gen_routes(cls, api: 'PyBrookApi', redis_dep: aioredis.Redis):
        model_cls = cls.model

        @api.fastapi.get(
            f'/{cls._options.name}',
            response_model=model_cls,  # type: ignore
            name=f'Retrieve {cls._options.name}')
        async def get_report(redis: aioredis.Redis = redis_dep):
            for msg_id, msg_body in (await
                                     redis.xrevrange(cls._options.stream_name,
                                                     count=1)):
                return model_cls(**redisable_decoder(msg_body))
            return {}

        @api.fastapi.on_event('startup')
        def startup():
            api.fastapi.state.socket_active = True
            signal.signal(signal.SIGINT, shutdown)
            signal.signal(signal.SIGTERM, shutdown)

        def shutdown(*args):
            logger.info('set socket active to false')
            api.fastapi.state.socket_active = False

        @api.fastapi.websocket(f'/{cls._options.name}')
        async def read_reports(websocket: fastapi.WebSocket,
                               redis: aioredis.Redis = redis_dep):
            await websocket.accept()
            last_msg = '$'
            stream_name = cls._options.stream_name
            active = True
            last_ping = time()
            while active and api.fastapi.state.socket_active:
                if time() - last_ping > 30:
                    try:
                        # Check if connection is active
                        await asyncio.wait_for(websocket.receive_bytes(),
                                               timeout=0.01)
                        last_ping = time()
                    except asyncio.TimeoutError:
                        pass
                    except (fastapi.WebSocketDisconnect, AssertionError):
                        active = False
                if messages := await redis.xread({stream_name: last_msg},
                                                 count=100,
                                                 block=100):
                    # Wiadomości w strumieniach są mapami
                    for m_data in dict(messages)[stream_name]:
                        last_msg, payload = m_data
                        try:
                            await websocket.send_text(
                                model_cls(**redisable_decoder(payload)).json())
                        except ConnectionClosedOK:
                            active = False
            try:
                await websocket.close()
                logger.info(f'WebSocket connection closed by server')
            except RuntimeError:
                logger.info(f'WebSocket connection closed by client')

        api.schema.streams.append(
            StreamInfo(stream_name=cls._options.stream_name,
                       websocket_path=f'/{cls._options.name}',
                       report_schema=cls.model.schema()))

    @classmethod
    def gen_consumers(cls, model: 'PyBrook'):
        dependency_resolver = DependencyResolver(
            redis_url=model.redis_url,
            output_stream_name=cls._options.stream_name,
            dependencies=[
                DependencyResolver.Dependency(
                    src_stream=field.source_field.stream_name,
                    src_key=field.source_field.field_name,
                    dst_key=field.destination_field_name)
                for field in cls.report_fields
            ],
            resolver_name=f'{cls._options.name}')
        model.add_consumer(dependency_resolver)


class ReportField:
    def __init__(self, source_field: Union[SourceField, Any]):
        if not isinstance(source_field, SourceField):
            raise RuntimeError(f'{source_field} is not a SourceField')
        self.destination_field_name: str
        self.source_field = source_field
        self._owner: Type[OutReport]

    def __repr__(self):
        return f'<{self.__class__.__name__} ' \
               f'destination_field_name=\'{self.destination_field_name}\' ' \
               f'source_field={self.source_field} ' \
               f'owner={self.owner}>'

    @property
    def owner(self) -> Type[OutReport]:
        return self._owner

    @property
    def destination_stream_name(self):
        return self._owner.stream_name

    def __set_name__(self, owner: Type[OutReport], name: str):
        self._owner = owner
        self.destination_field_name = name

    def __get__(self, instance, owner: Type[OutReport]):
        return self


class InputField(SourceField):
    def __init__(self, report_class: Type[InReport],
                 pydantic_field: pydantic.fields.ModelField):
        super().__init__(pydantic_field.name,
                         value_type=pydantic_field.type_,
                         source_obj=report_class)


class ArtificialField(SourceField, ConsumerGenerator):
    def __init__(self, calculate: Callable, name: str = None):
        annotations = get_type_hints(calculate)
        try:
            value_type = annotations.pop('return')
        except KeyError:
            raise ValueError(
                f'Please specify return value for {calculate.__name__}')
        super().__init__(field_name=(name or calculate.__name__),
                         value_type=value_type)
        self.args: inspect.Signature = inspect.signature(calculate)
        self.is_coro: bool = inspect.iscoroutinefunction(calculate)
        self.dependencies: Dict[str, Dependency] = {
            arg_name: arg.default
            for arg_name, arg in self.args.parameters.items()
        }
        if not all(
                isinstance(d, Dependency)
                for k, d in self.dependencies.items()):
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
            f'{SPECIAL_CHAR}{self.field_name}{SPECIAL_CHAR}deps',
            dependencies=[
                DependencyResolver.Dependency(
                    src_stream=dep.src_field.stream_name,
                    src_key=dep.src_field.field_name,
                    dst_key=dep_key)
                for dep_key, dep in self.dependencies.items() if dep.src_field
            ],
            resolver_name=f'{self.field_name}')
        model.add_consumer(dependency_resolver)

        field_generator_deps = [
            BaseFieldGenerator.Dependency(name=dep_name,
                                          value_type=dep.src_field.value_type)
            for dep_name, dep in self.dependencies.items() if dep.src_field
        ]

        generator_class = AsyncFieldGenerator if self.is_coro else SyncFieldGenerator

        field_generator = generator_class(
            redis_url=model.redis_url,
            dependency_stream=dependency_resolver.output_stream_name,
            field_name=self.field_name,
            generator=self.calculate,
            dependencies=field_generator_deps,
            pass_redis=[
                k for k, d in self.dependencies.items()
                if (d.is_aioredis and self.is_coro) or d.is_redis
            ])

        model.add_consumer(field_generator)


TI = TypeVar('TI', bound=Type[InReport])

TO = TypeVar('TO', bound=Type[OutReport])


class PyBrookApi:
    def __init__(self, brook: 'PyBrook'):
        self.fastapi = fastapi.FastAPI()
        self.brook = brook
        self.schema = PyBrookSchema()
        self.fastapi.add_middleware(CORSMiddleware,
                                    allow_credentials=True,
                                    allow_origins=['*'],
                                    allow_methods=["*"],
                                    allow_headers=["*"])

        @self.fastapi.get('/pybrook-schema.json', response_model=PyBrookSchema)
        def get_schema():
            return self.schema

    async def redis_dependency(self) -> AsyncIterator[aioredis.Redis]:
        """Redis FastAPI Dependency"""
        redis = await aioredis.from_url(self.brook.redis_url,
                                        encoding='utf-8',
                                        decode_responses=True)
        yield redis
        await redis.close()
        await redis.connection_pool.disconnect()

    def visit(self, generator: RouteGenerator):
        generator.gen_routes(self,
                             redis_dep=fastapi.Depends(self.redis_dependency))


class PyBrook:
    def __init__(self,
                 redis_url: str,
                 api_class: Type[PyBrookApi] = PyBrookApi):
        self.inputs: Dict[str, Type[InReport]] = {}
        self.outputs: Dict[str, Type[OutReport]] = {}
        self.artificial_fields: Dict[str, ArtificialField] = {}
        self.consumers: List[BaseStreamConsumer] = []
        self.redis_url: str = redis_url
        self.api: PyBrookApi = api_class(self)
        self.manager = None

    @property
    def app(self) -> fastapi.FastAPI:
        return self.api.fastapi

    def run(self):
        self.manager = WorkerManager(self.consumers)
        self.manager.run()

    def terminate(self):
        if not self.manager:
            raise RuntimeError('PyBrook is not running!')
        self.manager.terminate()

    def _gen_field_info(self, field: ReportField) -> FieldInfo:
        return FieldInfo(stream_name=field.destination_stream_name,
                         field_name=field.destination_field_name)

    def set_meta(self, *, latitude_field: ReportField,
                 longitude_field: ReportField, group_field: ReportField,
                 time_field: ReportField):
        self.api.schema.latitude_field = self._gen_field_info(latitude_field)
        self.api.schema.longitude_field = self._gen_field_info(longitude_field)
        self.api.schema.group_field = self._gen_field_info(group_field)
        self.api.schema.time_field = self._gen_field_info(time_field)

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
        def wrapper(cls) -> TO:
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
        self.consumers.append(consumer)
