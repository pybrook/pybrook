"""
This module is responsible for parsing report processing models.

It is the core of PyBrook.
"""
#  PyBrook
#
#  Copyright (C) 2023  Michał Rokita
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#
#
#  This program is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with this program.  If not, see <https://www.gnu.org/licenses/>.
#

import asyncio
import dataclasses
import inspect
import signal
from itertools import chain
from pathlib import Path
from time import time
from typing import (  # noqa: WPS235
    Any, AsyncIterator, Callable, Dict, Generic, List, Mapping, Optional,
    Sequence, Type, TypeVar, Union, get_type_hints,
)

import redis.asyncio as aioredis
import fastapi
import pydantic
import redis
from loguru import logger
from starlette.middleware.cors import CORSMiddleware
from starlette.staticfiles import StaticFiles
from websockets.exceptions import ConnectionClosedOK

from pybrook.config import (
    MSG_ID_FIELD,
    SPECIAL_CHAR,
    WEBSOCKET_PING_INTERVAL,
    WEBSOCKET_WAIT_TIME,
    WEBSOCKET_XREAD_BLOCK,
    WEBSOCKET_XREAD_COUNT,
)
from pybrook.consumers.base import BaseStreamConsumer
from pybrook.consumers.dependency_resolver import DependencyResolver
from pybrook.consumers.field_generator import (
    AsyncFieldGenerator,
    BaseFieldGenerator,
    SyncFieldGenerator,
)
from pybrook.consumers.splitter import Splitter
from pybrook.consumers.worker import ConsumerConfig, WorkerManager
from pybrook.encoding import decode_stream_message, encode_stream_message
from pybrook.schemas import FieldInfo, PyBrookSchema, StreamInfo


class ConsumerGenerator:
    """
    An interface describing objects that can generate stream consumers/producers.
    """
    @classmethod
    def gen_consumers(cls, model: 'PyBrook'):
        """
        Implementations of this method should add consumers to the [PyBrook][pybrook.models.PyBrook] instance passed as `model`.

        Args:
            model: The PyBrook instance processed.

        Raises:
            NotImplementedError: When used directly.
        """
        raise NotImplementedError


class RouteGenerator:
    """
    An interface describing objects that can generate API endpoints.
    """
    @classmethod
    def gen_routes(cls, api: 'PyBrookApi', redis_dep: aioredis.Redis):
        """
        Implementations of this method should add API endpoints
        to the [PyBrookApi][pybrook.models.PyBrookApi] instance passed as `api`.

        Args:
            api: The PyBrookApi instance processed.
            redis_dep: FastAPI redis dependency.

        Raises:
            NotImplementedError: When used directly.
        """
        raise NotImplementedError


class Registrable:
    """
    An interface describing objects that need to perform some tasks, after the initial processing stage is completed.

    This is used to solve issues like circular (recursive) references.
    """
    def on_registered(self, model: 'PyBrook'):
        """
        Used mostly to evaluate lazy stuff.

        Args:
            model: The model to register at.

        Raises:
            NotImplementedError: When used directly.
        """
        raise NotImplementedError


DTYPE = TypeVar('DTYPE')
"""
A TypeVar used by [dependency][pybrook.models.dependency]
and [historal_dependency][pybrook.models.historical_dependency],
enable mypy to detect issues with dependency types.
"""


def dependency(src: DTYPE) -> DTYPE:
    """
    Depending on `src`, the injected dependency will be value of some field (artificial or loaded from report)
    or a Redis instance.

    See [Dependency][pybrook.models.Dependency] for implementation details.

    Args:
        src: Target of the dependency.
             Should be an instance of [SourceField][pybrook.models.SourceField]
             or a Redis class (`aioredis.Redis` or `redis.Redis`).
    Returns:
         A [HistoricalDependency][pybrook.models.HistoricalDependency],
         but mypy is made to believe that the type the returned value is [src.value_type][pybrook.models.SourceField.value_type].
    """
    dep: DTYPE
    dep = Dependency(src)  # type: ignore
    return dep  # noqa: WPS331


def historical_dependency(src: DTYPE, history_length: int) -> Sequence[DTYPE]:
    """
    See [HistoricalDependency][pybrook.models.HistoricalDependency] for implementation details.

    Args:
        src: Target of the dependency.
             Should be an instance of [SourceField][pybrook.models.SourceField].
             You can also reference artificial fields by passing their name,
             which is useful for recursive dependencies.
    Returns:
        A [HistoricalDependency][pybrook.models.HistoricalDependency],
        but mypy is made to believe that it's a sequence containing items
        of type of the corresponding [SourceField][pybrook.models.SourceField].
    """
    dep: Sequence[DTYPE]
    dep = HistoricalDependency(  # type: ignore
        src,  # type: ignore
        history_length=history_length)
    return dep  # noqa: WPS331


DependencySource = Union['SourceField', Type[aioredis.Redis],
                         Type[redis.Redis]]


class Dependency(Registrable):
    """
    This class represents a dependency of an [ArtificialField][pybrook.models.ArtificialField].

    In theory, it can be used directly as an argument default of a calculation function,
    but it is recommended to use the [dependency][pybrook.models.dependency] callable,
    which provides mypy compatibility instead.
    """
    def __init__(self, src: DependencySource):
        """
        See [dependency][pybrook.models.dependency].
        """
        self.is_aioredis: bool = False
        self.is_redis: bool = False
        self.src_field: Optional[SourceField] = self.validate_source_field(src)
        self.is_historical = False

    @property
    def value_type(self):
        """Value type of the source field."""
        return self.src_field.value_type

    def validate_source_field(self, src: DependencySource):
        self.is_aioredis = type(src) == type and issubclass(  # noqa: WPS516
            src, aioredis.Redis)
        self.is_redis = type(src) == type and issubclass(  # noqa: WPS516
            src, redis.Redis)
        if isinstance(src, SourceField):
            return src
        elif not (self.is_aioredis or self.is_redis):
            raise ValueError(
                f'{src} is not an instance of SourceField or a Redis class')

    def on_registered(self, model: 'PyBrook'):
        """
        Artificial fields can be evaluated lazily, that's why this is required.

        Args:
            model: model at which the field has been registered

        Raises:
            ValueError: when src_field is not an ArtificialField instance
        """
        if isinstance(self.src_field, str):
            try:
                self.src_field = model.artificial_fields[self.src_field]
            except KeyError as e:
                raise ValueError(
                    f'Lazy evaluation is only supported for artificial fields,'
                    f' and {self.src_field} is not one of these.') from e

    def __repr__(self):
        if self.is_aioredis:
            return '<Dependency aioredis>'
        if self.is_redis:
            return '<Dependency redis>'
        return f'<Dependency src_field={self.src_field}>'


class HistoricalDependency(Dependency):
    """
    This class represents a historical dependency of an [ArtificialField][pybrook.models.ArtificialField].

    In theory, it can be used directly as an argument default of a calculation function,
    but it is recommended to use the [historical_dependency][pybrook.models.historical_dependency] callable,
    which provides mypy compatibility instead.
    """
    def __init__(self, src_field: Union['SourceField', str],
                 history_length: int):
        """
        See [historical_dependency][pybrook.models.historical_dependency].
        """
        super().__init__(src_field)  # type: ignore
        self.history_length = history_length
        self.is_historical = True

    @property
    def value_type(self):
        return List[Union[self.src_field.value_type, None]]

    def validate_source_field(self, src: DependencySource):
        if isinstance(src, str):
            return src
        return super().validate_source_field(src)


class SourceField:
    """
    A base class for fields ([InputField][pybrook.models.InputField], [ArtificialField][pybrook.models.ArtificialField])

    Provides metadata used to generate producers & consumers.
    """
    def __init__(self,
                 field_name: str,
                 *,
                 value_type: Type,
                 source_obj: Type['InReport'] = None):
        """
        Args:
            value_type: Field value type.
            field_name: Source field.
            source_obj: InReport that the field belongs to.
        """
        self.field_name: str = field_name
        self.source_obj: Optional[Type[InReport]] = source_obj
        self._value_type: Type = value_type

    @property
    def value_type(self):
        """Value type of the source field."""
        return self._value_type

    @property  # type: ignore
    def stream_name(self) -> str:
        if self.source_obj:
            return f'{SPECIAL_CHAR}{self.source_obj.pybrook_options.name}{SPECIAL_CHAR}split'
        return f'{SPECIAL_CHAR}artificial{SPECIAL_CHAR}{self.field_name}'

    def __repr__(self):
        return f'<{self.__class__.__name__} name={self.field_name},' \
               f' report_class={self.source_obj.__name__ if self.source_obj else "artificial"},' \
               f' value_type={self.value_type.__name__}>'


class ReportField:
    def __init__(self, source_field: Union[SourceField, Any]):
        if not isinstance(source_field, SourceField):
            raise RuntimeError(f'{source_field} is not a SourceField')
        self.destination_field_name: str
        self.source_field = source_field
        self.owner: Type[OutReport]

    def __repr__(self):
        return f'<{self.__class__.__name__} ' \
               f'destination_field_name=\'{self.destination_field_name}\' ' \
               f'source_field={self.source_field} ' \
               f'owner={self.owner}>'

    @property
    def destination_stream_name(self):
        return self.owner.stream_name

    def set_context(self, owner: Type['OutReport'], name: str):
        self.owner = owner
        self.destination_field_name = name


@dataclasses.dataclass
class InReportOptions:
    id_field: str
    name: str

    @property
    def stream_name(self):
        return f'{SPECIAL_CHAR}{self.name}'


TOPT = TypeVar('TOPT')
"""
A TypeVar used by [OptionsMixin][pybrook.models.OptionsMixin],
to define the type of the Options.
"""


class OptionsMixin(Generic[TOPT]):
    """
    A mixin used by metaclasses [InReportMeta][pybrook.models.InReportMeta] and [OutReportMeta][pybrook.models.OutReportMeta].

    It's responsibility is to add a `pybrook_options` property, that allows setting & validating passed options.
    """
    @property
    def pybrook_options(self) -> TOPT:
        """A property containing report options, like `id_field` or `name`."""
        return self._options

    @pybrook_options.setter
    def pybrook_options(self, options: TOPT):
        options = self._validate_options(options)
        self._options = options  # noqa: WPS112

    def _validate_options(self, options: TOPT) -> TOPT:
        raise NotImplementedError


class InReportMeta(OptionsMixin[InReportOptions],
                   pydantic.main.ModelMetaclass):
    pybrook_options: InReportOptions
    _input_fields: Dict[str, 'InputField']

    def __new__(mcs, name, bases, namespace):  # noqa: N804
        """
        Initialize an InReport subclass by generating a dictionary
        of [InputFields][pybrook.models.InputField] from the Pydantic fields provided.
        """
        cls = super().__new__(mcs, name, bases, namespace)  # noqa: WPS117
        cls._input_fields = {}
        for prop_name, field in cls.__fields__.items():
            cls._input_fields[prop_name] = InputField(cls,
                                                      field)  # type: ignore
        return cls

    def __getattr__(cls, item: str) -> SourceField:  # noqa: N805
        """This enables the `Model.field` syntax used for references in PyBrook models."""
        if not item.startswith('_') and item in cls._input_fields:
            return cls._input_fields[item]
        return super().__getattribute__(item)  # noqa: WPS613

    def _validate_options(
            cls, options: InReportOptions) -> InReportOptions:  # noqa: N805
        """Validate options set by [PyBrook.input()][pybrook.models.PyBrook.input]"""
        try:
            getattr(cls, options.id_field)
        except AttributeError:
            raise RuntimeError(
                f'Invalid id_field! {cls.__name__} '
                f'has no attribute "{options.id_field}".') from None
        return options


class InReport(ConsumerGenerator,
               RouteGenerator,
               pydantic.BaseModel,
               metaclass=InReportMeta):
    @classmethod
    def gen_consumers(cls, model: 'PyBrook'):
        splitter = Splitter(
            redis_url=model.redis_url,
            object_id_field=cls.pybrook_options.id_field,
            consumer_group_name=f'{cls.pybrook_options.name}{SPECIAL_CHAR}sp',
            namespace=cls.pybrook_options.name,
            input_streams=[cls.pybrook_options.stream_name])
        model.add_consumer(splitter)

    @classmethod
    def gen_routes(cls, api: 'PyBrookApi', redis_dep: aioredis.Redis):
        @api.fastapi.post(f'/{cls.pybrook_options.name}',
                          name=f'Add {cls.pybrook_options.name}')
        async def add_report(
                report: cls = fastapi.Body(...),  # type: ignore
                redis_conn: aioredis.Redis = redis_dep):
            await redis_conn.xadd(
                cls.pybrook_options.stream_name,
                encode_stream_message(
                    report.dict(by_alias=False))  # type: ignore
            )


@dataclasses.dataclass
class OutReportOptions:
    name: str

    @property
    def stream_name(self):
        return f'{SPECIAL_CHAR}{self.name}'


class OutReportMeta(OptionsMixin[OutReportOptions], type):
    _report_fields: Mapping[str, 'ReportField']
    _model: Type[pydantic.BaseModel]
    pybrook_options: OutReportOptions

    def __new__(mcs, name, bases, namespace):  # noqa: N804
        """
        Initialize an [OutReport][pybrook.models.OutReport] subclass by processing all of its ReportFields,
        and initializing the [pydantic_model][pybrook.models.OutReportMeta.pydantic_model] property.
        """
        cls = super().__new__(mcs, name, bases, namespace)  # noqa: WPS117
        cls._report_fields = {}
        for prop_name, report_field in inspect.getmembers(cls):
            if isinstance(report_field, ReportField):
                report_field.set_context(cls, prop_name)
                cls._report_fields[prop_name] = report_field
        pydantic_fields = {
            rep_field.destination_field_name:
            (rep_field.source_field.value_type, pydantic.Field())
            for rep_field in cls._report_fields.values()  # type: ignore
        }
        pydantic_fields[MSG_ID_FIELD] = (
            str,
            pydantic.Field(
                title='Message ID',
                description=(f'Message id - {{object ID}}'
                             f'{SPECIAL_CHAR}{{msg index for object}}')))
        cls._model = pydantic.create_model(
            cls.__name__ + 'Model',
            **pydantic_fields  # type: ignore
        )
        return cls

    @property
    def stream_name(cls):
        return cls.pybrook_options.stream_name

    @property  # type: ignore
    def pydantic_model(cls) -> Type[pydantic.BaseModel]:
        """
        A Pydantic model describing the output report.
        """
        pydantic_fields = {
            report_field.destination_field_name:
            (report_field.source_field.value_type, pydantic.Field())
            for report_field in cls._report_fields.values()  # type: ignore
        }
        pydantic_fields[MSG_ID_FIELD] = (
            str,
            pydantic.Field(
                title='Message ID',
                description=(f'Message id - {{object ID}}'
                             f'{SPECIAL_CHAR}{{msg index for object}}')))
        if not hasattr(cls, '_model'):
            cls._model: Type[pydantic.BaseModel] = pydantic.create_model(
                cls.__name__ + 'Model',
                **pydantic_fields  # type: ignore
            )
        return cls._model

    def _validate_options(cls, options: OutReportOptions) -> OutReportOptions:
        return options


class OutReport(ConsumerGenerator, RouteGenerator, metaclass=OutReportMeta):
    @classmethod
    def gen_routes(  # noqa: WPS217, WPS231
            cls, api: 'PyBrookApi', redis_dep: aioredis.Redis):
        model_cls = cls.pydantic_model

        @api.fastapi.get(
            f'/{cls.pybrook_options.name}',
            response_model=model_cls,  # type: ignore
            name=f'Retrieve {cls.pybrook_options.name}')
        async def get_report(redis_conn: aioredis.Redis = redis_dep):
            messages = await redis_conn.xrevrange(
                cls.pybrook_options.stream_name, count=1)
            for _msg_id, msg_body in messages:  # noqa: WPS328
                return model_cls(**decode_stream_message(msg_body))
            return {}

        @api.fastapi.websocket(f'/{cls.pybrook_options.name}')
        async def read_reports(  # noqa: WPS231
                websocket: fastapi.WebSocket,
                redis_conn: aioredis.Redis = redis_dep):
            await websocket.accept()
            last_msg = '$'
            stream_name = cls.pybrook_options.stream_name
            active = True
            last_ping = time()
            while active and api.fastapi.state.socket_active:
                if time() - last_ping > WEBSOCKET_PING_INTERVAL:
                    try:
                        # Check if connection is active
                        await asyncio.wait_for(websocket.receive_bytes(),
                                               timeout=WEBSOCKET_WAIT_TIME)
                    except asyncio.TimeoutError:
                        ...  # Everything is OK
                    except (fastapi.WebSocketDisconnect, AssertionError):
                        active = False
                    else:
                        last_ping = time()
                messages = await redis_conn.xread({stream_name: last_msg},
                                                  count=WEBSOCKET_XREAD_COUNT,
                                                  block=WEBSOCKET_XREAD_BLOCK)
                if messages:
                    # Wiadomości w strumieniach są mapami
                    for m_data in dict(messages)[stream_name]:
                        last_msg, payload = m_data
                        try:
                            await websocket.send_text(
                                model_cls(
                                    **decode_stream_message(payload)).json())
                        except ConnectionClosedOK:
                            active = False
                        except RuntimeError:
                            active = False
            try:
                await websocket.close()
            except RuntimeError:
                logger.info('WebSocket connection closed by client')
            else:
                logger.info('WebSocket connection closed by server')

        api.schema.streams.append(
            StreamInfo(stream_name=cls.pybrook_options.stream_name,
                       websocket_path=f'/{cls.pybrook_options.name}',
                       report_schema=cls.pydantic_model.schema()))

    @classmethod
    def gen_consumers(cls, model: 'PyBrook'):
        dependency_resolver = DependencyResolver(
            redis_url=model.redis_url,
            output_stream_name=cls.pybrook_options.stream_name,
            dependencies=[
                DependencyResolver.Dep(
                    src_stream=field.source_field.stream_name,
                    src_key=field.source_field.field_name,
                    dst_key=field.destination_field_name)
                for field in cls._report_fields.values()
            ],
            resolver_name=cls.pybrook_options.name)
        model.add_consumer(dependency_resolver)


class InputField(SourceField):
    def __init__(self, report_class: Type[InReport],
                 pydantic_field: pydantic.fields.ModelField):
        super().__init__(pydantic_field.name,
                         value_type=pydantic_field.type_,
                         source_obj=report_class)


class ArtificialField(SourceField, Registrable, ConsumerGenerator):
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
        all_defaults_are_deps = all(
            isinstance(d, Dependency) for k, d in self.dependencies.items())
        if not all_defaults_are_deps:
            raise RuntimeError(
                f'Artificial field "{self.field_name}" has default values'
                f' which do not subclass Dependency.')
        self.calculate = calculate

    def __call__(self, *args, **kwargs):
        return self.calculate(*args, **kwargs)

    @property
    def regular_dependencies(self) -> Dict[str, Dependency]:
        return {
            k: d
            for k, d in self.dependencies.items()
            if not isinstance(d, HistoricalDependency)
        }

    @property
    def historical_dependencies(self) -> Dict[str, HistoricalDependency]:
        return {
            k: d
            for k, d in self.dependencies.items()
            if isinstance(d, HistoricalDependency)
        }

    def on_registered(self, model: 'PyBrook'):
        for dep in self.dependencies.values():
            dep.on_registered(model)

    def gen_consumers(self, model: 'PyBrook'):  # type: ignore
        dependency_resolver = DependencyResolver(
            redis_url=model.redis_url,
            output_stream_name=(f'{SPECIAL_CHAR}{self.field_name}'
                                f'{SPECIAL_CHAR}deps'),
            dependencies=[
                DependencyResolver.Dep(src_stream=dep.src_field.stream_name,
                                       src_key=dep.src_field.field_name,
                                       dst_key=dep_key)
                for dep_key, dep in self.regular_dependencies.items()
                if dep.src_field
            ],
            historical_dependencies=[
                DependencyResolver.HistoricalDep(
                    src_stream=dep.src_field.stream_name,
                    src_key=dep.src_field.field_name,
                    dst_key=dep_key,
                    history_length=dep.history_length)
                for dep_key, dep in self.historical_dependencies.items()
                if dep.src_field
            ],
            resolver_name=self.field_name)
        model.add_consumer(dependency_resolver)

        field_generator_deps = [
            BaseFieldGenerator.Dep(name=dep_name, value_type=dep.value_type)
            for dep_name, dep in self.dependencies.items()
            if dep.src_field
        ]

        generator_class: Type[
            BaseFieldGenerator] = AsyncFieldGenerator if self.is_coro else SyncFieldGenerator

        field_generator = generator_class(
            redis_url=model.redis_url,
            dependency_stream=dependency_resolver.output_stream_name,
            field_name=self.field_name,
            generator=self.calculate,
            dependencies=field_generator_deps,
            redis_deps=[
                key for key, dep in self.dependencies.items()
                if (dep.is_aioredis and self.is_coro) or dep.is_redis
            ])

        model.add_consumer(field_generator)


TI = TypeVar('TI', bound=Type[InReport])

TO = TypeVar('TO', bound=Type[OutReport])


class PyBrookApi:
    """
    Represents the HTTP API.

    The default implementation is based on FastAPI.
    """
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

        @self.fastapi.on_event('startup')
        async def startup():
            frontend_dir = str(Path(__file__).parent / 'frontend')
            self.fastapi.mount('/panel/',
                               StaticFiles(directory=frontend_dir, html=True),
                               name='static')
            self.fastapi.state.redis = await aioredis.from_url(
                self.brook.redis_url, encoding='utf-8', decode_responses=True)
            self.fastapi.state.socket_active = True
            signal.signal(signal.SIGINT, shutdown)
            signal.signal(signal.SIGTERM, shutdown)

        @self.fastapi.on_event('shutdown')
        def shutdown(*args):
            logger.info('set socket active to false')
            self.fastapi.state.socket_active = False
            asyncio.create_task(self.fastapi.state.redis.close())  # noqa: WPS219
            asyncio.create_task(self.fastapi.state.redis.connection_pool.  # noqa: WPS219
                                disconnect())

    async def redis_dependency(self) -> AsyncIterator[aioredis.Redis]:
        """
        Redis FastAPI Dependency

        Yields:
            A Redis connection
        """
        yield self.fastapi.state.redis

    def visit(self, generator: RouteGenerator):
        """Visits a route generator to add new endpoints."""
        generator.gen_routes(self,
                             redis_dep=fastapi.Depends(self.redis_dependency))


class PyBrook:
    """This class represents a PyBrook model."""
    def __init__(self,
                 redis_url: str,
                 api_class: Type[PyBrookApi] = PyBrookApi):
        """
        Args:
            redis_url: Url of the Redis Gears server.
            api_class: API class - you can pass your own implementation of
            [PyBrookApi][pybrook.models.PyBrookApi] to modify the generated FastAPI app.
        """
        self.inputs: Dict[str, Type[InReport]] = {}
        self.outputs: Dict[str, Type[OutReport]] = {}
        self.artificial_fields: Dict[str, ArtificialField] = {}
        self.consumers: List[BaseStreamConsumer] = []
        self.redis_url: str = redis_url
        self.api: PyBrookApi = api_class(self)
        self.manager: Optional[WorkerManager] = None

    def process_model(self):
        if not self.consumers:
            report_classes = chain(self.inputs.values(), self.outputs.values())
            for report_class in report_classes:
                self.visit(report_class)
            for field in self.artificial_fields.values():
                self.visit(field)

    @property
    def app(self) -> fastapi.FastAPI:
        self.process_model()
        return self.api.fastapi

    def run(self, config: Dict[str, ConsumerConfig] = None):
        """
        Runs the workers.

        Args:
            config: Consumer config, can be skipped - defaults will be used. See [pybrook.__main__][pybrook.__main__] for details.
        """

        config = config or {}
        self.process_model()
        self.manager = WorkerManager(self.consumers, config=config)
        self.manager.run()

    def terminate(self):
        """Terminates all worker processes gracefully."""
        if not self.manager:
            raise RuntimeError('PyBrook is not running!')
        self.manager.terminate()

    def set_meta(self,
                 *,
                 latitude_field: ReportField,
                 longitude_field: ReportField,
                 group_field: ReportField,
                 time_field: ReportField,
                 direction_field: ReportField = None):
        """Use this method to set metadata used by frontend."""
        self.api.schema.latitude_field = self._gen_field_info(latitude_field)
        self.api.schema.longitude_field = self._gen_field_info(longitude_field)
        self.api.schema.group_field = self._gen_field_info(group_field)
        self.api.schema.time_field = self._gen_field_info(time_field)
        if direction_field:
            self.api.schema.direction_field = self._gen_field_info(
                direction_field)

    def input(  # noqa: A003
            self, name: str = None, *, id_field: str) -> Callable[[TI], TI]:
        """
        Register an input report.

        Returns:
            A decorator, which accepts an InReport as an argument.
        """
        def wrapper(cls) -> TI:
            name_safe = name or cls.__name__
            self.inputs[name_safe] = cls
            cls.pybrook_options = InReportOptions(id_field=id_field,
                                                  name=name_safe)
            self.api.visit(cls)
            return cls

        return wrapper

    def output(self, name: str = None) -> Callable[[TO], TO]:
        """
        Register an output report.

        Returns:
            A decorator, which accepts an OutReport as an argument.
        """
        def wrapper(cls) -> TO:
            name_safe = name or cls.__name__
            self.outputs[name_safe] = cls
            cls.pybrook_options = OutReportOptions(name=name_safe)
            self.api.visit(cls)
            return cls

        return wrapper

    def artificial_field(self, name: str = None) -> Callable[[Callable], Any]:
        """
        Register an artificial field.

        Returns:
            A decorator, which accepts a callable as an argument.
            The callable provided is used to calculate the value of the artificial field.
        """
        def wrapper(fun: Callable) -> Any:
            field = ArtificialField(fun, name=name)
            self.artificial_fields[name or fun.__name__] = field
            field.on_registered(self)
            return field

        return wrapper

    def visit(self, generator: ConsumerGenerator):
        """Visit a consumer generator and let it generate consumers."""
        generator.gen_consumers(self)

    def add_consumer(self, consumer: BaseStreamConsumer):
        """
        This is used by consumer generators.

        For now it just adds a new consumer to the consumer list.
        """

        self.consumers.append(consumer)

    def _gen_field_info(self, field: ReportField) -> FieldInfo:
        return FieldInfo(stream_name=field.destination_stream_name,
                         field_name=field.destination_field_name)
