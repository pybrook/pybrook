import argparse
from importlib import import_module, reload
from typing import Dict, List, Union

from loguru import logger
from watchdog.events import DirModifiedEvent, FileModifiedEvent, FileSystemEventHandler
from watchdog.observers import Observer

from pybrook.consumers.base import BaseStreamConsumer, GearsStreamConsumer
from pybrook.consumers.worker import ConsumerConfig
from pybrook.models import PyBrook


class ModelChangeEventHandler(FileSystemEventHandler):
    """
    Handles model hot-reloading.
    """
    def __init__(self, brook):
        """

        Args:
            brook: a PyBrook instance.
        """
        self.brook = brook
        self.modified = False

    def on_modified(self, event: Union[DirModifiedEvent, FileModifiedEvent]):
        """

        Args:
            event: Event representing file/directory modification.
        """
        logger.info('File change detected, reloading...')
        self.modified = True
        self.brook.terminate()


def add_consumer_args(
        parser: argparse.ArgumentParser,
        consumers: List[BaseStreamConsumer]) -> Dict[str, ConsumerConfig]:
    workers_config = {}
    for c in consumers:
        if not isinstance(c, GearsStreamConsumer):
            consumer_config = ConsumerConfig()
            parser.add_argument(
                f'--{c.consumer_group_name}-workers',
                type=int,
                help='(default: %(default)s)',  # noqa: WPS323
                default=consumer_config.workers)
            workers_config[c.consumer_group_name] = consumer_config
    return workers_config


def update_workers_config(args: argparse.Namespace,
                          workers_config: Dict[str, ConsumerConfig]):
    for c in workers_config.keys():
        for arg in ('workers', ):
            arg_name: str = c.replace('-', '_') + '_' + arg
            setattr(workers_config[c], arg, getattr(args, arg_name))


def main():
    """
    CLI Entrypoint for now.

    Starts PyBrook worker, takes just one argument for now - `<module>:<pybrook_attribute>`.

    Examples:
        ```bash
        pybrook pybrook.examples.buses:brook
        ```
    """

    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('-h', '--help', action='store_true')
    parser.add_argument('APP', nargs=1)
    args: argparse.Namespace
    unknown: List[str]
    args, unknown = parser.parse_known_args()
    app_arg = args.APP[-1].split(':') if args.APP else None
    if not app_arg and args.help:
        parser.print_help()
        return
    model_module = import_module(app_arg[0])
    modified = True
    while modified:
        brook: PyBrook = getattr(
            model_module,
            app_arg[1]) if len(app_arg) > 1 else model_module.brook
        brook.process_model()
        workers_config = add_consumer_args(parser, brook.consumers)
        args = parser.parse_args()
        if args.help:
            parser.print_help()
            return
        update_workers_config(args, workers_config)
        handler = ModelChangeEventHandler(brook)
        observer = Observer()
        observer.schedule(handler, model_module.__file__)  # noqa: WPS609
        observer.start()
        brook.run(config=workers_config)
        observer.stop()
        observer.join()
        reload(model_module)
        modified = handler.modified


if __name__ == '__main__':
    main()
