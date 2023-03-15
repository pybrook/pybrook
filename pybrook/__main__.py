"""PyBrook CLI entrypoint."""
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
            brook: A PyBrook instance.
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
    """

    Args:
        parser: The main argument parser.
        consumers: List of consumers to generate CLI options for.

    Returns:
        A dictionary of consumer configs filled with defaults. Consumer group names are used as keys.

    """
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
    """
    Updates `workers_config` with settings loaded from argparse arguments.

    Args:
        args: An argparse `Namespace`
        workers_config:  A dictionary of consumer configs to update using settings loaded from the `args` argument.

    Returns:

    """
    for c in workers_config.keys():
        for arg in ('workers', ):
            arg_name: str = c.replace('-', '_') + '_' + arg
            setattr(workers_config[c], arg, getattr(args, arg_name))


def main():
    """
    CLI Entrypoint.

    Starts PyBrook workers.

    Examples:

        ```bash
        ❯ pybrook pybrook.examples.demo:brook --help
        usage: pybrook [-h]
               [--location-report:dr-workers LOCATION_REPORT:DR_WORKERS]
               [--direction-report:dr-workers DIRECTION_REPORT:DR_WORKERS]
               [--brigade-report:dr-workers BRIGADE_REPORT:DR_WORKERS]
               [--direction:dr-workers DIRECTION:DR_WORKERS]
               [--direction:fg-workers DIRECTION:FG_WORKERS]
               APP

        positional arguments:
          APP

        options:
          -h, --help
          --location-report:dr-workers LOCATION_REPORT:DR_WORKERS
                                (default: 4)
          --direction-report:dr-workers DIRECTION_REPORT:DR_WORKERS
                                (default: 4)
          --brigade-report:dr-workers BRIGADE_REPORT:DR_WORKERS
                                (default: 4)
          --direction:dr-workers DIRECTION:DR_WORKERS
                                (default: 4)
          --direction:fg-workers DIRECTION:FG_WORKERS
                                (default: 4)

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
