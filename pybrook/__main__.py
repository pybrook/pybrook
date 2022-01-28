import sys
from importlib import import_module, reload
from typing import Union

from loguru import logger
from watchdog.events import FileSystemEventHandler, FileModifiedEvent, DirModifiedEvent
from watchdog.observers import Observer


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


def main():
    """
    CLI Entrypoint for now.

    Starts PyBrook worker, takes just one argument for now - `<module>:<pybrook_attribute>`.

    Examples:
        ```bash
        pybrook pybrook.examples.buses:brook
        ```
    """
    app_arg = sys.argv[1].split(':')
    model_module = import_module(app_arg[0])
    modified = True
    while modified:
        brook = getattr(model_module,
                        app_arg[1]) if len(app_arg) > 1 else model_module.brook
        handler = ModelChangeEventHandler(brook)
        observer = Observer()
        observer.schedule(handler, model_module.__file__)  # noqa: WPS609
        observer.start()
        brook.run()
        observer.stop()
        observer.join()
        reload(model_module)
        modified = handler.modified


if __name__ == '__main__':
    main()
