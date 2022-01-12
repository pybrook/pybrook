import sys
from importlib import import_module, reload

from loguru import logger
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


class ModelChangeEventHandler(FileSystemEventHandler):
    def __init__(self, brook):
        self.brook = brook
        self.modified = False

    def on_modified(self, event):
        logger.info('File change detected, reloading...')
        self.modified = True
        self.brook.terminate()


def main():
    app_arg = sys.argv[1].split(':')
    model_module = import_module(app_arg[0])
    modified = True
    while modified:
        brook = getattr(model_module,
                        app_arg[1]) if len(app_arg) > 1 else getattr(
                            model_module, 'brook')
        handler = ModelChangeEventHandler(brook)
        observer = Observer()
        observer.schedule(handler, model_module.__file__)
        observer.start()
        brook.run()
        observer.stop()
        observer.join()
        reload(model_module)
        modified = handler.modified


if __name__ == '__main__':
    main()
