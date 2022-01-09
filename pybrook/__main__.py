import sys
from importlib import import_module, reload
from multiprocessing import Process

from loguru import logger
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


class ModelChangeEventHandler(FileSystemEventHandler):
    def __init__(self, brook):
        self.brook = brook

    def on_modified(self, event):
        logger.info('File change detected, reloading...')
        self.brook.terminate()
        main()


m = None


def main():
    global m
    app_arg = sys.argv[1].split(':')
    m = import_module(app_arg[0]) if not m else reload(m)
    brook = getattr(m, app_arg[1]) if len(app_arg) > 1 else getattr(m, 'brook')
    observer = Observer()
    observer.schedule(ModelChangeEventHandler(brook), m.__file__)
    observer.start()
    brook.run()
    observer.stop()
    observer.join()


if __name__ == '__main__':
    main()
