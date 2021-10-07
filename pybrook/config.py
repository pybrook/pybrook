from starlette.config import Config

config = Config()

REDIS_URI = config('REDIS_URI', str, default='redis://localhost')
