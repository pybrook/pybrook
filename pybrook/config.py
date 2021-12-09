from starlette.config import Config

config = Config()

REDIS_URI = config('REDIS_URI', str, default='redis://localhost')

FIELD_PREFIX = config('INTERNAL_FIELD_PREFIX', str, default=':')
MSG_ID_FIELD = config('MSG_ID_FIELD', str, default=f'{FIELD_PREFIX}_msg_id')
ARTIFICIAL_NAMESPACE = config('ARTIFICIAL_NAMESPACE',
                              str,
                              default='artificial')
