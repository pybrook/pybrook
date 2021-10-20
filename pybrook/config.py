from starlette.config import Config

config = Config()

REDIS_URI = config('REDIS_URI', str, default='redis://localhost')

INTERNAL_FIELD_PREFIX = config('INTERNAL_FIELD_PREFIX', str, default='@')
OBJECT_ID_FIELD = config('OBJECT_ID_FIELD',
                         str,
                         default=f'{INTERNAL_FIELD_PREFIX}_obj_id')
MSG_ID_FIELD = config('MSG_ID_FIELD',
                      str,
                      default=f'{INTERNAL_FIELD_PREFIX}_msg_id')
