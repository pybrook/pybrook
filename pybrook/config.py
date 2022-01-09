from starlette.config import Config

config = Config()

REDIS_URI = config('REDIS_URI', str, default='redis://localhost')

SPECIAL_CHAR = config('SPECIAL_CHAR', str, default=':')
MSG_ID_FIELD = config('MSG_ID_FIELD', str, default=f'{SPECIAL_CHAR}_msg_id')
ARTIFICIAL_NAMESPACE = config('ARTIFICIAL_NAMESPACE',
                              str,
                              default='artificial')
