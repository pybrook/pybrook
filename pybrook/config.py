from starlette.config import Config

config = Config()

REDIS_URI = config('REDIS_URI', str, default='redis://localhost')

SPECIAL_CHAR = config('SPECIAL_CHAR', str, default=':')
MSG_ID_FIELD = config('MSG_ID_FIELD', str, default=f'{SPECIAL_CHAR}_msg_id')
ARTIFICIAL_NAMESPACE = config('ARTIFICIAL_NAMESPACE',
                              str,
                              default='artificial')
DEFAULT_WORKERS = config('DEFAULT_WORKERS', int, default=4)

WEBSOCKET_XREAD_BLOCK = config('WEBSOCKET_XREAD_BLOCK', int, default=100)

WEBSOCKET_XREAD_COUNT = config('WEBSOCKET_XREAD_COUNT', int, default=100)

WEBSOCKET_WAIT_TIME = config('WEBSOCKET_WAIT_TIME', float, default=0.01)

WEBSOCKET_PING_INTERVAL = config('WEBSOCKET_PING_INTERVAL', int, default=30)
