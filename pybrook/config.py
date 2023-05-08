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

WEBSOCKET_XREAD_COUNT = config('WEBSOCKET_XREAD_COUNT', int, default=1000)

WEBSOCKET_WAIT_TIME = config('WEBSOCKET_WAIT_TIME', float, default=0.01)

WEBSOCKET_PING_INTERVAL = config('WEBSOCKET_PING_INTERVAL', int, default=30)
