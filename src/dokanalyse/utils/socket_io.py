import socketio
from .helpers.common import get_env_var
import logging

_LOGGER = logging.getLogger(__name__)

def get_client():
    try:
        url = get_env_var('SOCKET_IO_SRV_URL')
        sio = socketio.SimpleClient()
        sio.connect(url, socketio_path='/ws/socket.io')

        return sio
    except Exception as error:
        _LOGGER.warning(error)
        return None
