import socketio
import logging
from .constants import SOCKET_IO_SRV_URL

_LOGGER = logging.getLogger(__name__)


def get_client() -> socketio.SimpleClient:
    if not SOCKET_IO_SRV_URL:
        return None

    try:
        sio = socketio.SimpleClient()
        sio.connect(SOCKET_IO_SRV_URL, socketio_path='/ws/socket.io')

        return sio
    except Exception as error:
        _LOGGER.warning(error)
        return None


__all__ = ['get_client']
