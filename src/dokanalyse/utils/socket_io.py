import socketio
import structlog
from structlog.stdlib import BoundLogger
from ..constants import SOCKET_IO_SRV_URL

_logger: BoundLogger = structlog.get_logger(__name__)


def get_client() -> socketio.SimpleClient:
    if not SOCKET_IO_SRV_URL:
        return None

    try:
        sio = socketio.SimpleClient()
        sio.connect(SOCKET_IO_SRV_URL, socketio_path='/ws/socket.io')

        return sio
    except Exception as err:
        _logger.warning('Socket.IO connection failed', error=str(err))
        return None


__all__ = ['get_client']
