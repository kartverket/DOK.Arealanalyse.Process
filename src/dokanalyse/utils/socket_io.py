import socketio
from .helpers.common import get_env_var
   
def get_client():
    try:
        url = get_env_var('SOCKET_IO_SRV_URL')
        sio = socketio.SimpleClient()
        sio.connect(url, socketio_path='/ws/socket.io')
        
        return sio
    except:
        return None