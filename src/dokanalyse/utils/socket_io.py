import os
import socketio
   
def get_client():
    try:
        url = os.environ.get('SOCKET_IO_URL', 'http://0.0.0.0:5002')
        sio = socketio.SimpleClient()
        sio.connect(url, socketio_path='/ws/socket.io')
        
        return sio
    except:
        return None