import paramiko
from config import settings
_sftp_instance = None
_transport = None
def get_instance():
    global _sftp_instance
    global _transport
    if _transport is not None and _transport.is_active():
        return _sftp_instance
    _transport = paramiko.Transport((settings.SSH_HOST, 
                                     settings.SSH_PORT))
    _transport.connect(username=settings.SSH_USERNAME, 
                       password=settings.SSH_PASSWORD)
    _sftp_instance = paramiko.SFTPClient.from_transport(_transport)
    return _sftp_instance

'''
import paramiko
from config import settings

_sftp_instance = None
_transport = None

def get_instance():
    global _sftp_instance, _transport
    if _transport is not None and _transport.is_active():
        return _sftp_instance
    try:
        # 2. Add a socket timeout (e.g., 15 seconds)
        # This prevents the TCP handshake from hanging forever
        _transport = paramiko.Transport((settings.SSH_HOST, 22))
        _transport.banner_timeout = 15 # Timeout for the SSH version exchange
        
        # 3. Connect with an authentication timeout
        _transport.connect(
            username=settings.SSH_USERNAME, 
            password=settings.SSH_PASSWORD
        )
        
        _sftp_instance = paramiko.SFTPClient.from_transport(_transport)
        return _sftp_instance

    except Exception as e:
        # Clean up partial states if connection fails
        close_connection()
        raise e

def close_connection():
    global _sftp_instance, _transport
    try:
        if _sftp_instance:
            _sftp_instance.close()
        if _transport:
            _transport.close()
    except:
        pass
    finally:
        _sftp_instance = None
        _transport = None
'''

