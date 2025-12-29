import paramiko
import os
from config import settings
_sftp_instance = None
_transport = None
def get_instance():
    global _sftp_instance
    global _transport
    if _transport is not None and _transport.is_active():
        return _sftp_instance
    _transport = paramiko.Transport((settings.SSH_HOST, 22))
    _transport.connect(username=settings.SSH_USERNAME, password=settings.SSH_PASSWORD)
    print("Creating NEW SFTP connection...")
    _sftp_instance = paramiko.SFTPClient.from_transport(_transport)
    return _sftp_instance

