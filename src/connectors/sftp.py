import paramiko
from config import settings

def get_new_instance():
    transport = paramiko.Transport((settings.SSH_HOST, settings.SSH_PORT))
    transport.set_keepalive(5) 
    transport.connect(
        username=settings.SSH_USERNAME, 
        password=settings.SSH_PASSWORD
    )
    print('new sftp conn')
    return paramiko.SFTPClient.from_transport(transport)

def ensure_connection(client):
    try:
        if client and client.get_channel() and client.get_channel().get_transport().is_active():
            client.listdir('.')
            # print(f'alive sftp{client}')
            return client
    except Exception:
        # logger.warning("SFTP connection lost. Reconnecting...")
        print(f'from sftp {client}- SFTP connection lost. Reconnecting...')
        raise
    return get_new_instance()

