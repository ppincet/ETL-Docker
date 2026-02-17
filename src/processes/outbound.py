from datetime import datetime
from utils import zip, ssh, constants, force
from config import settings
from pathlib import Path

def process(sf_conn, sftp_conn, strict = False):
    return
    zip.process_sftp_to_sf(sftp_conn, sf_conn)


