from datetime import datetime
from utils import zip, ssh, constants, force
from config import settings
from pathlib import Path
import logging

def process(sf_conn, sftp_conn, strict = False):
  logger = logging.getLogger(__name__)
  #return
  try:
    timestamp = datetime.now().strftime(f"%Y%m%d{settings.ZIP_NAME_SEPARATOR}%H%M%S")
    filename = f"./{settings.SSH_FILE_IPREFIX}{settings.ZIP_NAME_SEPARATOR}{timestamp}.zip"
    wm: dict[str, datetime] = {}

    logger.info(f"filename: {filename}")
    print(f'from inbound(filename){filename}:')
    
    if zip.upload_file(sf_conn,  filename, wm) == constants.ETL_SUCCESS:
      logger.info('----- zip is alive after force')
      logger.info(f'filename from inbound:{filename}')
      if ssh.upload(sf_conn, sftp_conn, filename) == constants.ETL_SUCCESS:
        print('done upload before wm')
        force.upsert_wm(sf_conn, wm)
        print('done from inbound')
    try:
      Path(filename).unlink()
    except FileNotFoundError:
     print('nothing to remove(from inbound)')
  except Exception as e:
    print(f'❌from inbound exc: {e}')




      





