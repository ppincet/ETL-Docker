from datetime import datetime
from utils import zip, ssh, constants, force
from config import settings
from pathlib import Path
import logging

def process(sf_conn, sftp_in, strict = False):
  
  logger = logging.getLogger(__name__)
  # return
  
  timestamp = datetime.now().strftime(f"%Y%m%d{settings.ZIP_NAME_SEPARATOR}%H%M%S")
  filename = f"./{settings.SSH_FILE_IPREFIX}{settings.ZIP_NAME_SEPARATOR}{timestamp}.zip"
  wm: dict[str, datetime] = {}
  if settings.DEBUG == True:
    logger.info(f"filename: {filename}")
    print(f'from inbound(filename){filename}:')
  
  if zip.upload_file(sf_conn,  filename, wm) == constants.ETL_SUCCESS:
    if settings.DEBUG == True:
      logger.info('----- zip is alive after force')
      logger.info(f'filename from inbound:{filename}')
    if ssh.upload(sf_conn, sftp_in, filename) == constants.ETL_SUCCESS:
      if settings.DEBUG == True:
        print('done upload before wm')
      force.upsert_wm(sf_conn, wm)
      print('done from inbound')
    print(f'is debug:{settings.DEBUG}')
  # if settings.DEBUG == True:
  #   try:
  #     Path(filename).unlink()
  #   except FileNotFoundError:
  #     print('nothing to remove(from inbound)')

    




      





