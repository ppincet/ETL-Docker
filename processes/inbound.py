from datetime import datetime
from connectors import salesforce, sftp
from utils import zip, ssh, constants, force
from config import settings
from pathlib import Path

def process():
  sf = salesforce.get_instance()
  conn = sftp.get_instance()
  timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
  filename = f"./{settings.SSH_FILE_IPREFIX}-{timestamp}.zip"
  wm: dict[str, datetime] = {}
  
  if zip.upload_file(sf,  filename, wm) == constants.ETL_SUCCESS:
    print(f'filename:{filename}')
    if ssh.upload(sf, conn, filename) == constants.ETL_SUCCESS:
      force.upsert_wm(sf, wm)
      print('done')
  try:
    Path(filename).unlink()
  except FileNotFoundError:
    print('nothing to remove')
      





