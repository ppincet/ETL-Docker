from datetime import datetime
from utils import zip, ssh, constants, force
from config import settings
from pathlib import Path

def process(sf_conn, sftp_conn, strict = False):
  try:
    # sf = salesforce.get_instance()
    #conn = sftp.get_instance()
    # print(f'from process(sf): {sf_conn}')
    # print(f'from proceess(sftp):{sftp_conn}')
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    filename = f"./{settings.SSH_FILE_IPREFIX}-{timestamp}.zip"
    wm: dict[str, datetime] = {}
    print('from inbound:')
    if zip.upload_file(sf_conn,  filename, wm) == constants.ETL_SUCCESS:
      #print('----- zip is alive after force')
      print(f'filename from inbound:{filename}')
      #print(f'wm after zip: {wm}')
    #   if ssh.upload(sf_conn, sftp_conn, filename) == constants.ETL_SUCCESS:
    #     force.upsert_wm(sf_conn, wm)
    #     print('done from inbound')
    # try:
    #   Path(filename).unlink()
    # except FileNotFoundError:
    #  print('nothing to remove(from inbound)')
  except Exception as e:
    print(f'from inbound: {e}')




      





