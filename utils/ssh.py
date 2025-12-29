import os
import traceback
from utils import common, force, constants
from config import settings
def upload(sf, sftp, file_name):
    remote_folder = settings.SSH_REMOTE_IFOLDER
    status = constants.ETL_SUCCESS
    try:
        print(f'remote folder:{remote_folder}')
        current_dir = sftp.getcwd() or ''
        if remote_folder and remote_folder not in current_dir:
            print(f'Changing folder from {current_dir} to {remote_folder}')
            sftp.chdir(remote_folder)

        # print(f'remote folder:{remote_folder}')
    # except IOError as e:
    #     print(f"{e}")
    #     # status = constants.ETL_FAILED
    #     # common.crushWrapper('',  ):
    #try:
        filename_only = os.path.basename(file_name)
        print(f'filename only:{filename_only}')
        sftp.put(file_name, filename_only)
    except Exception as e:
        print(f"SFTP Error: {e}")
        status = constants.ETL_FAIL
        traceback.print_exc()
        force.log(sf, common.crushWrapper(str(e), constants.ETL_SFTP_FAIL))
    finally:
        # if sftp: sftp.close()
        return status
    