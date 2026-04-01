from datetime import datetime
from utils import zip, ssh, constants, force
from config import settings
from pathlib import Path
import logging

def process(sf_conn, sftp_conn, strict = False):
    if settings.DEBUG == True:
        logger = logging.getLogger(__name__)
        logger.info('outbound')
        print('outbound')
    #return
    zip.process_sftp_to_sf(sftp_conn, 
                           sf_conn,
                           adjust_mapping(force.get_mappings(sf_conn)['Back'])
                           )
def adjust_mapping(victim):
    '''
    returns back new dict upon file name w/o .csv
    '''
    adjusted = {}
    for dev_name, item in victim.items():
        file_name = item.get('header', {}).get('file', dev_name) + '.csv'
        adjusted[file_name] = item
    return adjusted



