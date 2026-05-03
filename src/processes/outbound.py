from datetime import datetime
from utils import zip, ssh, constants, force
from config import settings
from pathlib import Path
import logging

def process(sf_conn, sftp_out, strict = False):
    if settings.DEBUG == True:
        logger = logging.getLogger(__name__)
        logger.info('outbound')
        # print('outbound')
    # return
    mapping = adjust_mapping(force.get_mappings(sf_conn)['Back'])
    conns = {
    'sftp_client': sftp_out,  
    'sf_client': sf_conn      
}
 
    zip.process_sftp_to_sf(conns,mapping)

def adjust_mapping(victim):
    '''
    returns back new dict upon file name w/o .csv
    '''
    adjusted = {}
    for dev_name, item in victim.items():
        file_name = item.get('header', {}).get('file', dev_name) + '.csv'
        adjusted[file_name] = item
    return adjusted



