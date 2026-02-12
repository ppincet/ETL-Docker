#from simple_salesforce import Salesforce
#from connectors import salesforce


def process(sf_conn, sftp_conn, strict = False):
    if strict == False:
        print('outbound')