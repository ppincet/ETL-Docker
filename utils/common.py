from utils import constants
def heartbeatWrapper(step, details='default'):
    return {
        'Message__c' : 'Heart beat',
        'Step__c' : step,
        'Details__c' : details,
        'Process_Name__c' : constants.ETL_PROCESS,
        'Status__c' : constants.ETL_SUCCESS
    }

def crushWrapper(trace, step=''):
    print('from crusher')
    print(f'step :{step} \n len:{len(step)}')
    return {
        'Message__c' : constants.ETL_CRITICAL,
        'Step__c' : step,
        'Details__c' : trace,
        'Process_Name__c' : constants.ETL_PROCESS,
        'Status__c' : constants.ETL_FAIL
    }
