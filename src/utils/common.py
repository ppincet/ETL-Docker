from utils import constants
from dataclasses import dataclass
def heartbeatWrapper(step, details='default'):
    return {
        'Message__c' : 'Heart beat',
        'Step__c' : step,
        'Details__c' : details,
        'Process_Name__c' : constants.ETL_PROCESS,
        'Status__c' : constants.ETL_SUCCESS
    }

def crushWrapper(trace, step=''):
    print('from crusher -- test')
    print(f'step :{step} \n len:{len(step)}')
    return {
        'Message__c' : constants.ETL_CRITICAL,
        'Step__c' : step,
        'Details__c' : trace,
        'Process_Name__c' : constants.ETL_PROCESS,
        'Status__c' : constants.ETL_FAIL
    }

@dataclass  
class ZipRange:
    start: int
    end: int
    zip_name: str
    def contains(self, index: int) -> bool:
        return self.start <= index <= self.end

