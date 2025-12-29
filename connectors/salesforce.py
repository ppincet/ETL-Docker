import requests
import time
import jwt 
from config import settings
from simple_salesforce import Salesforce
import functools
import requests

_sf_instance = None
def get_instance():
    global _sf_instance
    if _sf_instance is None:
        session = requests.Session()
        session.request = functools.partial(session.request, timeout=15)
        payload = {
            'iss': settings.CONSUMER_KEY,
            'sub': settings.USERNAME,
            'aud': settings.LOGIN_URL,
            'exp': int(time.time()) + 300  
        }
        
        encoded_token = jwt.encode(
            payload, 
            settings.PRIVATE_KEY, 
            algorithm='RS256'
        )

        response = session.post(
            f"{settings.LOGIN_URL}/services/oauth2/token",
            data={
                'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
                'assertion': encoded_token
            }
        )
        if response.status_code != 200:
            raise Exception(f"❌ Authentication Failed: {response.text}")
        auth_response = response.json()
        _sf_instance = Salesforce(instance_url=auth_response['instance_url'], 
            session_id=auth_response['access_token'],
            session=session)
    return _sf_instance
