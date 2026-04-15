import requests
import functools
import time
import jwt
from simple_salesforce import Salesforce
from config import settings

class SalesforceClient:
    def __init__(self):
        self._sf_instance = None

    def get_instance(self):
        if self._sf_instance is None:
            self.refresh_connection()
        return self._sf_instance

    def refresh_connection(self):
        print("🔄 Authenticating Salesforce...")
        if self._sf_instance and hasattr(self._sf_instance, 'session'):
            try:
                self._sf_instance.session.close()
            except Exception:
                pass
        self._sf_instance = None 
        session = requests.Session()
        session.request = functools.partial(session.request, timeout=60)
        self._sf_instance = None 

        session = requests.Session()
        session.request = functools.partial(session.request, timeout=60)

        payload = {
            'iss': settings.CONSUMER_KEY,
            'sub': settings.USERNAME,
            'aud': settings.LOGIN_URL,
            'exp': int(time.time()) + 300
        }
        encoded_token = jwt.encode(payload, settings.PRIVATE_KEY, algorithm='RS256')

        try:
            response = session.post(
                f"{settings.LOGIN_URL}/services/oauth2/token",
                data={
                    'grant_type': 'urn:ietf:params:oauth:grant-type:jwt-bearer',
                    'assertion': encoded_token
                }
            )

            if response.status_code == 200:
                auth_response = response.json()
                self._sf_instance = Salesforce(
                    instance_url=auth_response['instance_url'],
                    session_id=auth_response['access_token'],
                    session=session,
                    version=settings.SF_VERSION
                )
                print("✅ Salesforce Connected")
            else:
                raise Exception(f"❌ Auth Failed: {response.text}")
        except Exception as e:
            print(f"Connection error: {e}")
            raise e
