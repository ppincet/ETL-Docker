import os
from pathlib import Path
from dotenv import load_dotenv

try:
    from dotenv import load_dotenv
    env_path = Path(__file__).resolve().parent.parent.parent / '.env'
    if env_path.exists():
        load_dotenv(dotenv_path=env_path, override=True)
except ImportError:
    print('error')
    pass
try:
    CONSUMER_KEY = os.environ['SF_CONSUMER_KEY']
    USERNAME     = os.environ['SF_USERNAME']
    LOGIN_URL    = os.environ['SF_LOGIN_URL']
    _raw_key = os.environ['SF_PRIVATE_KEY']
    PRIVATE_KEY = _raw_key.replace('\\n', '\n') if '\\n' in _raw_key else _raw_key
    SSH_USERNAME = os.environ['SSH_USERNAME']
    SSH_PASSWORD = os.environ['SSH_PASSWORD']
    SSH_HOST     = os.environ['SSH_HOST']

except KeyError as e:
    raise RuntimeError(f"❌ CRITICAL ERROR: Missing environment variable {e}") from e
WINDOW             = float(os.getenv('WINDOW', 120)) 
DEBUG              = os.getenv('DEBUG', 'False').lower() == 'true'
SSH_REMOTE_IFOLDER = os.getenv('SSH_REMOTE_IFOLDER', '/inbound')
SSH_REMOTE_UFOLDER = os.getenv('SSH_REMOTE_UFOLDER', '/outbound')
SSH_FILE_IPREFIX   = os.getenv('SSH_FILE_IPREFIX', 'data_')
BUFFER_SIZE = int(os.getenv('BUFFER_SIZE', 2000))
ZIP_NAME_SEPARATOR = os.getenv('ZIP_NAME_SEPARATOR', '-')
INTEGRATION_USER_ID = os.getenv('INTEGRATION_USER_ID')
MAPPINGS_TTL = float(os.getenv('MAPPINGS_TTL', 2000))
MANIFEST_TTL = float(os.getenv('MANIFEST_TTL', 86400))
