import os
from pathlib import Path
from dotenv import load_dotenv

try:
    print('before loading env')
    from dotenv import load_dotenv
    env_path = Path(__file__).resolve().parent.parent.parent / '.env'
    print(f'env path{env_path}')
    if env_path.exists():
        print(f"Loading environment from {env_path}")
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


# import os
# from pathlib import Path
# from dotenv import load_dotenv

# BASE_DIR = Path(__file__).resolve().parent.parent
# env_path = BASE_DIR / '.env'
# load_dotenv(dotenv_path=env_path)
# CONSUMER_KEY = os.getenv('SF_CONSUMER_KEY')
# USERNAME = os.getenv('SF_USERNAME')
# PRIVATE_KEY = os.getenv('SF_PRIVATE_KEY').replace('\\n', '\n') 
# LOGIN_URL = os.getenv('SF_LOGIN_URL')
# WINDOW = os.getenv('WINDOW')
# DEBUG = os.getenv('DEBUG')
# SSH_USERNAME = os.getenv('SSH_USERNAME')
# SSH_PASSWORD = os.getenv('SSH_PASSWORD')
# SSH_HOST = os.getenv('SSH_HOST')
# SSH_REMOTE_IFOLDER = os.getenv('SSH_REMOTE_IFOLDER')
# SSH_REMOTE_UFOLDER = os.getenv('SSH_REMOTE_UFOLDER')
# SSH_FILE_IPREFIX = os.getenv('SSH_FILE_IPREFIX')