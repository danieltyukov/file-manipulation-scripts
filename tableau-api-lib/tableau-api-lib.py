import os

from tableau_api_lib import TableauServerConnection
from dotenv import load_dotenv

import tableauserverclient as TSC



config = load_dotenv()

USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')
TABLEAU_SERVER = os.getenv('TABLEAU_SERVER')
API_VERSION = os.getenv('API_VERSION')
SITE_NAME = os.getenv('SITE_NAME')
SITE_URL = os.getenv('SITE_URL')
SITE_ID = os.getenv('SITE_ID')
TOKEN_NAME = os.getenv('TOKEN_NAME')
TOKEN_SECRET = os.getenv('TOKEN_SECRET')

tableau_config = {
    'tableau_dev': {
        'server': TABLEAU_SERVER,
        'api_version': API_VERSION,
        'personal_access_token_name': TOKEN_NAME,
        'personal_access_token_secret': TOKEN_SECRET,
        #'username': USER,
        #'password': PASSWORD,
        'site_name': SITE_ID,
        'site_url':  SITE_ID,
        #'cache_buster': '',
        #'temp_dir': ''
    }
}

connection = TableauServerConnection(config_json=tableau_config, env='tableau_dev')
response = connection.sign_in()
res = connection.query_site()
res.json()
print("")