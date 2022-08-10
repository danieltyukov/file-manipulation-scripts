import os
import requests
import xmltodict
from dotenv import load_dotenv
from requests.structures import CaseInsensitiveDict
from tableau_api_lib import TableauServerConnection

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
        'site_name': SITE_ID,
        'site_url':  SITE_ID,
    }
}

class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token
    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r

def get_all_views_from_API():
    # API
    URL = "https://dub01.online.tableau.com/api/3.16/sites/852849f3-36fa-4b62-b361-c08af85c8472/views"
    #Conect to Tableau with json configu
    connection = TableauServerConnection(config_json=tableau_config, env='tableau_dev')
    response = connection.sign_in()

    headers = CaseInsensitiveDict()
    token = str(connection.auth_token)
    response = requests.get(URL, auth=BearerAuth(token))
    decode_response = response.content.decode("utf-8")
    # sending get request and saving the response as response object
    obj = xmltodict.parse(decode_response)
    list_of_views = obj["tsResponse"]["views"]["view"]
    # extracting data in json format
    return list_of_views

#Retrieve all views from tableau

views = get_all_views_from_API()

view_id_list = list()

for element in views:
    view_id_list.append(element["@id"])

connection = TableauServerConnection(config_json=tableau_config, env='tableau_dev')
response = connection.sign_in()

pandemic_query_view_data = list()
csv_info_pandemic_list = list()

#TODO Review this for because we are making a request for each view it could take time
for view in view_id_list:
    pandemic_query_view_data = connection.query_view_data(view)
    csv_info_pandemic_list.append(pandemic_query_view_data.content.decode("utf-8"))

# view = connection.get_view_by_path("Pandemic_Program_Reporting")
#
# dict = {"sites":"alphait-daniel"}
#
# #SITE INFORMATION
#
# site = connection.query_site()
# site_info_decode = site.content.decode("utf-8")
# json_decoded_site = json.loads(site_info_decode)
# ID = json_decoded_site["site"]["id"]
#
#
# #VIEW INFORMATION
# #view = connection.get_view(ID)
# #Passing Id
# view_by_PATH = connection.get_view_by_path("852849f3-36fa-4b62-b361-c08af85c8472")
#
#
# #my_new_string_value =  view.content.decode("utf-8")
# workbook = connection.query_workbooks_for_site(dict)
# decoded_workbook = workbook.content.decode("utf-8")
# json_workbook = json.loads(decoded_workbook)
# pandemic_workbook_info = json_workbook["workbooks"]["workbook"][2]
# pandemic_workbook_view_id = json_workbook["workbooks"]["workbook"][2]["defaultViewId"]
# query_views_for_pandemic = connection.query_views_for_site(ID)
# #Retrieve Information in csv format
# pandemic_query_view_data = connection.query_view_data("eecd5bae-3da6-4019-a950-8ec69527947c")
# csv_info_pandemic = pandemic_query_view_data.content.decode("utf-8")
# #Views
#
# view = connection.get_view()
# site = connection.query_site()
#
#
# #res.json()




