import csv
import json
import os
import re

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
        'site_url': SITE_ID,
    }
}


class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r


def get_all_views_from_API():
    URL = "https://dub01.online.tableau.com/api/3.16/sites/852849f3-36fa-4b62-b361-c08af85c8472/views"
    # Conect to Tableau with json configu
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


all_views_by_SITE = get_all_views_from_API()

views_filtered_for_pandemic_workbook = list()

for element in all_views_by_SITE:
    if element["workbook"]["@id"] == "bef3fbe0-e37b-4095-ab05-26d308f6c5ff":
        views_filtered_for_pandemic_workbook.append(element)

connection = TableauServerConnection(config_json=tableau_config, env='tableau_dev')
response = connection.sign_in()

pandemic_query_view_data = list()
csv_info_pandemic_list = list()

# TODO Review this for because we are making a request for each view it could take time

for view in views_filtered_for_pandemic_workbook:
    try:
        # Get Details of the specific view
        view_id = view["@id"]
        # get View  """Queries details for the specified view."""
        pandemic_query_view_data_details_from_view = connection.get_view(view_id)
        pandemic_query_view_data_decoded = pandemic_query_view_data_details_from_view.content.decode('utf-8')
        data = json.loads(pandemic_query_view_data_decoded)
        view_name = data["view"]["name"]
        # query_viewdata """Queries the underlying data within the specified view."""
        pandemic_query_view_data = connection.query_view_data(view_id)
        print("Making Request to obtain data from view " + view_name)
        content_decoded = pandemic_query_view_data.content.decode('utf-8').splitlines()
        print("Pharsing Request of view " + view_name)
        with open("headers_from_all_views.csv", "a") as csv_file:
            writer = csv.writer(csv_file, delimiter='\t')
            header = content_decoded[0]
            writer.writerow(re.split('\s+', header))
            print("Saving data from view " + view_name)
    except:
        print("Error reading view from API")

print("Process completed")
