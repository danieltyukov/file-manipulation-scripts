from __future__ import unicode_literals
import os, codecs
import tableaudocumentapi
import tableauserverclient as TSC
from dotenv import load_dotenv

load_dotenv()

USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')
SITE_NAME = os.getenv('SITE_NAME')

TABLEAU_SERVER = "https://dub01.online.tableau.com"
TABLEAU_SITE_ID = SITE_NAME
TABLEAU_USER = USER
TABLEAU_PASSWORD = PASSWORD
DOWNLOAD_DIR = "/download_dir"

print(TABLEAU_USER)
print(TABLEAU_PASSWORD)

def main():
    tableau_auth = TSC.TableauAuth(username=TABLEAU_USER, password=TABLEAU_PASSWORD, site_id=TABLEAU_SITE_ID)
    server = TSC.Server(server_address=TABLEAU_SERVER)

    #    Download all Workbooks From Server
    with server.auth.sign_in(tableau_auth):
        download_all_workbooks(server, DOWNLOAD_DIR)
        put_all_workbook_fields_to_csv(DOWNLOAD_DIR)


# download all workbooks from server
def download_all_workbooks(server, download_dir):
    for server_workbook in TSC.Pager(server.workbooks):
        wb_download_dir = os.path.join(download_dir, server_workbook.project_name)
        if not os.path.exists(wb_download_dir):
            os.makedirs(wb_download_dir)
        path = server.workbooks.download(server_workbook.id, filepath=wb_download_dir, include_extract=False)
        print("Downloaded workbook to {}".format(path))


# get all fields from workbooks in the directory
def put_all_workbook_fields_to_csv(workbooks_dir):
    field_list_file = workbooks_dir + '/all_workbook_fields.csv'

    with codecs.open(field_list_file, 'w', encoding='utf-8') as output_file:
        # Write Header
        seperator = ';'
        output_file.write('sep=' + seperator + '\n')
        output_file.write(';'.join(['Data_Source_Name'
                                       , "Workbook_Name"
                                       , "Worksheet_Name"
                                       , "Field_Name"
                                       , "Field_Aggregation"
                                       , "Field_Alias"
                                       , "Field_Calculation"
                                       , "Field_Datatype"
                                       , "Field_Description"
                                       , "Field_Id"
                                       , "Field_Role"
                                       , "Field_Type"
                                    ]) + "\n")

        # process all workbooks one by one
        for root, directories, filenames in os.walk(workbooks_dir):
            for filename in filenames:
                if "twb" in filename:
                    # read metadata of workbook
                    myWB = tableaudocumentapi.workbook.Workbook(os.path.join(root, filename))
                    for datasource in myWB.datasources:
                        for key, field in datasource.fields.iteritems():
                            if len(field.worksheets) > 0:
                                for worksheet in field.worksheets:
                                    output_file.write(
                                        FieldToCSVStr(field, datasource, myWB, worksheet, seperator=seperator))
                            else:
                                output_file.write(
                                    FieldToCSVStr(field, datasource, myWB, worksheet='', seperator=seperator))
    output_file.close()


def NoneToStr(string):
    return (string or '').replace('\r\n', ' ').replace('\n', ' ').replace('\t', ' ')


def FieldToCSVStr(field, datasource, workbook, worksheet='', seperator='\t'):
    return seperator.join([NoneToStr(datasource.caption or datasource.name)
                              , NoneToStr(os.path.basename(workbook.filename))
                              , NoneToStr(worksheet)
                              , NoneToStr(field.caption)
                              , NoneToStr(field._aggregation)
                              , NoneToStr(field.alias)
                              , NoneToStr(field.calculation)
                              , NoneToStr(field.datatype)
                              , NoneToStr(field.description)
                              , NoneToStr(field.id)
                              , NoneToStr(field.role)
                              , NoneToStr(field._type)
                           ]) + '\n'


if __name__ == '__main__':
    main()