import os

import pantab
from tableauhyperapi import TableName


def convert_to_csv(list_of_files):
    """
    Leverages pantab and pandas to convert a .hyper file to a df, and then convert
    the df to a csv file.
    """
    path = os.getcwd()
    table_name = TableName("Extract", "Extract")

    for hyperfile in list_of_files:
        file_name = hyperfile.replace(".hyper", "")
        df = pantab.frame_from_hyper(path + "\\" + hyperfile, table=table_name)
        csv_file_path = path + "\\output\\" + file_name + ".csv"
        df.to_csv(csv_file_path)


current_path = os.getcwd()
path_files = os.listdir(current_path)
list_of_files = list()

for file in path_files:
    file_ext = file[-6:]
    if file_ext == ".hyper":
        list_of_files.append(file)

convert_to_csv(list_of_files)
