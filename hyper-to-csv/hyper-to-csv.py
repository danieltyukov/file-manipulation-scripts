import os

import pantab
from tableauhyperapi import TableName

table_name = TableName("Extract", "Extract")
hyper_name = "federated_0adm2280nevxa21fel48n0"
file_name = "federated_0adm2280nevxa21fel48n0.hyper"

path = os.getcwd()
file_name = path + "\\"+"federated_0adm2280nevxa21fel48n0.hyper"

df = pantab.frame_from_hyper(file_name, table=table_name)
df.to_csv("output/{}.csv".format(hyper_name))