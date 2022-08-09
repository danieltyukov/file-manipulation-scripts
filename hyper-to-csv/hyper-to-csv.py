import pantab
from tableauhyperapi import TableName

table_name = TableName("Extract", "Extract")
hyper_name = "federated_0dppysj0xiytrz1fcj8v40"
file_name = "federated_0dppysj0xiytrz1fcj8v40.hyper"

df = pantab.frame_from_hyper(file_name, table=table_name)
df.to_csv("output/{}.csv".format(hyper_name))