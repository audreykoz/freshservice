import csv2cmdb as csv2
import difflib
import pandas as pd

data = input("Relationship file:")
rela_data = pd.read_csv(data)
rela_dict = {}
for item in range(len(csv2.get_rela_types())):
    number = csv2.get_rela_types()[item]["id"]
    name = csv2.get_rela_types()[item]["forward_relationship"]
    print(name.replace(" ", ""))
    match = difflib.get_close_matches(name.replace(" ", "")+"Relationship",rela_data["Type"].unique(), n = 1)
    print(match)
    if match == []:
        pass
    else:
        rela_dict[match[0]]  =  number
print(rela_dict)