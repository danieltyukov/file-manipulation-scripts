import re
my_str = "h1ey_ th~!e)(re"
my_new_string = re.sub('[^a-zA-Z0-9 \n\.]', '', my_str)
print(my_new_string)