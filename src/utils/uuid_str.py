import uuid
import hashlib

#Create UUID based on list of string
#Source:
#1. generate uuid from str https://samos-it.com/posts/python-create-uuid-from-random-string-of-words.html
#2. convert list to str https://www.geeksforgeeks.org/python/python-program-to-convert-a-list-to-string/
#3. UUID documentation https://docs.python.org/3/library/uuid.html
def create_uuid_from_list(lst:list[str])->uuid:
    #Convert list to string
    str_value=''.join(lst).lower()

    #Create hexadecimal string
    hex_string = hashlib.md5(str_value.encode("UTF-8")).hexdigest()

    #Create UUID value
    my_uuid=uuid.UUID(hex=hex_string)
    my_uuid=str(my_uuid)

    return my_uuid