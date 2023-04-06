# Purpose: Rename nested keys in a dictionary object
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/20/2023 (htu) - initial coding
#   03/29/2023 (htu) - 
#     1. added 2nd test case 
#     2. added code to check if it is a list 
#
#

import json
import re
# from io import StringIO
import sys
from ruamel.yaml import YAML

def rename_keys(d, regex:str=" ", replacement:str = "_", mapping=None):
    """
    Rename keys in a dictionary object recursively
    :param d: the input dictionary object
    :param mapping: a dictionary containing the old key names as keys and the new key names as values
    :param regex: a regular expression pattern to match against key names
    :param replacement: the character to replace the matched pattern with
    :return: the modified dictionary object
    """
    if mapping is None:
        mapping = {}
    # if isinstance(d, dict):
    #     for k1, v1 in list(d.items()):
    #         if k1 in mapping:
    #             k2 = mapping[k1]
    #         elif re.search(regex, k1):
    #             k2 = re.sub(regex, replacement, k1)
    #             # k2 = re.sub(re2,replacement,k2)
    #         else:
    #             k2 = k1
    #         d[k2] = d.pop(k1)
    #         if isinstance(v1, dict):
    #             d[k2] = rename_keys(v1, regex, replacement, mapping)
    #         elif isinstance(v1, list):
    #             for i, item in enumerate(v1):
    #                 v1[i] = rename_keys(item,  regex, replacement, mapping)
    if isinstance(d, list):
        for item in d:
            rename_keys(item, regex, replacement, mapping)
    elif isinstance(d, dict):
        for key, value in list(d.items()):
            # if key in mapping:
            #     new_key = mapping[key]
            # else: 
            new_key = key.replace(regex, replacement)
            # print(f"Key: {key}; New Key: {new_key}")
            if new_key != key:
                d[new_key] = d.pop(key)
                if hasattr(d, 'lc') and hasattr(d, 'ca'):
                    d.lc.move(new_key, key)
                    d.ca.items[new_key] = d.ca.items.pop(key)
            rename_keys(value, regex, replacement, mapping)
    return d



# Test cases
if __name__ == "__main__":
    # Test Case 01: convert _ to " "
    d = {'a_1': 1, 'b__1': {'c': 2, 'd___1': 3, 'e-1': {'f-1': 4, 'g__1': 5}}}
    m = {}
    d = rename_keys(d, "_", " ", m)
    print(d)
    # Output: {'a 1': 1, 'b  1': {'c': 2, 'd   1': 3, 'e 1': {'f 1': 4, 'g  1': 5}}}

    # Test Case 02: Convert " " to "_"
    b = {'a 1': 1, 'b  1': {'c': 2, 'd   1': 3, 'e 1': {'f 1': 4, 'g  1': 5}}}

    d = rename_keys(b, " ", "_", m)
    print(d)

    # Test Case 03: Convert keys in YAML string to JSON 
    y = YAML()
    y.indent(mapping=2, sequence=4, offset=2)
    y.preserve_quotes = True
    y_str = """\
# example 1
person:
  - full name: Smith Alice
    # details
    first name: John  # very common
    last name: Smith  # last name - Smith
    # Age is numerical
    age: 18  # Age is 18
    hobbies:
        1st hobby: car
        2nd hobby: garden
  - full name: Tom Evans
    first name: Tom
    last name: Evans
    age: 20
"""

    from ruamel.yaml.comments import CommentedMap

    def convert_spaces_to_underscores(obj, a: str=" ", b: str="_"):
        if isinstance(obj, list):
            for item in obj:
                convert_spaces_to_underscores(item)
        elif isinstance(obj, CommentedMap):
            for key, value in list(obj.items()):
                new_key = key.replace(a, b)
                if new_key != key:
                    obj[new_key] = obj.pop(key)
                    if hasattr(obj, 'ca'):
                        obj.ca.items[new_key] = obj.ca.items.get(key)
                        if key in obj.ca.items:
                            del obj.ca.items[key]
                convert_spaces_to_underscores(value)

    # Load the YAML string
    d1 = y.load(y_str)

    # Add the comment for Tom
    d1['person'][1].yaml_add_eol_comment(
        "This is a comment for Tom", 'first name')

    # Dump the modified YAML content back to a string
    y.dump(d1, sys.stdout)

    # rename the keys
    m = {"first name":"1st name", "last name":"family name"}
    # rename_keys(d1, " ", "_")
    convert_spaces_to_underscores(d1)
    # convert_spaces_to_underscores(d1)

    # Convert the data to a JSON string
    json_str = json.dumps(d1, indent=2)


    # Print the JSON string to the screen
    print(json_str)

    y.dump(d1, sys.stdout)



