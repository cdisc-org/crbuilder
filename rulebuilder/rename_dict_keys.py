# Purpose: Rename nested keys in a dictionary object 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/20/2023 (htu) - initial coding 
#
#

from io import StringIO
import json
import sys 

from ruamel.yaml import YAML

def rename_dict_keys (a_dict, str_a: str="_", str_b: str=" "):
    """
    =================
    rename_dict_keys 
    =================
    This method renames keys nested in a python dictionary object. 

    Parameters:
    -----------
    a_dict: dict
        a dict object 

    returns
    -------
        dict_modified 

    Raises
    ------
    ValueError
        None

    """

    d = a_dict 
    for key in d.keys():
        if str_a in key:
            new_key = key.replace(str_a, str_b)
            d[new_key] = d.pop(key)
    return d 

# Output: {'key one': 1, 'key two': 2, 'key three': {'nested key one': 3, 'nested key two': 4}}

# Test cases
if __name__ == "__main__":
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


    def convert_spaces_to_underscores(obj):
        if isinstance(obj, list):
            for item in obj:
                convert_spaces_to_underscores(item)
        elif isinstance(obj, dict):
            for key, value in list(obj.items()):
                new_key = key.replace(" ", "_")
                if new_key != key:
                    obj[new_key] = obj.pop(key)
                convert_spaces_to_underscores(value)

    # Load the YAML string
    d1 = y.load(y_str)

    # Add the comment for Tom
    d1['person'][1].yaml_add_eol_comment("This is a comment for Tom", 'first name')

    # Dump the modified YAML content back to a string
    y.dump(d1, sys.stdout)

    # rename the keys 
    rename_dict_keys(d1, r'[ ]', "_")
    # convert_spaces_to_underscores(d1)

    # Convert the data to a JSON string
    json_str = json.dumps(d1, indent=2)

    # Print the JSON string to the screen
    print(json_str)

