# Purpose: Rename nested keys in a dictionary object
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/20/2023 (htu) - initial coding
#   03/29/2023 (htu) - 
#     1. added 2nd test case 
#     2. added code to check if it is a list 
#   03/30/2023 (htu) - 
#     1. fixed the missing strings and added CommentedSeq
#     2. added echo_msg 
#   04/04/2023 (htu) - added step 1.5 and "CommentedMap, dict" in step 1.41

import json
import re
# from io import StringIO
import sys
from rulebuilder.echo_msg import echo_msg
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq

def rename_keys(obj, a: str = "_", b: str = " "):
    v_prg = __name__
    v_stp = 1.0
    v_msg = "Rename Keys: (" + a + ") --> (" + b + ")..."
    echo_msg(v_prg, v_stp, v_msg, 4)
    v_stp = 1.1
    v_msg = " Obj Type = " + str(type(obj)) 
    echo_msg(v_prg, v_stp, v_msg, 5)
    if isinstance(obj, (str)):
        v_stp = 1.2
        v_msg = " . In Str: " + obj 
        echo_msg(v_prg, v_stp, v_msg, 5)
    elif isinstance(obj, (list, CommentedSeq)):
        v_stp = 1.3
        v_msg = " . In CommentedSeq" 
        echo_msg(v_prg, v_stp, v_msg, 5)
        for item in obj:
            rename_keys(item, a, b)
    elif isinstance(obj, (CommentedMap,dict)):
        v_stp = 1.4
        v_msg = " . In CommentedMap"
        echo_msg(v_prg, v_stp, v_msg, 5)
        for key, value in list(obj.items()):
            v_stp = 1.41
            if isinstance(value, (list, CommentedSeq, CommentedMap, dict)):
                rename_keys(value, a, b)
            new_key = key.replace(a, b)
            v_stp = 1.5
            v_msg = " . Keys: " + key + " --> " + new_key
            echo_msg(v_prg, v_stp, v_msg, 5)
            if new_key != key:
                obj[new_key] = obj.pop(key)
                if hasattr(obj, 'ca'):
                    obj.ca.items[new_key] = obj.ca.items.get(key)
                    if key in obj.ca.items:
                        del obj.ca.items[key]
    else:
        v_stp = 1.5
        v_msg = f"ERR: Object Type not being processed: {type(obj)}"
        echo_msg(v_prg, v_stp, v_msg, 5)

            
# End of rename_keys 

# Test cases
if __name__ == "__main__":
    # Test Case 01: convert _ to " "
    d = {'a_1': 1, 'b__1': {'c': 2, 'd___1': 3, 'e-1': {'f-1': 4, 'g__1': 5}}}
    d = rename_keys(d, "_", " ")
    print(d)
    # Output: {'a 1': 1, 'b  1': {'c': 2, 'd   1': 3, 'e 1': {'f 1': 4, 'g  1': 5}}}

    # Test Case 02: Convert " " to "_"
    b = {'a 1': 1, 'b  1': {'c': 2, 'd   1': 3, 'e 1': {'f 1': 4, 'g  1': 5}}}
    d = rename_keys(b, " ", "_")
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
    rename_keys(d1)
    # convert_spaces_to_underscores(d1)

    # Convert the data to a JSON string
    json_str = json.dumps(d1, indent=2)


    # Print the JSON string to the screen
    print(json_str)

    y.dump(d1, sys.stdout)



