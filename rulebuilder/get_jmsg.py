# Purpose: Get json.Message for a rule
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/21/2023 (htu) - extracted out from get_desc 
#   03/22/2023 (htu) - added exist_rule_data 
#

import pandas as pd
import os
from rulebuilder.echo_msg import echo_msg
from rulebuilder.replace_operator import replace_operator


def get_jmsg(rule_data, exist_rule_data: dict = {}):
    """
    Returns a JSON message string based on the given rule data.

    Paramgers:
    ----------
    rule_data: dataframe 
        A Pandas DataFrame containing the rule data.
        
    existing_rule_data: dict 
        a data frame containng all the records for a rule that already developed. It 
        can be read from the existing rule folder using get_existing_rule. 

    Returns:
        A JSON message string.

    Raises:
        None.
    """
    v_prg = __name__
    v_stp = 1.0
    v_msg = "Getting Message for json.Message..."
    echo_msg(v_prg, v_stp, v_msg, 3)
    r_str = exist_rule_data.get("json", {}).get("Message")
    if r_str is None: 
        r_condition = rule_data.iloc[0]["Condition"]
        r_rule = rule_data.iloc[0]["Rule"]
        # Debugging print statement
        v_stp = 1.2
        v_msg = " . r_condition: " + str(r_condition) + ", r_rule: " + r_rule
        echo_msg(v_prg, v_stp, v_msg, 4)
        r_desc1 = replace_operator(r_condition)
        r_desc2 = replace_operator(r_rule)
        # Debugging print statement
        v_stp = 1.3
        v_msg = " . r_desc1: " + str(r_desc1) + ", r_desc2: " + str(r_desc2)
        echo_msg(v_prg, v_stp, v_msg, 4)
        r_str = r_desc2 if r_desc1 is None else r_desc1 + " and " + r_desc2
        v_stp = 1.4
        v_msg = " . r_desc3: " + str(r_str)        # Debugging print statement
        echo_msg(v_prg, v_stp, v_msg, 4)
    return r_str


# Test cases
if __name__ == "__main__":
    v_prg = __name__ + "::get_jmsg"
    os.environ["g_lvl"] = "1"
    v_stp = 1.0
    echo_msg(v_prg, v_stp, "Test Case 01: Basic Parameter",1)

    # Create a test dataframe
    data = {'Condition': ['a = b'], 'Rule': ['c in (1, 2, 3)']}
    df = pd.DataFrame(data)

    # Test case 1: Test the function with a given rule data
    assert get_jmsg(df) == 'a not equal to b and c not in (1, 2, 3)'

    # Test case 2: Test the function with None values in the rule data
    data2 = {'Condition': [None], 'Rule': ['a = b']}
    df2 = pd.DataFrame(data2)
    # s2 = get_jmsg(df2)
    # print(f" . S2: {s2}")
    assert get_jmsg(df2) == 'a not equal to b'

    # Test case 3: Test the function with an unknown operator in the rule data
    data3 = {'Condition': ['a ^ b'], 'Rule': ['c in (1, 2, 3)']}
    df3 = pd.DataFrame(data3)
    assert get_jmsg(df3) == 'a ^ b and c not in (1, 2, 3)'

    # Test case 4: Test the function with multiple conditions and rules
    data4 = {'Condition': ['a = b', 'c = d'], 'Rule': ['e in (1, 2, 3)', 'f = g']}
    df4 = pd.DataFrame(data4)
    assert get_jmsg(df4) == 'a not equal to b and e not in (1, 2, 3)'

    print("All test cases passed.")


    