# Purpose: Get json.Description for a rule 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/14/2023 (htu) - 
#     1. ported from proc_rules_sdtm.py and modulized as get_desc
#     2. included the dependent methods: get_jmsg, compose_desc, and 
#        replace_operator
#   03/15/2023: added "import re" 
#   03/21/2023 (htu) - 
#     1. added docstring and test cases
#     2. combine the compose_desc into replace_operator 
#     3. extracted out replace_operator function 
#     4. extracted out get_jmsg
#   03/22/2023 (htu) - added exist_rule_data
#   04/11/2023 (htu) - added r_std 
#    
#
import pandas as pd
import os 
from rulebuilder.echo_msg import echo_msg
from rulebuilder.get_jmsg import get_jmsg
from rulebuilder.get_existing_rule import get_existing_rule


def get_desc(rule_data, exist_rule_data: dict = {}, r_std:str=None):
    """
    Returns a string describing the trigger condition based on the given rule data.

    Parameters:
    ----------
    rule_data: dataframe
        A Pandas DataFrame containing the rule data.

    existing_rule_data: dict 
        a data frame containing all the records for a rule that already developed. It 
        can be read from the existing rule folder using get_existing_rule. 

    Returns:
        A string describing the trigger condition.

    Raises:
        None.
    """
    v_prg = __name__
    v_stp = 1.0
    v_msg = "Getting Rule Description..."
    echo_msg(v_prg, v_stp, v_msg, 2)
    v_desc = exist_rule_data.get("json", {}).get("Description")
    if v_desc is not None:
        return v_desc
    
    print(f"RuleData: {rule_data}")

    v_stp = 2.0
    r_std = os.getenv("r_standard") if r_std is None else r_std
    r_std = r_std.upper()
    v_msg = f"From {r_std} rule definition..."
    if r_std == "SDTM_V2_0":
        v_stp = 2.1
        jmsg = get_jmsg(rule_data,exist_rule_data=exist_rule_data,r_std=r_std)
        # print(f"jmsg: {jmsg}")  # Debugging print statement
        v_desc = "Trigger error when " + jmsg
        # print(f"desc: {desc}")  # Debugging print statement
        # Disable the description for SDTM 
        v_desc = None 
    elif r_std == "FDA_VR1_6":
        v_stp = 2.2
        # print(f"VDesc: {rule_data.iloc[0]}")
        v_desc = rule_data.iloc[0]["FDA Validator Rule Description"]
        
    return v_desc

# Test cases
if __name__ == "__main__":
    v_prg = __name__ + "::get_desc"
    os.environ["g_lvl"] = "1"
    r_dir = "/Volumes/HiMacData/GitHub/data/core-rule-builder"
    existing_rule_dir = r_dir + "/data/output/json_rules1"

    # Create a test dataframe

    # Test case 1: Test the function with a given rule data
    v_stp = 1.0
    echo_msg(v_prg, v_stp, "Test Case 01: Basic Parameter", 1)
    data = {'Condition': ['a = b'], 'Rule': ['c in (1, 2, 3)']}
    df = pd.DataFrame(data)
    v_desc = get_desc(df,{})
    assert get_desc(df) == 'Trigger error when a not equal to b and c not in (1, 2, 3)'

    # Test case 2: Test the function with None values in the rule data
    v_stp = 2.0
    echo_msg(v_prg, v_stp, "Test Case 02: With on rule", 1)
    data2 = {'Condition': [None], 'Rule': ['a = b']}
    df2 = pd.DataFrame(data2)
    rule_id = "CG0001"
    d2_data = get_existing_rule(rule_id, existing_rule_dir)
    v_desc = get_desc(df2,d2_data)
    print(f"Test 02: {v_desc}")
    # assert get_desc(df2) == 'Trigger error when a not equal to b'

    print("All test cases passed.")
