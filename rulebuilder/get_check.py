# Purpose: Generate Check for a rule 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/14/2023 (htu) - ported from proc_rules_sdtm as get_check module
#   03/17/2023 (htu) - added docstring and test cases
#   03/23/2023 (htu) - added exist_rule_data to get existing check 
#   03/30/2024 (htu) - changed to use content YAML Check 
#    

import os
import json
import pandas as pd
from rulebuilder.echo_msg import echo_msg
from rulebuilder.read_rules import read_rules


def get_check(rule_data, exist_rule_data: dict = {}):
    """
    ===============
    get_check
    ===============
    This method builds the json.Check element in the core rule.

    Parameters:
    -----------
    rule_data: dataframe
        a data frame containng all the records for a rule. It can be obtained from
        read_rules and select the records from the rule definition data frame.

    returns
    -------
        r_json: json content for json.Check

    Raises
    ------
    ValueError
        None

    """
    v_prg = __name__ 
    v_stp = 1.0
    v_msg = "Setting parameters..."
    echo_msg(v_prg, v_stp, v_msg,2)
    v_stp = 1.1
    v_msg = "Input parameter rule_data is empty."
    if rule_data.empty:
        echo_msg(v_prg, v_stp, v_msg,0)
        return {}

    # r_json = exist_rule_data.get("json", {}).get("Check")
    r_json = exist_rule_data.get("Check")
    if r_json is not None:
        return r_json

    r_json = {
        "all": []
    }
    return r_json


# Test cases
if __name__ == "__main__":
    # set input parameters
    v_prg = __name__ + "::get_check"
    os.environ["g_lvl"] = "3"
    r_dir = "/Volumes/HiMacData/GitHub/data/core-rule-builder"
    yaml_file = r_dir + "/data/target/SDTM_and_SDTMIG_Conformance_Rules_v2.0.yaml"
    df_data = read_rules(yaml_file)

    # 1. Test with basic parameters
    v_stp = 1.0
    echo_msg(v_prg, v_stp, "Test Case 01: Basic Parameter", 1)
    rule_data = pd.DataFrame()
    r_json = get_check(rule_data)
    # print out the result
    print(json.dumps(r_json, indent=4))

    # Expected output:

   # 2. Test with parameters
    v_stp = 2.0
    echo_msg(v_prg, v_stp, "Test Case 02: With one rule id", 1)
    # rule_id = "CG0001"
    rule_id = "CG0180"
    rule_data = df_data[df_data["Rule ID"] == rule_id]
    r_json = get_check(rule_data)
    # print out the result
    print(json.dumps(r_json, indent=4))

    # Expected output:

# End of File
