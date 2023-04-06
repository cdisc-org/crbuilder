# Purpose: Get json.Rule_Type for a rule 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/14/2023 (htu) - ported from proc_rules_sdtm as get_rtype module
#   03/15/2023 (htu) - added "import re"
#   03/17/2023 (htu) - added docstring and test case
#   03/21/2023 (htu) -
#     10. Rule Type and Sensitivity should be left null
#   03/22/2023 (htu) - added exist_rule_data
#    

import re 
import os
import pandas as pd 
from rulebuilder.echo_msg import echo_msg
from rulebuilder.read_rules import read_rules
# from rulebuilder.get_existing_rule import get_existing_rule


def get_rtype(rule_data, exist_rule_data: dict = {}):
    """
    ===============
    get_rtype
    ===============
    This method builds the json.Rule_Type element in the core rule.

    Parameters:
    -----------
    rule_data: dataframe
        a data frame containing all the records for a rule. It can be obtained from
        read_rules and select the records from the rule definition data frame.
    
    existing_rule_data: dict 
        a data frame containing all the records for a rule that already developed. It 
        can be read from the existing rule folder using get_existing_rule. 


    returns
    -------
        r_str: a string for json.Rule_Type
        
        possible rule type: 
        - Dataset Contents Check against Define XML and Library Metadata
        - Dataset Metadata Check
        - Dataset Metadata Check against Define XML
        - Define-XML
        - Domain Presence Check
        - Record Data
        - Value Level Metadata Check against Define XML
        - Variable Metadata Check
        - Variable Metadata Check against Define XML

    Raises
    ------
    ValueError
        None

    """
    v_prg = __name__
    v_stp = 1.0
    v_msg = "Setting parameters..."
    echo_msg(v_prg, v_stp, v_msg, 3)
    # if not rule_data.empty: 
    #    return None

    v_stp = 1.1
    v_msg = "Input parameter rule_data is empty."
    if rule_data.empty:
        echo_msg(v_prg, v_stp, v_msg, 0)
        return None
    r_str = exist_rule_data.get("json", {}).get("Rule_Type")
    if r_str is None: 
        r_condition = rule_data.iloc[0]["Condition"]
        # r_rule = rule_data.iloc[0]["Rule"]
        if r_condition is None:
            v_stp = 1.2
            v_msg = "No Condition is found in the rule definition."
            echo_msg(v_prg, v_stp, v_msg, 0)
            return None
        r_str = "Record Data"
        pattern = r"^(Dataset Metadata Check|Define-XML)"
        # Use the re.search() method to search for the pattern in the input string
        match = re.search(pattern, r_condition, re.IGNORECASE)
        # Check if a match was found
        if match:
            v_stp = 2.1
            # Convert the matched keyword to lowercase and capitalize the first letter
            r_str = match.group(1).lower().capitalize()
            v_msg = "Found a match."
            echo_msg(v_prg, v_stp, v_msg, 4)
        else:
            # No match found
            v_stp = 2.2
            v_msg = "No match found from '" + r_condition + "'"
            echo_msg(v_prg, v_stp, v_msg, 4)
    return r_str 


# Test cases
if __name__ == "__main__":
    # set input parameters
    v_prg = __name__ + "::get_rtype"
    os.environ["g_lvl"] = "3"
    r_dir = "/Volumes/HiMacData/GitHub/data/core-rule-builder"
    yaml_file = r_dir + "/data/target/SDTM_and_SDTMIG_Conformance_Rules_v2.0.yaml"
    df_data = read_rules(yaml_file)

    # 1. Test with basic parameters
    v_stp = 1.0
    echo_msg(v_prg, v_stp, "Test Case 01: Basic Parameter", 1)
    rule_data = pd.DataFrame()
    r_type = get_rtype(rule_data)
    # print out the result
    print(f"Rule Type: {r_type}")

   # 2. Test with parameters
    v_stp = 2.0
    echo_msg(v_prg, v_stp, "Test Case 02: With one rule id", 1)
    rule_id = "CG0001"
    rule_data = df_data[df_data["Rule ID"] == rule_id]
    r_type = get_rtype(rule_data)
    # print out the result
    print(f"Rule Type: {r_type}")

    # Expected output:

# End of File

    