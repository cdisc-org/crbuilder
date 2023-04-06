# Purpose: Get json.Sensitivity for a rule 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/14/2023 (htu) - ported from proc_rules_sdtm as get_sensitivity module
#   03/15/2023 (htu) - added "import re"
#   03/21/2023 (htu) - added docstring and test cases
#     10. Rule Type and Sensitivity should be left null
#   03/22/2023 (htu) - added exist_rule_data
#    

import os
import re 
import json 
import pandas as pd 
from rulebuilder.echo_msg import echo_msg
from rulebuilder.read_rules import read_rules


def get_sensitivity(rule_data, exist_rule_data: dict = {}):
    """
    ===============
    get_sensitivity
    ===============
    This method builds the json.Sensitivity element in the core rule.

    Parameters:
    -----------
    rule_data: dataframe
        a data frame containng all the records for a rule. It can be obtained from
        read_rules and select the records from the rule definition data frame.
    
    existing_rule_data: dict 
        a data frame containng all the records for a rule that already developed. It 
        can be read from the existing rule folder using get_existing_rule. 


    returns
    -------
        r_str: a string for json.Sensitivity

        possible sensitivities: 
          - Domain
          - Dataset
          - Study
          - Record
          - Variable
          - Term 


    Raises
    ------
    ValueError
        None

    """
 
    if rule_data.empty:
        return None

    r_str = exist_rule_data.get("json", {}).get("Sensitivity")
    if r_str is None: 
        r_condition = rule_data.iloc[0]["Condition"]
        # r_rule = rule_data.iloc[0]["Rule"]
        r_str = "Record"
        if r_condition is not None:
            # pattern = r"^(study|dataset|domain|variable|term)"
            pattern = r"^(dataset|record)"
            # Use the re.search() method to search for the pattern in the input string
            match = re.search(pattern, r_condition, re.IGNORECASE)
            # Check if a match was found
            if match:
                # Convert the matched keyword to lowercase and capitalize the first letter
                r_str = match.group(1).lower().capitalize()
            else:
                # No match found
                print(f"No match found from {r_condition}")
    return r_str


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
    r_json = get_sensitivity(rule_data)
    # print out the result
    print(json.dumps(r_json, indent=4))

    # Expected output:

   # 2. Test with parameters
    v_stp = 2.0
    echo_msg(v_prg, v_stp, "Test Case 02: With one rule id", 1)
    rule_id = "CG0001"
    rule_data = df_data[df_data["Rule ID"] == rule_id]
    r_json = get_sensitivity(rule_data)
    # print out the result
    print(json.dumps(r_json, indent=4))

    # Expected output:

# End of File

    