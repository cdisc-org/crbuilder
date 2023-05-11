# Purpose: Get json.Core for a rule 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/14/2023 (htu) - ported from proc_rules_sdtm as get_core module
#   03/17/2023 (htu) - added docstring and test cases
#   03/21/2023 (htu) - 
#     08. Don't add Core.Id
#   04/11/2023 (htu) - added r_std and r_constants and removed org and std
#   04/14/2023 (htu) - added code to skip if Core exists 
#    


import os
import json
from rulebuilder.echo_msg import echo_msg
from rulebuilder.get_existing_rule import get_existing_rule
from rulebuilder.get_rule_constants import get_rule_constants


def get_core(rule_id: str = None, r_std: str = None, r_constants: dict=None, exist_rule_data: dict = {}):
    """
    ===============
    get_core
    ===============
    This method builds the json.Core element in the core rule.

    Parameters:
    -----------
    rule_id: str
        Core rule id from rule definition
    org: str
        Organization name or abbreviation
    std: str
        Standard name or abbreviation
    existing_rule_data: dict 
        a data frame containing all the records for a rule that already developed. It 
        can be read from the existing rule folder using get_existing_rule. 

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
    echo_msg(v_prg, v_stp, v_msg, 2)

    r_std = os.getenv("r_standard") if r_std is None else r_std
    r_std = r_std.upper()
    if r_constants is None:
        r_cst = get_rule_constants(r_std=r_std)
    else:
        r_cst = r_constants

    v_stp = 1.1
    if rule_id is None:
        v_msg = "Input parameter rule_id is empty."
        echo_msg(v_prg, v_stp, v_msg, 0)
        return {}
    
    # if r_std == "SDTM_V2_0":
    
    d2_json =  exist_rule_data
    r_json = d2_json.get("json",{}).get("Core")
    if r_json is not None:
        return r_json 

    v_org = r_cst.get("Authorities").get("Organization")
    v_snm = r_cst.get("Authorities").get("Standards.Name")
    core_id = v_org + '.' + v_snm + '.' + rule_id 
    if r_json is None: 
        r_json= {
            "Id": core_id,
            "Version": r_cst.get("Core",{}).get("Version"),
            "Status": r_cst.get("Core").get("Status")
        }
    return r_json


# Test cases
if __name__ == "__main__":
    # set input parameters
    v_prg = __name__ + "::get_core"
    os.environ["g_lvl"] = "3"
    r_dir = "/Volumes/HiMacData/GitHub/data/core-rule-builder"
    existing_rule_dir = r_dir + "/data/output/json_rules1"

    # 1. Test with basic parameters
    v_stp = 1.0
    echo_msg(v_prg, v_stp, "Test Case 01: Basic Parameter", 1)
    r_json = get_core()
    # print out the result
    print(json.dumps(r_json, indent=4))

    # Expected output:

   # 2. Test with parameters
    v_stp = 2.0
    echo_msg(v_prg, v_stp, "Test Case 02: With one rule id", 1)
    rule_id = "CG0001"
    d2_data = get_existing_rule(rule_id, existing_rule_dir)
    r_json = get_core(rule_id, exist_rule_data=d2_data)
    # print out the result
    print(json.dumps(r_json, indent=4))

    # Expected output:

# End of File
