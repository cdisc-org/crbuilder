# Purpose: Get json.Authorities for a rule 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/29/2023 (htu) - initial coding based on get_authorities module
#   03/30/2023 (htu) - added code to compare versions 
#   04/10/2023 (htu) - 
#     1. added r_std input parameter and called to get_rule_constants 
#     2. renamed exist_rule_data to rule_obj 
#     3. moved content to get_authority_sdtm and called to it 
#     4. called to get_authority_fda, get_authority_adam, get_authority_send
#    

import os 
import json
import pandas as pd
from rulebuilder.echo_msg import echo_msg
from rulebuilder.read_rules import read_rules
from rulebuilder.get_rule_constants import get_rule_constants
from rulebuilder.get_authority_fda import get_authority_fda 
from rulebuilder.get_authority_adam import get_authority_adam
from rulebuilder.get_authority_send import get_authority_send 
from rulebuilder.get_authority_sdtm import get_authority_sdtm
from rulebuilder.get_rule_constants import get_rule_constants

def get_yaml_authorities(rule_data, rule_obj, r_std:str=None):
    """
    ===============
    get_authorities
    ===============
    This method builds the json.Authorities elements in the core rule.

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
        r_json: json content for Authorities 

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
    if len(rule_data) == 0:
        v_msg = "Input parameter rule_data is empty."
        echo_msg(v_prg, v_stp, v_msg,0)
        return {}
    r_std = os.getenv("r_standard") if r_std is None else r_std
    r_std = r_std.upper()
    r_cst = get_rule_constants(r_std)

    # 2.0 process authority based on standard 
    v_msg = f"Get {r_std} Authorities..."
    if r_std in ("SDTM_V2_0"):
        v_stp = 2.1 
        echo_msg(v_prg, v_stp, v_msg,3)
        df = get_authority_sdtm(r_std=r_std, rule_data=rule_data,
                                    rule_obj=rule_obj,
                                    rule_constants=r_cst)
    elif r_std in ("FDA_VR1_6"):
        v_stp = 2.2 
        echo_msg(v_prg, v_stp, v_msg,3)
        df = get_authority_fda(r_std=r_std, rule_data=rule_data,
                                    rule_obj=rule_obj,
                                    rule_constants=r_cst)
    elif r_std in ("SEND_V4_0"):
        v_stp = 2.3 
        echo_msg(v_prg, v_stp, v_msg,3)
        df = get_authority_send(r_std=r_std, rule_data=rule_data,
                                    rule_obj=rule_obj,
                                    rule_constants=r_cst)
    else:
        v_stp = 2.4
        echo_msg(v_prg, v_stp, v_msg, 3)
        df = get_authority_adam(r_std=r_std, rule_data=rule_data,
                                   rule_obj=rule_obj,
                                   rule_constants=r_cst)
    return df 

# Test cases
if __name__ == "__main__":
    # set input parameters
    v_prg = __name__ + "::get_authorities"
    os.environ["g_lvl"] = "3"
    r_dir = "/Volumes/HiMacData/GitHub/data/core-rule-builder"
    yaml_file = r_dir + "/data/target/SDTM_and_SDTMIG_Conformance_Rules_v2.0.yaml"
    existing_rule_dir = r_dir + "/data/output/json_rules1"
    df_data = read_rules(yaml_file)

    # 1. Test with basic parameters
    v_stp = 1.0
    echo_msg(v_prg, v_stp, "Test Case 01: Basic Parameter", 1)
    rule_data = pd.DataFrame()
    d2_data = {}
    r_json = get_yaml_authorities(rule_data, d2_data)
    # print out the result
    print(json.dumps(r_json, indent=4))


# End of File
