# Purpose: Get json.Authorities for a rule 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/29/2023 (htu) - initial coding based on get_authorities module
#   03/30/2023 (htu) - added code to compare versions 
#   04/10/2023 (htu) - 
#     1. added r_std and rule_constants input parameters
#     2. called to get_rule_constants
#     3. renamed exist_rule_data to rule_obj
#   04/18/2023 (htu) - changed strategies to get Authorities at steps 1.2 and 2.5
#    

import os 
import json
import pandas as pd
from rulebuilder.echo_msg import echo_msg
from rulebuilder.read_rule_definitions import read_rule_definitions
from rulebuilder.get_existing_rule import get_existing_rule
from rulebuilder.get_rule_constants import get_rule_constants

def get_authority_sdtm(rule_data, rule_obj = None, r_std:str=None, rule_constants = None):
    """
    ===============
    get_authorities
    ===============
    This method builds the json.Authorities elements in the core rule.

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
    v_msg = "Input parameter rule_data is empty."
    if len(rule_data) == 0:
        echo_msg(v_prg, v_stp, v_msg,0)
        return {}
    r_std = os.getenv("r_standard") if r_std is None else r_std
    if rule_constants is None: 
        r_cst = get_rule_constants(r_std)
    else:
        r_cst = rule_constants 
    # v_msg = f"Rule Constants: {r_cst}"
    v_auth = r_cst.get("Authorities")
    v_orig = v_auth.get("Standards.References.Origin")
    v_vers = v_auth.get("Standards.References.Version")
    v_stdn = v_auth.get("Standards.Name")
    v_orga = v_auth.get("Organization")

    # print(f"get_authorities: Rule Data: {rule_data}")
    # create a sample dataframe
    df_rules = rule_data
    d2_rules = rule_obj      # data from content 
    d2_auth  = d2_rules.get("Authorities") 
    # print(f"Authorities: {d2_auth}")

    v_stp = 1.2
    if d2_auth is not None:
        r_json = d2_auth
        r_stds = d2_auth[0].get("Standards")
    else:
        r_json = []     # for Authorities 
        r_stds = []

    v_stp = 2.0
    v_msg = "Looping through each row of the rule records..."
    echo_msg(v_prg, v_stp, v_msg,2)
    r_refs = []         # for References
    r_cits = []         # for Citations 

    # r_json.append(CommentedMap({"Organization": "CDISC", "Standards": r_stds}))

    # i = -1
    # print(f"DF_RULES: {df_rules}")
    # for row in df_rules.itertuples(index=False):
    for i, row in df_rules.iterrows():
    # for row in df_rules:
        # i += 1 
        # print(f"{i} - {row}")

        # 2.1 build a citation 
        v_stp = 2.1
        v_item = row.get("Item")
        rule_id = row.get("Rule ID")
        v_sdtmig_version = row.get("SDTMIG Version")
        v_msg = f"  {i:02d} Rule ID - {rule_id}: {v_sdtmig_version}"
        echo_msg(v_prg, v_stp, v_msg,4)
        r_a_cit = {"Cited Guidance": row.get("Cited Guidance"),
                   "Document": row.get("Document"),
                   "Item": v_item,
                   "Section": row.get("Section")
                   }
        
        # 2.2 append citation 
        v_stp = 2.2
        if not v_item:  # Check if "Item" is empty
            del r_a_cit["Item"]  # Remove "Item" key from r_a_cit dictionary
        r_cits.append(r_a_cit) 

        # 2.3 build a reference and append it to r_refs 
        v_stp = 2.3
        v_msg = "Row (" + str(i) + "): " + str(r_a_cit)
        echo_msg(v_prg, v_stp, v_msg,5)
        # print(f"Row {i}: {r_a_cit}") 
        v_rule_version = row.get("Rule Version")
        r_a_ref = {"Origin": v_orig,
                   "Rule Identifier": {
                       "Id": rule_id,
                       "Version": v_rule_version
                   },
                   "Version": v_vers,
                   "Citations": [r_a_cit]
                   }
        r_refs.append(r_a_ref) 

        # 2.4 we build a standard from rule definition 
        v_stp = 2.4
        # v_sdtmig_version = df_rules.iloc[i]["SDTMIG Version"]
        v1_id = f"{v_stdn}{v_sdtmig_version}"

        
        r_a_std = {"Name": v_stdn,
                   "Version": v_sdtmig_version,
                   "References": [r_a_ref]
                   }
 
        # Since Authorities is defined in the existing rule (d2_auth), 
        # we need to check the standards and their citations
        # 2.5 we check standards in the existing rule 
        v_stp = 2.5 
        # Iterate over each authority and their standards
        # if we find a standard and it has the same version as rule definition,
        # we will use it 
        std_existing_cnt = 0 
        if d2_auth is not None: 
            for auth_std in d2_auth:
                for standard in auth_std["Standards"]:
                    # Check the version of the standard
                    d2_stdn = standard.get("Name")
                    # Check the version of the standard
                    d2_ig_version = standard.get('Version')
                    v2_id = f"{d2_stdn}{d2_ig_version}"
                    v_msg = f" . V1V2: {str(v1_id)}-->>{str(v2_id)}"
                    echo_msg(v_prg, v_stp, v_msg, 4)
                    if v1_id == v2_id:
                        std_existing_cnt += 1 
                        v_msg = f"   Standard Versions matched."
                        echo_msg(v_prg, v_stp, v_msg, 4)
                        v_stp = 2.53
                        # 04/12/2023: comment out so that we will force the change
                        # r_a_std = standard

                    # d2_sdtmig_version = standard.get('Version')
                    # if d2_sdtmig_version == v_sdtmig_version:
                    #     v_msg = " . SDTMIG Versions matched: " + \
                    #         str(v_sdtmig_version) + "->" + str(d2_sdtmig_version)
                    #     echo_msg(v_prg, v_stp, v_msg, 3)
                    #     r_a_std = standard
                    #     d2_sdtmig_version = None  
                    #     break 

                # End of loop through existing standards 
            # End of for auth_std in d2_auth
        # End of if d2_auth is not None
        if std_existing_cnt == 0: 
            r_stds.append(r_a_std)
        
    # End of loop through rule definition records 

    if d2_auth is None: 
        r_json.append( {"Organization": v_orga, "Standards": r_stds})
    else:
        r_json[0]["Organization"] = v_orga
        r_json[0]["Standards"] = r_stds
    # print(f"R_JSON: {r_json}")

    return r_json 


# Test cases
if __name__ == "__main__":
    # set input parameters
    v_prg = __name__ + "::get_authority_sdtm"
    os.environ["g_msg_lvl"] = "3"
    rule_dir = os.getenv("existing_rule_dir")
    df_data = read_rule_definitions("SDTM_V2_0")

    # 1. Test with basic parameters
    v_stp = 1.0
    echo_msg(v_prg, v_stp, "Test Case 01: Basic Parameter", 1)
    rule_data = pd.DataFrame()
    d2_data = {}
    r_json = get_authority_sdtm(rule_data, d2_data)
    # print out the result
    json.dumps(r_json, indent=4)

    # Expected output:

    # 2. Test with parameters
    v_stp = 2.0
    echo_msg(v_prg, v_stp, "Test Case 02: With one rule id", 1)
    v_std = "SDTM_V2_0"
    rule_id = "CG0001"

    print(f"RuleDef: {df_data}")
    rule_data = df_data[df_data["Rule ID"] == rule_id]
    print(f"RuleData: {rule_data}")
    d2_data = get_existing_rule(rule_id, in_rule_folder=rule_dir)
    r_json = get_authority_sdtm(rule_data, d2_data, r_std=v_std)
    # print out the result
    print(json.dumps(r_json, indent=4))

# End of File
