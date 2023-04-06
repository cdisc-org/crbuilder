# Purpose: Get json.Authorities for a rule 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/14/2023 (htu) - ported from proc_rules_sdtm as get_authorities module
#   03/17/2023 (htu) - added docstring and test case 
#   03/21/2023 (htu) - added v_item to check and skip citation if it is None 
#     06. If Item is empty, don't include the Item property for the citation
#   03/22/2023 (htu) - added existing_rule_data to merge Standards 
#   03/29/2023 (htu) - used len
#    

import os 
import json
import pandas as pd
from rulebuilder.echo_msg import echo_msg
from rulebuilder.read_rules import read_rules
from rulebuilder.get_existing_rule import get_existing_rule


def get_authorities(rule_data, exist_rule_data: dict = {}):
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
    v_msg = "Input parameter rule_data is empty."
    if len(rule_data) == 0:
        echo_msg(v_prg, v_stp, v_msg,0)
        return {}

    # print(f"get_authorities: Rule Data: {rule_data}")
    # create a sample dataframe
    df_rules = rule_data
    d2_rules = exist_rule_data
    d2_auth  = d2_rules.get("json",{}).get("Authorities") 
    # print(f"Authorities: {d2_auth}")

    # define authorities variables 
    r_a_cit = {"Cited_Guidance": None,
               "Document": None,
               "Item": None,
               "Section": None
               }
    r_cits = []
    r_a_ref = {"Origin": "SDTM and SDTMIG Conformance Rules",
               "Rule_Identifier": {
                   "Id": "CG0378",
                   "Version": "1"
               },
               "Version": "2.0",
               "Citations": r_cits
               }
    r_refs = []
    r_a_std = {"Name": "SDTMIG",
               "Version": "3.4",
               "References": r_refs
               }
    r_stds = []
    r_json = [{"Organization": "CDISC",
               "Standards": r_stds 
               }
              ]
    # loop through each row of the dataframe
    v_stp = 2.0
    v_msg = "Looping through each row of the rule records..."
    echo_msg(v_prg, v_stp, v_msg,2)
    # i = -1
    # for row in df_rules.itertuples(index=False):
    for i, row in df_rules.iterrows():
    # for row in df_rules:
        # i += 1
        v_stp = 2.1
        # v_item          = df_rules.iloc[i]["Item"]
        # rule_id         = df_rules.iloc[i]["Rule ID"]
        v_item = row.get("Item")
        rule_id = row.get("Rule ID")
        v_msg = print(f"  {i:02d} Rule ID - {rule_id}")
        echo_msg(v_prg, v_stp, v_msg,3)

        # r_a_cit = {"Cited_Guidance": df_rules.iloc[i]["Cited Guidance"],
        #            "Document": df_rules.iloc[i]["Document"],
        #            "Item": df_rules.iloc[i]["Item"],
        #            "Section": df_rules.iloc[i]["Section"]
        #            }
        r_a_cit = {"Cited_Guidance": row.get("Cited Guidance"),
                   "Document": row.get("Document"),
                   "Item": v_item,
                   "Section": row.get("Section")
                   }

        v_stp = 2.2
        if not v_item:  # Check if "Item" is empty
            del r_a_cit["Item"]  # Remove "Item" key from r_a_cit dictionary
        r_cits.append(r_a_cit) 

        v_stp = 2.3
        v_msg = "Row {" + str(i) + "}: " + str(r_a_cit)
        echo_msg(v_prg, v_stp, v_msg,5)
        # print(f"Row {i}: {r_a_cit}") 
        v_rule_version = row.get("Rule Version")
        r_a_ref = {"Origin": "SDTM and SDTMIG Conformance Rules",
                   "Rule_Identifier": {
                       "Id": rule_id,
                       "Version": v_rule_version
                   },
                   "Version": "2.0",
                   "Citations": [r_a_cit]
                   }
        r_refs.append(r_a_ref) 

        v_stp = 2.4
        # v_sdtmig_version = df_rules.iloc[i]["SDTMIG Version"]
        v_sdtmig_version = row.get("SDTMIG Version")
        r_a_std = {"Name": "SDTMIG",
                   "Version": v_sdtmig_version,
                   "References": [r_a_ref]
                   }
        
        if d2_auth is not None:
            v_stp = 2.5

            # Iterate over each authority and their standards
            for auth_std in d2_auth:
                for standard in auth_std["Standards"]:
                    # Print the version of the standard
                    d2_sdtmig_version = standard.get('Version')
                    v_msg = " . SDTMIG Versions: " + str(v_sdtmig_version) + "->" + str(d2_sdtmig_version)
                    echo_msg(v_prg, v_stp, v_msg,2)
                    if d2_sdtmig_version == v_sdtmig_version:
                        r_a_std = standard
                        break 
        r_stds.append(r_a_std) 

        # print(f"Row {i}: {df_rules.iloc[i]}")
        #print("Record:")
        # for col, value in zip(df_rules.columns, row):
        #  print(f"  {col}: {value}")
    return r_json 


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
    r_json = get_authorities(rule_data, d2_data) 
    # print out the result
    print(json.dumps(r_json, indent=4))

    # Expected output:

   # 2. Test with parameters
    v_stp = 2.0
    echo_msg(v_prg, v_stp, "Test Case 02: With one rule id", 1)
    rule_id = "CG0001"
    rule_data = df_data[df_data["Rule ID"] == rule_id]
    d2_data = get_existing_rule(rule_id, existing_rule_dir)
    r_json = get_authorities(rule_data,d2_data)
    # print out the result
    print(json.dumps(r_json, indent=4))

    # Expected output:

# End of File
