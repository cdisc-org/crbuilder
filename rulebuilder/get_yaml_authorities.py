# Purpose: Get json.Authorities for a rule 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/29/2023 (htu) - initial coding based on get_authorities module
#   03/30/2023 (htu) - added code to compare versions 
#    

import os 
import json
import pandas as pd
from rulebuilder.echo_msg import echo_msg
from rulebuilder.read_rules import read_rules
from rulebuilder.get_existing_rule import get_existing_rule
from ruamel.yaml.comments import CommentedMap


def get_yaml_authorities(rule_data, exist_rule_data):
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
    d2_rules = exist_rule_data      # data from content 
    d2_auth  = d2_rules.get("Authorities") 
    # print(f"Authorities: {d2_auth}")

    v_stp = 2.0
    v_msg = "Looping through each row of the rule records..."
    echo_msg(v_prg, v_stp, v_msg,2)
    r_json = []         # for Authorities
    r_stds = []         # for Standards 
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
        v_msg = print(f"  {i:02d} Rule ID - {rule_id}: {v_sdtmig_version}")
        echo_msg(v_prg, v_stp, v_msg,4)
        r_a_cit = {"Cited_Guidance": row.get("Cited Guidance"),
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
        r_a_ref = {"Origin": "SDTM and SDTMIG Conformance Rules",
                   "Rule_Identifier": {
                       "Id": rule_id,
                       "Version": v_rule_version
                   },
                   "Version": "2.0",
                   "Citations": [r_a_cit]
                   }
        r_refs.append(r_a_ref) 

        # 2.4 we build a standard from rule definition 
        v_stp = 2.4
        # v_sdtmig_version = df_rules.iloc[i]["SDTMIG Version"]
        
        r_a_std = {"Name": "SDTMIG",
                   "Version": v_sdtmig_version,
                   "References": [r_a_ref]
                   }
        
        # 2.5 we check standards in the existing rule 
        if d2_auth is not None:
            v_stp = 2.5
            # Iterate over each authority and their standards
            # if we find a standard and it has the same version as rule definition,
            # we will use it 
            for auth_std in d2_auth:
                for standard in auth_std["Standards"]:
                    # Check the version of the standard
                    d2_sdtmig_version = standard.get('Version')
                    if d2_sdtmig_version == v_sdtmig_version:
                        v_msg = " . SDTMIG Versions matched: " + \
                            str(v_sdtmig_version) + "->" + str(d2_sdtmig_version)
                        echo_msg(v_prg, v_stp, v_msg, 3)
                        r_a_std = standard
                        d2_sdtmig_version = None  
                        break 

                # End of loop through existing standards 
        # r_stds.append(CommentedMap(r_a_std))
        # print(f"R_STDS: {r_a_std}")
        r_stds.append(r_a_std)
        
    # End of loop through rule definition records 

    # r_json.append(CommentedMap(
    #        {"Organization": "CDISC", "Standards": r_stds}))
    r_json.append( {"Organization": "CDISC", "Standards": r_stds})
    # print(f"R_JSON: {r_json}")

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
    r_json = get_yaml_authorities(rule_data, d2_data)
    # print out the result
    print(json.dumps(r_json, indent=4))

    # Expected output:

   # 2. Test with parameters
    v_stp = 2.0
    echo_msg(v_prg, v_stp, "Test Case 02: With one rule id", 1)
    rule_id = "CG0001"
    rule_data = df_data[df_data["Rule ID"] == rule_id]
    d2_data = get_existing_rule(rule_id, existing_rule_dir)
    r_json = get_yaml_authorities(rule_data, d2_data)
    # print out the result
    print(json.dumps(r_json, indent=4))

    # Expected output:
    jd = {
        "id": "9959136a-523f-4546-9520-ffa22cda8867",
        "changed": "2023-03-19T20:59:01.002Z",
        "content": "Check:\n  all:\n    - name: --DY\n      operator: non_empty\n    - name: --DTC\n      operator: is_complete_date\n    - name: RFSTDTC\n      operator: is_complete_date\n    - name: --DY # checking failure criteria\n      operator: not_equal_to\n      value: $val_dy # this is created in the operations statement that follows\nOperations:\n  - name: RFSTDTC\n    domain: DM\n    operator: dy # does dy op take into acct +1?\n    id: $val_dy # ex in devops 2538 used -- but that threw an error\nCore:\n  Id: CDISC.SDTMIG.CG0006\n  Version: '1'\n  Status: Draft\nDescription: Raise an error when --DY is not calculated as per the study day algorithm\n  as a non-zero integer value when the date portion of --DTC is complete and the date\n  portion of DM.RFSTDTC is a complete date AND --DY is not empty\nMatch Datasets:\n  - Name: DM\n    Keys:\n      - USUBJID\nOutcome:\n  Message: --DY is not calculated correctly even though the date portion of --DTC\n    is complete, the date portion of DM.RFSTDTC is a complete date, and --DY is not\n    empty.\n  Output Variables:\n    - --DY\n    - --DTC\n    - RFSTDTC\nRule Type: Record Data\nSensitivity: Record\nExecutability: Fully Executable\nAuthorities:\n  - Organization: CDISC\n    Standards:\n      - Name: SDTMIG\n        Version: '3.4'\n        References:\n          - Origin: SDTM and SDTMIG Conformance Rules\n            Rule Identifier:\n              Id: CG0006\n              Version: '2'\n            Version: '2.0'\n            Citations:\n              - Cited Guidance: The Study Day value is incremented by 1 for each date\n                  following RFSTDTC. Dates prior to RFSTDTC are decreased by 1, with\n                  the date preceding RFSTDTC designated as Study Day -1 (there is\n                  no Study Day 0)....All Study Day values are integers. Thus to calculate\n                  Study Day, --DY = (date portion of --DTC) - (date portion of RFSTDTC)\n                  + 1 if --DTC is on or after RFSTDTC, --DY = (date portion of --DTC)\n                  - (date portion of RFSTDTC) if --DTC precedes RFSTDTC. This algorithm\n                  should be used across all domains.\n                Document: SDTMIG v3.4\n                Section: 4.4.4\nScope:\n  Classes:\n    Include:\n      - ALL\n  Domains:\n    Include:\n      - ALL\n",
        "created": "2022-04-11T04:35:12Z",
        "creator": {
            "id": "6ae4d700-ba2d-4fad-9812-f33baa02bc82"
        },
    }

# End of File
