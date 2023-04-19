# Purpose: Get json.Authorities for a rule 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   04/10/2023 (htu) - initial coding based on get_authority_sdtm module
#   04/12/2023 (htu) - we force the change for existing standards at step 2.53

import os
import re
import json
import pandas as pd
from rulebuilder.echo_msg import echo_msg
from rulebuilder.read_rule_definitions import read_rule_definitions
from rulebuilder.get_existing_rule import get_existing_rule
from rulebuilder.get_rule_constants import get_rule_constants


def get_authority_fda(rule_data, rule_obj=None, r_std: str = None, rule_constants=None):
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
    echo_msg(v_prg, v_stp, v_msg, 2)
    v_stp = 1.1
    v_msg = "Input parameter rule_data is empty."
    if len(rule_data) == 0:
        echo_msg(v_prg, v_stp, v_msg, 0)
        return {}
    r_std = os.getenv("r_standard") if r_std is None else r_std
    if rule_constants is None:
        r_cst = get_rule_constants(r_std)
    else:
        r_cst = rule_constants
    # print(f"Rule Constants: {r_cst}")
    v_auth = r_cst.get("Authorities")
    v_orig = v_auth.get("Standards.References.Origin")
    v_vers = v_auth.get("Standards.References.Version")
    v_stdn = v_auth.get("Standards.Name")
    v_orga = v_auth.get("Organization")

    # print(f"get_authorities: Rule Data: {rule_data}")
    # create a sample dataframe
    df_rules = rule_data
    d2_rules = rule_obj      # data from content
    d2_auth = d2_rules.get("Authorities")
    # print(f"Authorities: {d2_auth}")
    v_msg = f"D2_Auth: ----------"
    echo_msg(v_prg, v_stp, v_msg, 8)
    echo_msg(v_prg, v_stp, d2_auth, 8)

    v_stp = 2.0
    v_msg = "Looping through each row of the rule records..."
    echo_msg(v_prg, v_stp, v_msg, 2)
    r_json = []         # for Authorities
    r_stds = []         # for Standards
    r_refs = []         # for References
    r_cits = []         # for Citations

    # r_json.append(CommentedMap({"Organization": "CDISC", "Standards": r_stds}))

    # i = -1
    # print(f"DF_RULES: {df_rules}")
    # for row in df_rules.itertuples(index=False):
    v_vs = r_cst.get("VS")
    # ["SDTMIG3.1.2","SDTMIG3.1.3","SDTMIG3.2", "SDTMIG3.3", "SENDIG3.0", "SENDIG3.1","SENDIG3.1.1","SENDIG-AR1.0", "SENDIG-DART1.1"]
    v_re = r'([^\d]+)(\d.*)'
    for i, row in df_rules.iterrows():
        # v_item = row.get("Item")
        v_msg = f"Row: {row}"
        echo_msg(v_prg, v_stp, v_msg, 9)
        rule_id = row.get("Rule ID")
        v_docum = row.get("Publisher")
        v_cited = row.get("Publisher ID")
        r_a_cit = {"Cited Guidance": v_cited, "Document": v_docum}

        for j in v_vs: 
            v_ig_version = re.sub(r'([G|R|T])(\d)', r'\1 \2', j)
            v_jv = row.get(j).upper()
            match = re.match(v_re, j)
            if match:
                v_stdn = match.group(1)
                v_ig_version = match.group(2)

            v_msg = f"  {i:02d} Rule ID - {rule_id}: {v_ig_version} ({v_jv})"
            echo_msg(v_prg, v_stp, v_msg, 4)

            if v_jv == "X":
                # 2.1 build a citation
                v_stp = 2.1

                # 2.2 append citation
                v_stp = 2.2
                r_a_cit = {"Cited Guidance": v_cited, "Document": v_docum}
                # if not v_item:  # Check if "Item" is empty
                #    del r_a_cit["Item"]  # Remove "Item" key from r_a_cit dictionary
                r_cits.append(r_a_cit)

                # 2.3 build a reference and append it to r_refs
                v_stp = 2.3
                v_msg = "Row (" + str(i) + "): " + str(r_a_cit)
                echo_msg(v_prg, v_stp, v_msg, 5)
                # print(f"Row {i}: {r_a_cit}")
                v_rule_version = row.get("Rule Version")
                if v_rule_version is None: 
                    v_rule_version = r_cst.get("Authorities").get(
                        "Standard.References.Rule Identifier.Version")
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
                r_a_std = {"Name": v_stdn,
                   "Version": v_ig_version,
                   "References": [r_a_ref]
                   }
                v1_id = f"{v_stdn}{v_ig_version}"
                # 2.5 we check standards in the existing rule
                if d2_auth is not None:
                    v_stp = 2.5
                    # Iterate over each authority and their standards
                    # if we find a standard and it has the same version as rule definition,
                    # we will use it
                    for auth_std in d2_auth:
                        for standard in auth_std["Standards"]:
                            d2_stdn = standard.get("Name")
                            # Check the version of the standard
                            d2_ig_version = standard.get('Version')
                            v2_id = f"{d2_stdn}{d2_ig_version}"
                            v_msg = f" . V1V2: {str(v1_id)}-->>{str(v2_id)}"
                            echo_msg(v_prg, v_stp, v_msg, 4)
                            if v1_id == v2_id:
                                v_msg = f"   Standard Versions matched." 
                                echo_msg(v_prg, v_stp, v_msg, 4)
                                v_stp = 2.53
                                # 04/12/2023: comment out so that we will force the change
                                # r_a_std = standard
                                d2_ig_version = None
                                break

                # End of loop through existing standards
                r_stds.append(r_a_std)

    # End of loop through rule definition records

    r_json.append({"Organization": v_orga, "Standards": r_stds})
    # print(f"R_JSON: {r_json}")

    return r_json

# Test cases
if __name__ == "__main__":
    # set input parameters
    v_prg = __name__ + "::get_authority_fda"
    os.environ["g_msg_lvl"] = "5"
    v_std = "FDA_VR1_6"
    rule_dir = os.getenv("existing_rule_dir")
    df_data = read_rule_definitions(v_std)

    # 1. Test with basic parameters
    v_stp = 1.0
    echo_msg(v_prg, v_stp, "Test Case 01: Basic Parameter", 1)
    rule_data = pd.DataFrame()
    d2_data = {}
    r_json = get_authority_fda(rule_data, d2_data)
    # print out the result
    json.dumps(r_json, indent=4)

    # Expected output:

   # 2. Test with parameters
    v_stp = 2.0
    echo_msg(v_prg, v_stp, "Test Case 02: With one rule id", 1)
    rule_id = "CT2001"

    print(f"RuleDef: {df_data}")
    rule_data = df_data[df_data["Rule ID"] == rule_id]
    print(f"RuleData: {rule_data}")
    d2_data = get_existing_rule(rule_id, in_rule_folder=rule_dir)
    r_json = get_authority_fda(rule_data, d2_data, r_std=v_std)
    # print out the result
    print(json.dumps(r_json, indent=4))

# End of File
