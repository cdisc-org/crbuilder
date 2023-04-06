# Purpose: Get json.Scope for a rule 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/14/2023 (htu) - ported from proc_rules_sdtm and modulized as get_scope
#   03/22/2023 (htu) - added exist_rule_data, docstring and test cases
#   03/24/2023 (htu) - added set_scope sub function
#    


import os
import json
from itertools import chain
from rulebuilder.echo_msg import echo_msg
from rulebuilder.read_rules import read_rules
from rulebuilder.decode_classes import decode_classes
from rulebuilder.get_existing_rule import get_existing_rule

def get_scope(rule_data, exist_rule_data: dict = {}):
    
    r_json = exist_rule_data.get("json", {}).get("Scope")
    if r_json is not None: 
        return r_json
    else:
        r_json = {
            "Classes": {},
            "Domains": {}
        }
    df_rules = rule_data
    df = decode_classes(df_rules) 
    # print(f"{__name__}:\n  Class: {df['Class']}\n  Domains: {df['Domain']}")
    # print(f"  C_Exc: {df['Classes_Exclude']}\n  D_Exc: {df['Domains_Exclude']}")

    def set_scope (k, c1="Class", c2="Classes"):
        if df[c1].iloc[0] is not None:
            v = list(set(chain.from_iterable(df[c1])))
            r_json[c2][k] = v
        else:
            if k in r_json[c2]:
                del r_json[c2][k]
    # End of subfunction set_scope 
    
    # v_ci = list(set(chain.from_iterable(df['Class'])))
    # v_ce = list(set(chain.from_iterable(df["Classes_Exclude"])))
    # v_di = list(set(chain.from_iterable(df["Domain"])))
    # v_de = list(set(chain.from_iterable(df["Domains_Exclude"])))
    set_scope("Include", c1="Class", c2="Classes") 
    set_scope("Exclude", c1="Classes_Exclude",c2="Classes")
    set_scope("Include", c1="Domain", c2="Domains")
    set_scope("Exclude", c1="Domains_Exclude", c2="Domains")

    # if df["Class"].iloc[0] is not None:
    #     r_json["Classes"]["Include"] = list(set(chain.from_iterable(df['Class'])))
    # else:
    #    if "Include" in r_json["Classes"]:
    #        del r_json["Classes"]["Include"]
    
    # if df["Classes_Exclude"].iloc[0] is not None: 
    #     r_json["Classes"]["Exclude"] = list(set(chain.from_iterable(df["Classes_Exclude"])))
    # else:
    #     if "Exclude" in r_json["Classes"]:
    #            del r_json["Classes"]["Exclude"]

    
    # if df["Domain"].iloc[0] is not None:
    #     r_json["Domains"]["Include"] = list(set(chain.from_iterable(df["Domain"])))
    # else:
    #     if "Include" in r_json["Domains"]:
    #         del r_json["Domains"]["Include"]


    # if df["Domains_Exclude"].iloc[0] is not None:  
    #     r_json["Domains"]["Exclude"] = list(set(chain.from_iterable(df["Domains_Exclude"])))
    # else:
    #     if "Include" in r_json["Domains"]:
    #         del r_json["Domains"]["Include"]

    return r_json


# Test cases
if __name__ == "__main__":
    # set input parameters
    v_prg = __name__ + "::get_scope"
    os.environ["g_lvl"] = "3"
    r_dir = "/Volumes/HiMacData/GitHub/data/core-rule-builder"
    existing_rule_dir = r_dir + "/data/output/json_rules1"
    yaml_file = r_dir + "/data/target/SDTM_and_SDTMIG_Conformance_Rules_v2.0.yaml"
    df_data = read_rules(yaml_file)

    # 1. Test with basic parameters
    v_stp = 1.0
    echo_msg(v_prg, v_stp, "Test Case 01: Basic Parameter", 1)
    rule_id = "CG0001"
    rule_data = df_data[df_data["Rule ID"] == rule_id]
    rule_data = rule_data.reset_index(drop=True)
    r_json = get_scope(rule_data)
    print(f"Result: {r_json}")
    # print out the result
    print(json.dumps(r_json, indent=4))

    # Expected output:

   # 2. Test with parameters
    v_stp = 2.0
    echo_msg(v_prg, v_stp, "Test Case 02: With one rule id", 1)
    # rule_id = "CG0053"
    rule_id = "CG0156"
    rule_data = df_data[df_data["Rule ID"] == rule_id]
    rule_data = rule_data.reset_index(drop=True)
    d2_data = get_existing_rule(rule_id, existing_rule_dir)
    r_json = get_scope(rule_data, exist_rule_data=d2_data)
    # print out the result
    print(json.dumps(r_json, indent=4))

    # Expected output:

# End of File
