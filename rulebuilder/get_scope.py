# Purpose: Get json.Scope for a rule 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/14/2023 (htu) - ported from proc_rules_sdtm and modularized as get_scope
#   03/22/2023 (htu) - added exist_rule_data, docstring and test cases
#   03/24/2023 (htu) - added set_scope sub function
#   04/10/2023 (htu) - added r_std input parameter 
#   04/12/2023 (htu) - added r_cst to get rule classes from get_rule_constants 
#    


import os
import json
from itertools import chain
from rulebuilder.echo_msg import echo_msg
from rulebuilder.read_rule_definitions import read_rule_definitions
from rulebuilder.decode_classes import decode_classes
from rulebuilder.get_rule_constants import get_rule_constants

def get_scope(rule_data, exist_rule_data: dict = {}, r_std:str=None, r_cst = None):
    v_prg = __name__
    v_stp = 1.0
    v_msg = "Getting Scope..."
    echo_msg(v_prg, v_stp, v_msg, 2) 
    
    r_json = exist_rule_data.get("json", {}).get("Scope")
    v_msg = f"Scope: {r_json}"
    echo_msg(v_prg, v_stp, v_msg, 8) 

    if r_json is not None: 
        return r_json

    r_std = os.getenv("r_standard") if r_std is None else r_std
    r_std = r_std.upper()

    if r_cst is None:
        r_cst = get_rule_constants(r_std=r_std)
    v_cs = r_cst.get("Classes")

    r_json = {
        "Classes": {},
        "Domains": {}
    }
    df_rules = rule_data
    v_msg = f" . for {r_std} "
    if r_std in ("SDTM_V2_0"):
        v_stp = 2.1 
        echo_msg(v_prg, v_stp, v_msg, 3) 
        
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
    
        set_scope("Include", c1="Class", c2="Classes") 
        set_scope("Exclude", c1="Classes_Exclude",c2="Classes")
        set_scope("Include", c1="Domain", c2="Domains")
        set_scope("Exclude", c1="Domains_Exclude", c2="Domains")
    elif r_std in ("FDA_VR1_6"):
        v_stp = 2.2
        echo_msg(v_prg, v_stp, v_msg, 3) 

        # v_cs = ["INTERVENTIONS", "EVENTS", "FINDINGS", "FINDINGS ABOUT"]
        # v_str = "INTERVENTIONS, EVENTS, FINDINGS, FINDINGS ABOUT, SE, SM, SV, INTERVENTIONS, EVENTS, FINDINGS, FINDINGS ABOUT,CV, EG, FT, LB, MK, NV, OE, PC, PP, RE, UR, AE, CE, MH"
        v_str = df_rules.iloc[0]["Domains"]
        # Split v_str into a list of words
        words = [word.strip() for word in v_str.split(',')]

        # Initialize two sets to store unique words
        in_cs = set()
        not_in_cs = set()

        # Loop through each word
        for word in words:
            if word in v_cs:
                in_cs.add(word)
            else:
                not_in_cs.add(word)

        # Convert sets to lists and sort them
        in_cs = sorted(list(in_cs))
        not_in_cs = sorted(list(not_in_cs))

        # Print the two lists
        v_msg = f"Words in v_cs: {in_cs}"
        v_msg = f"Words not in v_cs: {not_in_cs}"

        if len(in_cs) > 0: 
            r_json["Classes"]["Include"] = in_cs
        if len(not_in_cs) > 0: 
            r_json["Domains"]["Include"] = not_in_cs


    return r_json


# Test cases
if __name__ == "__main__":
    # set input parameters
    v_prg = __name__ + "::get_scope"
    os.environ["g_msg_lvl"] = "5"
    v_std = "FDA_VR1_6"
    rule_dir = os.getenv("existing_rule_dir")
    df_data = read_rule_definitions(v_std)

    # 1. Test with basic parameters
    v_stp = 1.0
    echo_msg(v_prg, v_stp, "Test Case 01: Basic Parameter", 1)
    d2_data = {}
    rule_id = "SD1086"
    rule_data = df_data[df_data["Rule ID"] == rule_id]
    r_json = get_scope(rule_data, d2_data)
    # print out the result
    json.dumps(r_json, indent=4)

# End of File
