# Purpose: Read a rule file from output folder
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   04/01/2023 (htu) - initial coding based on get_existing_rule
#

import os
import sys
from datetime import datetime, timezone
import uuid
import json
from ruamel.yaml import YAML, parser, scanner
# from io import StringIO
from transformer.transformer import Transformer
from yaml import safe_load
from dotenv import load_dotenv
from rulebuilder.echo_msg import echo_msg


def read_a_rule(rule_id:str=None, doc_id: str = None, rule_dir: str = None):
    v_prg = __name__

    # 1.0 check parameters
    v_stp = 1.0
    v_msg = "Read a rule file from rule_dir..."
    echo_msg(v_prg, v_stp, v_msg, 1)

    if rule_id is None and doc_id is None:
        v_stp = 1.1
        v_msg = "No rule id nor doc id is provided. "
        echo_msg(v_prg, v_stp, v_msg, 0)
        return {}
    
    if rule_dir is None:
        v_stp = 1.2
        v_msg = f"No rule json dir is provided."
        echo_msg(v_prg, v_stp, v_msg, 0)
        return {}
    v_msg = f" . rule_dir: {rule_dir}"
    echo_msg(v_prg, v_stp, v_msg, 1)
    
    # 2.0 search the rule in the folder
    v_stp = 2.0
    v_msg = f"Search"
    if rule_id is not None:
        v_msg += f" rule id ({rule_id}) "
    if doc_id is not None:
        v_msg += f" / doc_id ({doc_id}) "
    # v_msg += f"from {rule_dir}"
    echo_msg(v_prg, v_stp, v_msg, 1)

    cnt = {"All":0, "Searched": 0, "Matched": 0}
    r_json = {}
    rule_guid = None
    all_files = os.listdir(rule_dir)
    cnt["All"] = len(all_files)
    for filename in all_files:
        cnt["Searched"] += 1
        filepath = os.path.join(rule_dir, filename)
        # 2.1 search the file using rule_id 
        if rule_id is not None and rule_id in filename:
            cnt["Matched"] += 1
            v_stp = 2.1
            v_msg = f" . Reading rule_id ({rule_id}) file: {filename}"
            echo_msg(v_prg, v_stp, v_msg, 4)
            with open(filepath, 'r') as f:
                r_json = json.load(f)
                rule_guid = r_json.get('id')
            break
        if doc_id is not None and doc_id != rule_guid: 
            v_stp = 2.2
            with open(filepath, 'r') as f:
               r_json = json.load(f)
               rule_guid = r_json.get('id')
               core_id = r_json.get("json",{}).get("Core",{}).get("Id")
               v_msg = f" . Reading file {filename}: {core_id}"
               echo_msg(v_prg, v_stp, v_msg, 4)
            if doc_id == rule_guid: 
                v_msg = f" . Matched: ({doc_id})=({rule_guid})"
                echo_msg(v_prg, v_stp, v_msg, 4)
                cnt["Matched"] += 1
                break

    v_stp = 2.3
    v_msg = f" . Result(Matched/Searched/All): {cnt['Matched']}/{cnt['Searched']}/{cnt['All']}. "
    echo_msg(v_prg, v_stp, v_msg, 3)


    return r_json


if __name__ == "__main__":
    load_dotenv()
    out_dir = os.getenv("output_dir")
    rule_dir = out_dir + "/rules_json" 
    os.environ["g_lvl"] = "5"

    # Test case 1: Use Rule ID
    r_id = "CG0006"
    r_1 = read_a_rule(rule_id=r_id,rule_dir=rule_dir)
    doc_id = r_1.get("id")
    print(f"Doc ID: {doc_id}")
    # json.dump(r_1, sys.stdout, indent=4)

    # Test case 2: Use Doc ID 
    # d_id = "9959136a-523f-4546-9520-ffa22cda8867"     # CG0006
    d_id = "6161d4d5-6c96-47e1-baeb-4e70b1ffed46"       # CG0443
    r_2 = read_a_rule(doc_id=d_id,rule_dir=rule_dir)
    rule_id = r_2.get("json",{}).get("Core",{}).get("Id")
    print(f"Rule ID: {rule_id}")
    # json.dump(r_2, sys.stdout, indent=4)

    # Test case 3: Provide both rule and doc IDs 
    d_id = "8e4ce5f6-5d7d-420c-887d-26c09b89b793"   # CG0008 
    r_3 = read_a_rule(rule_id=r_id, doc_id=d_id, rule_dir=rule_dir)
    rule_id = r_3.get("json",{}).get("Core",{}).get("Id")
    print(f"r_id: {r_id}, doc_id: {d_id}, RuleID: {rule_id}")
    # json.dump(r_3, sys.stdout, indent=4)
