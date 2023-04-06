# Purpose: Create a Container in a Cosmos DB
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   04/05/2023 (htu) - initial coding 
#
import os
import sys
import json 
from rulebuilder.get_db_cfg import get_db_cfg
from rulebuilder.echo_msg import echo_msg
from rulebuilder.get_doc_stats import get_doc_stats
from azure.cosmos.exceptions import CosmosResourceNotFoundError


def read_db_rule(rule_id: str, db_cfg = None, r_ids = None, 
                 db_name:str=None, ct_name:str=None):
    v_prg = __name__

    # 1.0 check parameters

    # 1.1 check rule_id 
    if rule_id is None:
        v_stp = 1.1
        v_msg = "No rule_id is provided."
        echo_msg(v_prg, v_stp, v_msg, 0)
        return {}

    # 1.2 get db connection 
    v_stp = 1.2
    if db_cfg is None:
        if db_name is not None and ct_name is not None:
            db_cfg = get_db_cfg(db_name=db_name,ct_name=ct_name)
        else: 
            v_msg = f"No DB configuration is provided."
            echo_msg(v_prg, v_stp, v_msg, 0)
            return {}
    ctc = db_cfg.get("ct_conn")
    db  = db_cfg.get("db_name")
    ct  = db_cfg.get("ct_name")

    # 1.3 get rule stats
    if r_ids is None:
        r_ids = get_doc_stats(db=db,ct=ct)

    # 2.0 read the doc associated with the rule_id
    r_json = {}
    if rule_id in r_ids.keys():
        v_stp = 2.1
        r_docs = r_ids.get(rule_id, {}).get("ids")
        d_id = r_docs[0]
        try:
            v_stp = 2.11
            v_msg = f"Reading {rule_id} ({d_id}) in {db}.{ct}"
            r_json = ctc.read_item(item=d_id, partition_key=d_id)
            echo_msg(v_prg, v_stp, v_msg, 2)
        except CosmosResourceNotFoundError:
            v_stp = 2.12
            v_msg = f"Could not read {rule_id} ({d_id}) from {db}.{ct}"
            echo_msg(v_prg, v_stp, v_msg, 0)
    else:
        v_stp = 2.2
        
        v_msg = f"Could not find {rule_id} in {db}.{ct}"
        echo_msg(v_prg, v_stp, v_msg, 0)
    
    return r_json


# Test cases
if __name__ == "__main__":
    # set input parameters
    os.environ["g_lvl"] = "3"
    v_prg = __name__ + "::read_db_rule"
    # rule_list = ["CG0373", "CG0378", "CG0379"]
    rule_id = "CG0015"
    db = 'library'
    ct = 'editor_rules_dev'
    r = read_db_rule(rule_id=rule_id,db_name=db,ct_name=ct)
    json.dump(r, sys.stdout, indent=4)
