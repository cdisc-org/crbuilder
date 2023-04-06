# Purpose: Publish a rule to a Cosmos DB
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   04/01/2023 (htu) - initial coding based on get_existing_rule 
#   04/03/2023 (htu) - added code to get status
#   04/04/2023 (htu) -
#     1. changed "Rule Identifier" to Rule_Identifier
#     2. added step 4.1 to backup docs before replacing it 
#

import os
# import sys
import json 
from dotenv import load_dotenv
from rulebuilder.echo_msg import echo_msg
from rulebuilder.read_a_rule import read_a_rule
from rulebuilder.get_db_cfg import get_db_cfg
from azure.cosmos.exceptions import CosmosResourceNotFoundError


def publish_a_rule(rule_id = None, doc_id:str=None, rule_dir:str=None, 
                   db_cfg = None, r_ids = None):
    """
    Publishes a rule to a Cosmos DB container.

    Args:
        rule_id (str): The ID of the rule to publish.
        doc_id (str): The ID of the rule document to publish.
        rule_dir (str): The path to the directory where rule JSON files are stored.
        db_cfg (dict): The configuration for the Cosmos DB container.
        r_ids (dict): The stats for existing documents

    Returns:
        str: A message indicating whether the rule was added or replaced.

    Raises:
        Exception: If there was an error publishing the rule.

    """
    v_prg = __name__
    v_stp = 1.0
    g_lvl = int(os.getenv("g_lvl"))
    log_fn = os.getenv("log_fn") 
    v_msg = "Getting existing rule..."
    echo_msg(v_prg, v_stp, v_msg, 1)

    # 1.1 check rul_json_dir 
    v_stp = 1.1
    if rule_dir is None:
        load_dotenv()
        rule_dir = os.getenv("rule_json_dir")
    if rule_dir is None:
        output_dir = os.getenv("output_dir")
        rule_dir = output_dir + "/rules_json"
    if not os.path.exists(rule_dir):
        v_msg = f"Could not find rule_dir: {rule_dir}"
        echo_msg(v_prg, v_stp, v_msg, 0)
        return None
    v_msg = f"Rule JSON dir: {rule_dir}"
    echo_msg(v_prg, v_stp, v_msg, 2)

    # 1.2 check container connection
    v_stp = 1.3 
    if db_cfg is None:
        v_msg = f"No DB configuration is provided."
        echo_msg(v_prg, v_stp, v_msg, 0)
        return None
    ct_conn =db_cfg.get("ct_conn")
    db = db_cfg.get("db_name")
    ct = db_cfg.get("ct_name") 

    if ct_conn is None:
        v_msg = f"No container connection is provided."
        echo_msg(v_prg, v_stp, v_msg, 0)
        return None 
    
    
    # 2.0 get the rule document 
    v_stp = 2.0 
    v_msg = "Get json document based on rule_id or doc_id..."
    echo_msg(v_prg, v_stp, v_msg, 1)
    r_json = read_a_rule(rule_id=rule_id, doc_id=doc_id, 
                         rule_dir=rule_dir)

    # 3.0 publish the documnet 
    v_stp = 3.0
    v_msg = "Get information from the document..." 
    echo_msg(v_prg, v_stp, v_msg, 1)

    ctc = ct_conn
    new_document = r_json 
    # if g_lvl >= 5:
    #    json.dump(new_document, sys.stdout, indent=4)
    document_id = new_document.get("id")
    if document_id is None:
        v_stp = 3.1 
        v_msg = "Did not find documend_id."
        echo_msg(v_prg, v_stp, v_msg, 0)
        return None 
    doc_link = f"dbs/{db}/colls/{ct}/docs/{document_id}"
    v_msg = f" . Composed Doc Link: {doc_link}"
    echo_msg(v_prg, v_stp, v_msg, 2)

    # 3.2 build a row to track status 
    v_stp = 3.2 
    core_id = r_json.get("json", {}).get("Core", {}).get("Id")
    v_msg = f" . Found Core ID: {core_id} in the document."
    echo_msg(v_prg, v_stp, v_msg, 2)
    r_status = None
    df_row = {"rule_id": None, "core_id": None,  "user_id": None, "guid_id": None,
              "created": None, "changed": None, "status": None, "version": None,
              "publish_status": None}
    r_auth = r_json.get("json", {}).get("Authorities")
    r_ref = r_auth[0].get("Standards")[0].get("References")
    r_id = r_ref[0].get("Rule_Identifier",{}).get("Id")
    core_status = r_json.get("json", {}).get("Core", {}).get("Status")
    df_row.update({"rule_id": r_id})
    df_row.update({"core_id": core_id} )
    df_row.update({"user_id": r_json.get("creator",{}).get("id")})
    df_row.update({"guid_id": r_json.get("id")})
    df_row.update({"created": r_json.get("created")})
    df_row.update({"changed": r_json.get("changed")})
    df_row.update({"status": core_status})
    # v_vers = r_json.get("json", {}).get("Authorities", {}).get(
    #    "Standards", {}).get("Version", {})
    v_vs = []
    v_vers = None
    # print(r_json["json"]) 
    # for authority in r_json['json']['Authorities']: 
    for authority in r_auth: 
        for standard in authority['Standards']:
            v_vs.append(standard['Version'])
    v_vers = ", ".join(v_vs)
    df_row.update({"version": v_vers})

    # 4.0 add or replace document
    v_stp = 4.0 
    v_msg = "Publishing the document..."
    echo_msg(v_prg, v_stp, v_msg, 1)

    v_stp = 4.1
    v_msg = "Backing up the docuemnt first..."
    fn_path = os.path.dirname(log_fn)
    echo_msg(v_prg, v_stp, v_msg, 2)
    if r_ids is not None:
        v_stp = 4.11
        docs = r_ids[r_id]["ids"] if r_id in r_ids.keys() else None
        doc_cnt = 0
        if docs is not None:
            for d_id in docs:  
                doc_cnt += 1
                fn=f"{fn_path}/{r_id}-{core_status}-{doc_cnt}.json"        
                e_doc = ctc.read_item(item=d_id, partition_key=d_id)
                with open(fn, 'w') as f:
                    v_stp = 4.111
                    v_msg = "Writing to: " + fn
                    echo_msg(v_prg, v_stp, v_msg, 3)
                    json.dump(e_doc, f, indent=4)
                # Delete the existing document
                v_stp = 4.112
                ctc.delete_item(item=e_doc, partition_key=d_id)
                v_msg = f" . Document with id {d_id} deleted."
                echo_msg(v_prg, v_stp, v_msg, 3)
        r_status = "Added" if doc_cnt == 0 else "Replaced" 
    else:
        v_stp = 4.12
        v_msg = "No backup IDs are defined. "
        echo_msg(v_prg, v_stp, v_msg, 3)

    v_stp = 4.2
    v_msg = "Let's publish the document..."
    echo_msg(v_prg, v_stp, v_msg, 2)
    try:
        v_stp = 4.21
        # Check if the document exists
        existing_document = ctc.read_item(
            item=document_id, partition_key=document_id)
        e_doc_id = existing_document.get("id")
        v_msg = f"Found it: ({document_id})=({e_doc_id})"
        echo_msg(v_prg, v_stp, v_msg, 2)

        # Get the self link of the existing document
        document_link = existing_document["_self"]
        v_msg = f" . Existing Doc Link: {document_link}"
        echo_msg(v_prg, v_stp, v_msg, 2)

        # # If the document exists, replace it with the new document
        # ctc.replace_item(item=document_link, body=new_document,
        #              partition_key=document_id)
        # v_msg = f" . Document with id {document_id} replaced."
        # echo_msg(v_prg, v_stp, v_msg, 2)
        # r_status = "Published: replaced"

        # Delete the existing document
        ctc.delete_item(item=existing_document, partition_key=document_id)
        v_msg = f" . Document with id {document_id} deleted."
        echo_msg(v_prg, v_stp, v_msg, 2)

        # Create a new document with the same id
        ctc.create_item(body=new_document, partition_key=document_id)
        v_msg = f" . Document with id {document_id} created."
        echo_msg(v_prg, v_stp, v_msg, 2)
        if r_status is None: 
            r_status = "Replaced"
    except CosmosResourceNotFoundError:
        v_stp = 4.22
        # If the document does not exist, create it
        ctc.create_item(body=new_document, partition_key=document_id)
        v_msg = f"  Document with id {document_id} created."
        echo_msg(v_prg, v_stp, v_msg, 2)
        if r_status is None: 
            r_status = "Added"

    df_row.update({"publish_status": r_status})

    return df_row 


if __name__ == "__main__":
    load_dotenv()
    out_dir = os.getenv("output_dir")
    rule_dir = out_dir + "/rules_json"
    os.environ["g_lvl"] = "5"
    db_name="library"
    ct_name = "core_rules_dev"
    cfg = get_db_cfg(db_name=db_name, container_name=ct_name)
    ct_conn = cfg["ct_conn"]


    # Test case 1: Use Rule ID
    r_id = "CG0006"
    r_rst = publish_a_rule(rule_id=r_id,db_cfg=cfg)
    print(f"Status: {r_rst}")
    # json.dump(r_1, sys.stdout, indent=4)

    # Test case 2: Use Doc ID
    # d_id = "9959136a-523f-4546-9520-ffa22cda8867"     # CG0006
    d_id = "6161d4d5-6c96-47e1-baeb-4e70b1ffed46"       # CG0443
    # r_rst = publish_a_rule(doc_id=d_id, db_cfg=cfg)
    # print(f"Stauts: {r_rst}")
    # json.dump(r_2, sys.stdout, indent=4)

    # Test case 3: Provide both rule and doc IDs
    d_id = "8e4ce5f6-5d7d-420c-887d-26c09b89b793"   # CG0008
    # r_rst = publish_a_rule(rule_id=r_id, doc_id=d_id, db_cfg=cfg)
    # print(f"Stauts: {r_rst}")
    # json.dump(r_3, sys.stdout, indent=4)
