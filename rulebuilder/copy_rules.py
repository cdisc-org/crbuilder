# Purpose: Create a Container in a Cosmos DB
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   04/13/2023 (htu) - initial coding based on get_doc_stats
#
import os
from dotenv import load_dotenv
from rulebuilder.echo_msg import echo_msg
from rulebuilder.get_db_cfg import get_db_cfg
from rulebuilder.get_doc_stats import get_doc_stats
from azure.cosmos.exceptions import CosmosResourceNotFoundError


def copy_rules(r_str:str = None, f_ct:str=None, t_ct:str=None, f_db: str = None, 
        t_db: str = None, write2file:int=0):
    v_prg = __name__
    # 1.0 check parameters
    v_stp = 1.0
    if r_str is None:
        v_stp = 1.1
        v_msg = "Missing rule id list."
        echo_msg(v_prg, v_stp, v_msg, 0)
        return 
    r_list = [s.strip().upper() for s in r_str.split(',')]

    if f_ct is None or t_ct is None or f_db is None:
        v_stp = 1.2
        v_msg = "Missing From or To DB or CT specification. "
        echo_msg(v_prg, v_stp, v_msg, 0)
        return

    t_db = f_db if t_db is None else t_db 
    v_msg = f"Copying rules ({r_str}) from {f_db}.{f_ct} to {t_db}.{t_ct}..."
    v_msg += f"\n . {r_list}"
    echo_msg(v_prg, v_stp, v_msg, 2)

    # 2.0 get DB configuration
    v_stp = 2.0
    v_msg = f"Get DB configuration..."
    echo_msg(v_prg, v_stp, v_msg, 3)

    cfg1 = get_db_cfg(db_name=f_db, ct_name=f_ct)
    cfg2 = get_db_cfg(db_name=t_db, ct_name=t_ct)
    c1 = cfg1["ct_conn"]
    c2 = cfg2["ct_conn"]

    # 3.0 get source container stats
    v_stp = 3.0
    v_msg = "Getting source DB stats..."
    echo_msg(v_prg, v_stp, v_msg, 3)
    r_ids = get_doc_stats(db_cfg=cfg1,wrt2file=write2file)

    # 4.0 Loop through each rule ids requested
    v_stp = 4.0
    v_msg = f"Processing requested rule ids: {r_list}..."
    echo_msg(v_prg, v_stp, v_msg, 3)
    r_cnt = len(r_list)
    r_i = 0
    for r_id in r_list: 
        r_i += 1 
        v_stp = 4.1
        if r_id not in r_ids.keys():
            v_msg = f"{r_i}/{r_cnt}: Did not find {r_id} in the source DB."
            echo_msg(v_prg, v_stp, v_msg, 4)
            continue
        else:
            v_msg = f"{r_i}/{r_cnt}: Found {r_id} in the source DB."
            echo_msg(v_prg, v_stp, v_msg, 4)
        v_cnt = r_ids[r_id]["cnt"]
        v_doc = r_ids[r_id]["ids"]
        if v_cnt > 1:
            v_stp = 4.2
            v_msg = f"There are {v_cnt} duplicates:{v_doc}"
            echo_msg(v_prg, v_stp, v_msg, 4)
        v_stp = 4.3
        v_msg = f"Copying each doc for {r_id}..."
        echo_msg(v_prg, v_stp, v_msg, 4)
        for doc_id in v_doc: 
            v_stp = 4.31
            e_doc = None 
            try: 
                v_msg = f"Reading doc {doc_id}..."
                echo_msg(v_prg, v_stp, v_msg, 4)
                e_doc = c1.read_item(item=doc_id, partition_key=doc_id)
            except Exception as e:
                v_msg = f"Error: {e}\n . DocID: {doc_id}"
                echo_msg(v_prg, v_stp, v_msg, 4)
            core_id = e_doc.get("json", {}).get("Core", {}).get("Id")
            v_stp = 4.32
            try:
                c2.delete_item(item=e_doc, partition_key=doc_id)
                v_msg = f" . Document with id {doc_id} deleted."
                echo_msg(v_prg, v_stp, v_msg, 4)
            except Exception as e:
                v_msg = f"Error: {e}\n . DocID: {doc_id},  CoreID: {core_id}"
                echo_msg(v_prg, v_stp, v_msg, 4)
            v_stp = 4.33
            try: 
                # Create a new document with the same id
                c2.create_item(body=e_doc, partition_key=doc_id)
                v_msg = f" . Document with id {doc_id} created."
                echo_msg(v_prg, v_stp, v_msg, 4)
                break 
            except Exception as e:
                v_msg = f"Error: {e}\n . DocID: {doc_id},  CoreID: {core_id}"
                echo_msg(v_prg, v_stp, v_msg, 4)

    # End of for i in doc_list



# Test cases
if __name__ == "__main__":
    # set input parameters
    load_dotenv()
    os.environ["g_msg_lvl"] = "5"
    os.environ["write2log"] = "0"
    v_prg = __name__ + "::copy_rules"
    r_ids = "CG0143, CG0100,CG0377"
    f_db = 'library'
    t_db = 'library'
    f_ct = 'editor_rules_dev_20230411'
    t_ct = 'editor_rules_dev'
    copy_rules(r_ids, f_ct, t_ct, f_db, t_db)
