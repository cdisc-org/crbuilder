# Purpose: Copy rules from one container to another container in a Cosmos DB
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   04/13/2023 (htu) - initial coding based on get_doc_stats
#   04/14/2023 (htu) - 
#    1. added code to check the target before deleting docs
#    2. added steps 1.3 and  5
#   04/17/2023 (htu) - added delete_target_docs parameter and used delete_rules 
#
import os
import datetime as dt
from dotenv import load_dotenv
from rulebuilder.echo_msg import echo_msg
from rulebuilder.get_db_cfg import get_db_cfg
from rulebuilder.delete_rules import delete_rules 
from rulebuilder.create_log_dir import create_log_dir
from rulebuilder.get_doc_stats import get_doc_stats


def copy_rules(r_str:str = None, f_ct:str=None, t_ct:str=None, f_db: str = None, 
        t_db: str = None, write2file:int=0, delete_target_docs:int=0):
    v_prg = __name__
    # 1.0 check parameters
    st_all = dt.datetime.now()
    v_stp = 1.0
    if r_str is None:
        v_stp = 1.1
        v_msg = "Missing rule id list."
        echo_msg(v_prg, v_stp, v_msg, 0)
        return 

    if f_ct is None or t_ct is None or f_db is None:
        v_stp = 1.2
        v_msg = "Missing From or To DB or CT specification. "
        echo_msg(v_prg, v_stp, v_msg, 0)
        return
    
    tm = dt.datetime.now()
    job_id = tm.strftime("C%H%M%S")
    
    if write2file > 0:
        v_stp = 1.3 
        log_cfg = create_log_dir(job_id=job_id)
        log_fn = f"{log_cfg['log_fdir']}/{job_id}-detailed.txt" 
        os.environ["log_fn"] = log_fn


    t_db = f_db if t_db is None else t_db 
    v_msg = f"Copying rules ({r_str}) from {f_db}.{f_ct} to {t_db}.{t_ct}..."
    echo_msg(v_prg, v_stp, v_msg, 2)

    # 2.0 get DB configuration
    v_stp = 2.0
    v_msg = f"Get DB configuration..."
    echo_msg(v_prg, v_stp, v_msg, 3)

    cfg1 = get_db_cfg(db_name=f_db, ct_name=f_ct)
    cfg2 = get_db_cfg(db_name=t_db, ct_name=t_ct)
    c1 = cfg1["ct_conn"]        # source container
    c2 = cfg2["ct_conn"]        # target container 
    v_stp = 2.1
    v_msg = f"-----C1: {c1}\n-----C2: {c2}"
    echo_msg(v_prg, v_stp, v_msg, 3)

    # 3.0 get source container stats
    v_stp = 3.0
    v_msg = "Getting source DB stats..."
    echo_msg(v_prg, v_stp, v_msg, 3)
    r_ids = get_doc_stats(job_id=job_id, db_cfg=cfg1,wrt2file=write2file)

    if delete_target_docs == 1: 
        v_stp = 3.5
        v_msg = "Deleting docs in the target container..."
        echo_msg(v_prg, v_stp, v_msg, 3)
        delete_rules(r_str=r_str,db_cfg=cfg2,r_ids=r_ids, write2file=write2file)


    # 4.0 Loop through each rule ids requested
    v_stp = 4.0
    v_msg = f"Processing requested rule ids: {r_str}..."
    echo_msg(v_prg, v_stp, v_msg, 3)

    if r_str.upper() == "ALL":
        r_list = r_ids.keys()
    else: 
        r_list = [s.strip().upper() for s in r_str.split(',')]
    r_cnt = len(r_list)
    r_i = 0

    for r_id in r_list: 
        r_i += 1 
        v_stp = 4.1
        v_msg = f"{r_i}/{r_cnt}: Copying rule id {r_id}..."
        echo_msg(v_prg, v_stp, v_msg, 2)
        
        if r_id not in r_ids.keys():
            v_stp = 4.11
            v_msg = f"{r_i}/{r_cnt}: Did not find {r_id} in the source DB."
            echo_msg(v_prg, v_stp, v_msg, 4)
            continue
        else:
            v_stp = 4.12 
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
        r_j = 0
        rj_tot = len(v_doc) 
        echo_msg(v_prg, v_stp, v_msg, 4)
        for doc_id in v_doc:
            r_j += 1 
            v_stp = 4.31
            e_doc = None 
            core_id = None 
            try: 
                v_msg = f" . {r_j}/{rj_tot} Reading {doc_id} from {c1}..."
                echo_msg(v_prg, v_stp, v_msg, 4)
                e_doc = c1.read_item(item=doc_id, partition_key=doc_id)
                core_id = e_doc.get("json", {}).get("Core", {}).get("Id")
            except Exception as e:
                v_msg = f"Error: {e}\n . DocID: {doc_id}"
                echo_msg(v_prg, v_stp, v_msg, 4)
            v_stp = 4.32
            # if core_id is None:
            #     v_msg = f" . We could not find the source doc {doc_id} in {c1}."
            #     echo_msg(v_prg, v_stp, v_msg, 4)
            #     continue 
            try: 
                t_doc = c2.read_item(item=doc_id, partition_key=doc_id)
                c2.delete_item(item=t_doc, partition_key=doc_id)
                v_msg = f" . Document with id {doc_id} {core_id} deleted from {c2}."
                echo_msg(v_prg, v_stp, v_msg, 4)
            except Exception as e: 
                v_msg = f"Error: {e}\n . Doc {doc_id} does not exist in {c2}"
                echo_msg(v_prg, v_stp, v_msg, 4)
            v_stp = 4.33
            try: 
                # Create a new document with the same id
                c2.create_item(body=e_doc, partition_key=doc_id)
                v_msg = f" . Document with id {doc_id} {core_id} created in {c2}."
                echo_msg(v_prg, v_stp, v_msg, 4)
                # break 
            except Exception as e:
                v_msg = f"Error: {e}\n . DocID: {doc_id},  CoreID: {core_id}"
                echo_msg(v_prg, v_stp, v_msg, 4)
    # End of or r_id in r_list

    v_stp = 5.0 
    et_all = dt.datetime.now()
    st = st_all.strftime("%Y-%m-%d %H:%M:%S")
    et = et_all.strftime("%Y-%m-%d %H:%M:%S")
    v_msg = f"The job {job_id} was done between: {st} and {et}"
    echo_msg(v_prg, v_stp, v_msg,1)


# Test cases
if __name__ == "__main__":
    # set input parameters
    load_dotenv()
    os.environ["g_msg_lvl"] = "3"
    os.environ["write2log"] = "0"
    v_prg = __name__ + "::copy_rules"
    # r_str = "CG0143, CG0100,CG0377,CG0041,CG0033"
    # r_str = "CG0509, CG0630, CG0136, CG0110, CG0111"
    # r_str = "CG0006"
    # r_str = "CG0001,CG0002,CG0006,CG0017,CG0155,CG0156,CG0100,CG0143"
    r_str = "ALL"
    # r_str = "CG0157,CG0158,CG0159"
    f_db = 'library'
    t_db = 'library'
    f_ct = 'editor_rules_dev_20230411'
    t_ct = 'editor_rules_dev'
    copy_rules(r_str, f_ct, t_ct, f_db, t_db, write2file=1)
