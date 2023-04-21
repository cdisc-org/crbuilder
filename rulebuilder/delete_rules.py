# Purpose: Delete documents from a Container in a Cosmos DB
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   04/18/2023 (htu) - initial coding based on get_doc_stats and copy_rules
#
import os
import datetime as dt
from dotenv import load_dotenv
from rulebuilder.echo_msg import echo_msg
from rulebuilder.get_db_cfg import get_db_cfg
from rulebuilder.create_log_dir import create_log_dir
from rulebuilder.get_doc_stats import get_doc_stats

def delete_rules(r_str: str = None, ct_name: str = None, db_name: str = None,
               db_cfg = None, r_ids = None, write2file:int=0):
    v_prg = __name__
    # 1.0 check parameters
    st_all = dt.datetime.now()
    v_stp = 1.0
    if r_str is None:
        v_stp = 1.1
        v_msg = "Missing rule id list."
        echo_msg(v_prg, v_stp, v_msg, 0)
        return

    if db_cfg is None and (ct_name is None or db_name is None):
        v_stp = 1.2
        v_msg = "Missing DB or CT specification. "
        echo_msg(v_prg, v_stp, v_msg, 0)
        return
    
    tm = dt.datetime.now()
    job_id = tm.strftime("D%H%M%S")
    if write2file > 0:
        v_stp = 1.3 
        log_cfg = create_log_dir(job_id=job_id)
        log_fn = f"{log_cfg['log_fdir']}/{job_id}-deleted.txt" 
        os.environ["log_fn"] = log_fn


    # 2.0 get DB configuration
    v_stp = 2.0
    v_msg = f"Get DB configuration..."
    echo_msg(v_prg, v_stp, v_msg, 3)

    if db_cfg is None: 
        cfg = get_db_cfg(db_name=db_name, ct_name=ct_name)
    else: 
        cfg = db_cfg 
    ctc = cfg["ct_conn"]      

    # 3.0 get document list from the container
    v_stp = 3.0
    v_msg = f"Getting doc list from {db_name}.{ct_name}..."
    echo_msg(v_prg, v_stp, v_msg, 3)

    if r_ids is None:
        r_ids = get_doc_stats(job_id=job_id, db_cfg=cfg, wrt2file=write2file)
    
    v_stp = 3.1
    tot_docs = len(r_ids)
    v_msg = f" . Total number of docs: {tot_docs}."
    echo_msg(v_prg, v_stp, v_msg, 3)
        
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
        v_msg = f"{r_i}/{r_cnt}: Deleting rule id {r_id}..."
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
        v_msg = f"Deleting each doc for {r_id}..."
        r_j = 0
        rj_tot = len(v_doc)
        echo_msg(v_prg, v_stp, v_msg, 4)
        for doc_id in v_doc:
            r_j += 1
            v_stp = 4.31
            core_id = None
            v_stp = 4.32
            v_msg = f" . {r_j}/{rj_tot}: Deleting {doc_id}..."
            echo_msg(v_prg, v_stp, v_msg, 4)
            try:
                t_doc = ctc.read_item(item=doc_id, partition_key=doc_id)
                core_id = t_doc.get("json", {}).get("Core", {}).get("Id")
                ctc.delete_item(item=t_doc, partition_key=doc_id)
                v_msg = f" . Document with id {doc_id} {core_id} deleted from {ctc}."
                echo_msg(v_prg, v_stp, v_msg, 4)
            except Exception as e:
                v_msg = f"Error: {e}\n . Doc {doc_id} does not exist in {ctc}"
                echo_msg(v_prg, v_stp, v_msg, 4)
            v_stp = 4.33
    # End of or r_id in r_list

    v_stp = 5.0
    et_all = dt.datetime.now()
    st = st_all.strftime("%Y-%m-%d %H:%M:%S")
    et = et_all.strftime("%Y-%m-%d %H:%M:%S")
    v_msg = f"The job {job_id} was done between: {st} and {et}"
    echo_msg(v_prg, v_stp, v_msg, 1)


# Test cases
if __name__ == "__main__":
    # set input parameters
    load_dotenv()
    os.environ["g_msg_lvl"] = "3"
    os.environ["write2log"] = "0"
    v_prg = __name__ + "::delete_rules"
    # r_str = "CG0143, CG0100,CG0377,CG0041,CG0033"
    # r_str = "CG0143"
    r_str = "ALL"
    # r_str = "CG0001,CG0002,CG0006,CG0017,CG0155,CG0156,CG0100,CG0143"
    t_db = 'library'
    t_ct = 'editor_rules_dev'
    # t_ct = "core_rules_dev"
    delete_rules(r_str, ct_name=t_ct, db_name=t_db, write2file=1)
