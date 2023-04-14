# Purpose: Create log directory for storing log files
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   04/03/2023 (htu) - initial coding by extracting code from proc_sdtm_rules 
#   04/07/2023 (htu) - used getenv to get job_id and sub_dir
#   04/10/2023 (htu) - removed some redundant codes 
#   04/14/2023 (htu) - added create_dir method and simplified the coding 
#

import os 
import datetime as dt
from dotenv import load_dotenv
# from datetime import datetime, timezone
from rulebuilder.echo_msg import echo_msg


def create_log_dir(log_dir: str = None, job_id: str = None, 
                   sub_dir: str = None, wrt2log: int = 1,
                   fn_root: str = "log",fn_suffix:str ="xlsx"):
    v_prg = __name__

    # 1.0 check inputs 
    v_stp = 1.0 
    v_msg = "Checking input parameters..."
    echo_msg(v_prg, v_stp, v_msg, 2)
    load_dotenv()
    r_dir = os.getenv("r_dir")
    tm = dt.datetime.now()
    job_id = os.getenv("job_id") if job_id is None else job_id 
    job_id = tm.strftime("J%H%M%S") if job_id is None else job_id 
    s_dir = tm.strftime("/%Y/%m/%d")
    sub_dir = f"{s_dir}/{job_id}"
    r_dir = "." if r_dir is None else r_dir
    log_dir = os.getenv("log_dir") if log_dir is None else log_dir
    log_dir = f"{r_dir}/logs" if log_dir is None else log_dir 
    log_f1 = f"{log_dir}{sub_dir}/job-{job_id}-log1.txt"
    log_f2 = f"{log_dir}{sub_dir}/job-{job_id}-log2.txt"

    os.environ["job_id"] = job_id
    os.environ["s_dir"] = s_dir
    os.environ["sub_dir"]  = sub_dir
    os.environ["log_dir"] = log_dir
    os.environ["log_fn_1"] = log_f1
    os.environ["log_fn_2"] = log_f2

    # now_utc = datetime.now(timezone.utc)
    # now_utc = datetime.now()
    w2log = os.getenv("write2log")

    r_cfg = {"job_id": job_id,"r_dir":r_dir, "s_dir": s_dir, "sub_dir": sub_dir,
             "log_dir":log_dir, "log_fn_1": log_f1, "log_fn_2": log_f2}
    log_dir = os.getenv("log_dir") if log_dir is None else log_dir 
    if log_dir is None:
        v_stp = 1.1
        v_msg = "No log_dir is provided."
        echo_msg(v_prg, v_stp, v_msg, 0)
        return r_cfg 
    r_cfg["log_dir"] = log_dir
        
    # get environment setting for write2log 
    w2log = 0 if w2log is None else int(w2log)
    r_cfg["wrt2log"] = wrt2log 
    # we use the input from this as higher priority
    if wrt2log > 0:
        os.environ["write2log"] = "1"

    def create_dir (v_stp, v_dir):
        if not os.path.exists(v_dir):
            v_msg = f"Dir does not exist: {v_dir}"
            echo_msg(v_prg, v_stp, v_msg, 3)
            if wrt2log > 0: 
                v_msg = f"Making dir - {v_dir}"
                echo_msg(v_prg, v_stp, v_msg, 3)
                os.makedirs(v_dir)

    # 2.0 check log_dir 
    create_dir(2.1, log_dir)

    log_fdir = f"{log_dir}{sub_dir}/logs"
    create_dir(2.2, log_fdir)

    r_cfg["log_fdir"] = log_fdir
    
    fn=f"{fn_root}-{job_id}.{fn_suffix}"
    r_cfg["file_name"]=fn
    r_cfg["fn_path"]=f"{log_fdir}/{fn}"

    # 3.0 Check rule output folders
    # existing_rule_dir = ./data/output/orig_rules
    #json_rule_dir = ./data/output/orig_json
    #output_dir = ./data/output
    e_rule_dir = os.getenv("existing_rule_dir")
    j_rule_dir = os.getenv("json_rule_dir")
    output_dir = os.getenv("output_dir")

    if e_rule_dir is not None:
        create_dir(3.1, e_rule_dir)
    if j_rule_dir is not None:
        create_dir(3.1, j_rule_dir)
    if output_dir is not None:
        create_dir(3.1, output_dir)
    
    return r_cfg


if __name__ == "__main__":
    load_dotenv()
    log_dir = os.getenv("log_dir")
    tm = dt.datetime.now()

    # Test case 1: Use Rule ID
    fdir = log_dir + tm.strftime("/%Y/%m/%d/J%H%M%S")
    d1 = create_log_dir()
    print(f"File Path: {d1['fn_path']}")
    assert fdir == d1["log_fdir"] 

    # Test case 2: Use Doc ID
    j_id = "a1234"
    fdir = log_dir + tm.strftime("/%Y/%m/%d/") + j_id 
    d2 = create_log_dir(job_id = j_id )
    print(f"File Path: {d2['fn_path']}") 
    assert fdir == d2["log_fdir"] 

    print("All tests run successfully")

