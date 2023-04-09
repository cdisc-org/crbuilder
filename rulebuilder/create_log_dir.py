# Purpose: Create log directory for storing log files
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   04/03/2023 (htu) - initial coding by extracting code from proc_sdtm_rules 
#   04/07/2023 (htu) - used getenv to get job_id and sub_dir
#

import os 
import datetime as dt
from dotenv import load_dotenv
# from datetime import datetime, timezone
from rulebuilder.echo_msg import echo_msg


def create_log_dir(log_dir: str = None, job_id: str = None, 
                   sub_dir: str = None, wrt2log: int = 1,
                   fn_root: str = "log",fn_sufix:str ="xlsx"):
    v_prg = __name__

    # 1.0 check inputs 
    v_stp = 1.0 
    v_msg = "Checking input parameters..."
    echo_msg(v_prg, v_stp, v_msg, 2)
    load_dotenv()
    r_dir = os.getenv("r_dir")
    tm = dt.datetime.now()
    job_id = tm.strftime("J%H%M%S")
    s_dir = tm.strftime("/%Y/%m/%d")
    sub_dir = f"{s_dir}/{job_id}"
    r_dir = "." if r_dir is None else r_dir
    log_dir = os.getenv("log_dir") if log_dir is None else log_dir
    log_dir = f"{r_dir}/logs" if log_dir is None else log_dir 
    log_f1 = f"{log_dir}{s_dir}/job-{job_id}-log1.txt"
    log_f2 = f"{log_dir}{s_dir}/job-{job_id}-log2.txt"

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
    
    if job_id is None: 
        job_id = os.getenv("job_id")
        if job_id is None: 
            job_id = tm.strftime("%Y%m%d_%H%M%S")
        r_cfg["job_id"] = job_id

    if sub_dir is None:
        sub_dir = tm.strftime("/%Y/%m/%d") + job_id
        r_cfg["sub_dir"] = sub_dir
    
    # get enviroment setting for write2log 
    w2log = 0 if w2log is None else int(w2log)
    r_cfg["wrt2log"] = wrt2log 
    # we use the input from this as higher priority
    if wrt2log > 0:
        os.environ["write2log"] = "1"

    # 2.0 check log_dir 
    if not os.path.exists(log_dir):
        v_stp = 2.1
        v_msg = f"Could not find log dir: {log_dir}"
        echo_msg(v_prg, v_stp, v_msg, 3)
        if wrt2log > 0: 
            v_msg = f"Making dir - {log_dir}"
            echo_msg(v_prg, v_stp, v_msg, 3)
            os.makedirs(log_dir)

    log_fdir = log_dir + sub_dir
    r_cfg["log_fdir"] = log_fdir
    if not os.path.exists(log_fdir):
        v_stp = 2.2
        v_msg = f"Could not find log dir: {log_fdir}"
        echo_msg(v_prg, v_stp, v_msg, 3)
        if wrt2log > 0: 
            v_msg = f"Making dir - {log_fdir}"
            echo_msg(v_prg, v_stp, v_msg, 3)
            os.makedirs(log_fdir)
    
    fn=f"{fn_root}-{job_id}.{fn_sufix}"
    r_cfg["file_name"]=fn
    r_cfg["fn_path"]=f"{log_fdir}/{fn}"
     
    
    return r_cfg


if __name__ == "__main__":
    load_dotenv()
    log_dir = os.getenv("log_dir")
    tm = dt.datetime.now(dt.timezone.utc)

    # Test case 1: Use Rule ID
    fdir = log_dir + tm.strftime("/%Y/%m/%Y%m%d_%H%M%S")
    d1 = create_log_dir()
    print(f"File Paht: {d1['fn_path']}")
    assert fdir == d1["log_fdir"] 

    # Test case 2: Use Doc ID
    j_id = "a1234"
    fdir = log_dir + tm.strftime("/%Y/%m/") + j_id 
    d2 = create_log_dir(job_id = j_id )
    print(f"File Paht: {d2['fn_path']}") 
    assert fdir == d2["log_fdir"] 

    print("All tests run successfully")

