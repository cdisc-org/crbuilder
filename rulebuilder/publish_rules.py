# Purpose: Publish CORE rules to Cosmos DB
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/31/2023 (htu) - initial coding based on
#     https://learn.microsoft.com/en-us/python/api/overview/azure/cosmos-readme?view=azure-python#create-a-container
#   04/03/2023 (htu) - called to publish_a_rule and added write2log
#
import os 
import pandas as pd
from dotenv import load_dotenv
from rulebuilder.echo_msg import echo_msg
from rulebuilder.create_log_dir import create_log_dir
from datetime import datetime, timezone
from rulebuilder.get_db_cfg import get_db_cfg
from rulebuilder.publish_a_rule import publish_a_rule


def publish_rules (rule_ids:list=["CG0001"], doc_ids:list=[],
                   rule_dir:str=None, db_cfg = None,
                   write2log:int = 0 
                   ):
    v_prg = __name__
    # 1.0 check input parameters
    v_stp = 1.0
    v_msg = "Publishing rules..."
    echo_msg(v_prg, v_stp, v_msg, 1)
    load_dotenv()
    now_utc = datetime.now(timezone.utc)
    job_id = now_utc.strftime("%Y%m%d_%H%M%S")

    if len(rule_ids) == 0 and len(doc_ids == 0):
        v_stp = 1.1
        v_msg = "No rule id nor doc id is provided. "
        echo_msg(v_prg, v_stp, v_msg, 0)
        return
    
    if rule_dir is None:
        v_stp = 1.2
        rule_dir = os.getenv("rule_json_dir")
        output_dir = os.getenv("output_dir")
        if rule_dir is None:
            rule_dir = output_dir + "/rules_json"
        if not os.path.exists(rule_dir):
            v_msg = f"Could not find rule_dir: {rule_dir}"
            echo_msg(v_prg, v_stp, v_msg, 0)
            return

    if db_cfg is None:
        v_stp = 1.3
        v_msg = "Non database connection is defined and provided."
        echo_msg(v_prg, v_stp, v_msg, 0)
        return

    ctc = db_cfg.get("ct_conn")
    if ctc is None:
        v_stp = 1.4
        v_msg = "Non container connection is defined and provided."
        echo_msg(v_prg, v_stp, v_msg, 0)
        return
    
    df_log = pd.DataFrame(columns=["rule_id", "core_id",  "user_id", "guid_id", 
                                   "created", "changed", "status", "version",
                                   "publish_status" ]
                        )
    df_row = {"rule_id": None, "core_id": None,  "user_id": None, "guid_id": None,
           "created": None, "changed": None, "status": None, "version": None,
           "publish_status": None}
    rows = [] 

 
    
    # 2.0 loop through rule list
    v_stp = 2.0
    v_msg = "Loop through rule list..."
    echo_msg(v_prg, v_stp, v_msg, 1)

    if len(rule_ids) > 0:
        v_stp = 2.1 
        for r in rule_ids: 
            v_msg = f" . Rule ID: {r}"
            echo_msg(v_prg, v_stp, v_msg, 2)
            df_row = publish_a_rule(rule_id=r, rule_dir=rule_dir,db_cfg=db_cfg)
            rows.append(df_row)
    else:
        v_stp = 2.2 
        v_msg = "No rule list is providedf."
        echo_msg(v_prg, v_stp, v_msg, 2)
    
    # 3.0 loop through document list
    v_stp = 3.0
    v_msg = "Loop through doc list..."
    echo_msg(v_prg, v_stp, v_msg, 1)
    if len(doc_ids) > 0:
        v_stp = 3.1
        for d in doc_ids:
            v_msg = f" . Doc ID: {d}"
            echo_msg(v_prg, v_stp, v_msg, 2)
            df_row=publish_a_rule(
                doc_id=d, rule_dir=rule_dir, db_cfg=db_cfg)
            rows.append(df_row)
    else:
        v_stp = 3.1 
        v_msg = "No doc list is providedf."
        echo_msg(v_prg, v_stp, v_msg, 2)

    # 4.0 write to 
    v_stp = 4.0
    df_log = pd.DataFrame.from_records(rows)

    if write2log == 1: 
        log_cfg = create_log_dir(job_id=job_id, wrt2log=1,fn_root="pub")
        rst_fn = log_cfg["fn_path"]
        v_msg = f"Output result to {rst_fn}..."
        df_log.to_excel(rst_fn, index=False)
    else:
        v_stp = 4.2 
        v_msg = f"Chosen not to write log file ({write2log})."


# Test cases
if __name__ == "__main__":
    # set input parameters
    os.environ["g_lvl"] = "5"
    v_prg = __name__ + "::publish_rules"
    db_name="library"
    ct_name = "core_rules_dev"
    cfg = get_db_cfg(db_name=db_name, ct_name=ct_name)
 
    # rule_list = ["CG0373", "CG0378", "CG0379"]
 
    # 1. Test with basic parameters
    v_stp = 1.0
    echo_msg(v_prg, v_stp, "Test Case 01: Basic Parameter", 1)
    r_list = ["CG0006"]
 
    # proc_sdtm_rules(rule_ids=["CG0006"], wrt2log=True)
    publish_rules(rule_ids=r_list, write2log=1, db_cfg=cfg)


# End of File
