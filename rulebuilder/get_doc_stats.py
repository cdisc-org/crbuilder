# Purpose: Create a Container in a Cosmos DB
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/31/2023 (htu) - initial coding based on
#     https://learn.microsoft.com/en-us/python/api/overview/azure/cosmos-readme?view=azure-python#create-a-container
#   04/04/2023 (htu) - converted code into get_db_documents
#   04/05/2023 (htu) - added docstring
#   04/07/2023 (htu) - 
#     1. added job_id and populated values for doc_cnt and dup_ids 
#     2. added db_cfg 
#   04/11/2023 (htu) - 
#     1. added more debug message in step 4.12
#     2. added deep_match 
#     3. added step 4.32 to write out r_ids 
#   04/14/2023 (htu) - added r_ids[r_id]["status"]
#   04/18/2023 (htu) - added step 4.32 
#   04/19/2023 (htu) - sorted r_ids and df_log 
#
import os
import re
import json 
import pandas as pd
import datetime as dt
from dotenv import load_dotenv
from rulebuilder.echo_msg import echo_msg
from rulebuilder.get_db_cfg import get_db_cfg
from rulebuilder.get_rule_ids import get_rule_ids
from rulebuilder.create_a_dir import create_a_dir
from azure.cosmos.exceptions import CosmosResourceNotFoundError


def get_doc_stats(qry: str = None, db: str = 'library', 
                  ct: str = 'editor_rules_dev', wrt2file:int=0, 
                  job_id: str=None,db_cfg = None):
    """
    Retrieves statistics from a Cosmos DB container.

    Parameters:
    qry (str): the SQL query to execute on the container (default is "SELECT * FROM c")
    db (str): the name of the Cosmos DB database to connect to (default is "library")
    ct (str): the name of the Cosmos DB container to connect to (default is "editor_rules_dev")
    wrt2file (int): whether to write the result to an Excel file (1) or not (0) (default is 0)
    db_cfg (dict): database configuration 

    Returns:
    A dictionary containing document statistics, where each key is a rule ID and the 
    corresponding value is a dictionary containing the following keys:
        - cnt: the number of documents with this rule ID
        - ids: a list of document IDs for documents with this rule ID
    """
    v_prg = __name__
    # 1.0 check parameters

    if qry is None:
        qry = "SELECT * FROM c"
    # query_options = {'enable_cross_partition_query': True}

    # . 2.0 get DB configuration
    v_stp = 2.0
    v_msg = f"Get DB configuration ({db}.{ct})..."
    echo_msg(v_prg, v_stp, v_msg, 2)
    if db_cfg is None: 
        v_stp = 2.1 
        # print("Getting CFG....")
        cfg = get_db_cfg(db_name=db, ct_name=ct)
    else: 
        v_stp = 2.2 
        cfg = db_cfg 
    v_msg = f"DB: {db}, CT: {ct}"
    echo_msg(v_prg, v_stp, v_msg, 3)

    ctc = cfg["ct_conn"]

    def deep_match(data, pattern, doc_id=None):
        for key, value in data.items():
            if isinstance(value, dict):
                deep_match(value, pattern)
            else:
                match = re.search(pattern, str(value))
                if match:
                    v_stp = 2.3
                    v_msg = f"Match found for {key}: {match.group()} in {doc_id}"
                    echo_msg(v_prg, v_stp, v_msg, 5)

    # 3.0 Execute the query
    doc_list = list()
    try:
        v_stp = 3.1
        doc_list = list(
            ctc.query_items(query=qry,
                            enable_cross_partition_query=True
                            # ,partition_key=PartitionKey('id')
                            ))
    except CosmosResourceNotFoundError:
        v_stp = 3.2
        v_msg = f"Could not run query - {qry}"
        echo_msg(v_prg, v_stp, v_msg, 0)
        return

    # 4.0 process the results
    df_log = pd.DataFrame(columns=["rule_id", "core_id",  
                                   "combined_id", "user_id", "guid_id",
                                   "created", "changed", "rule_status", "version",
                                   "rule_cnt", "rule_ids",
                                   "doc_cnt", "dup_ids"])   
    v_stp = 4.1
    v_msg = "Processing each doc..."
    echo_msg(v_prg, v_stp, v_msg, 3)
    r_ids = {}                  # contain a list of rule ids 
    rows = []
    v_re1 = r'Id:\s*(\w+)\s*\r'
    tot_docs = len(doc_list)
    doc_cnt  = 0 
    for i in doc_list:
        doc_cnt += 1 
        doc_id = i.get("id")
        core_id = i.get("json", {}).get("Core", {}).get("Id")
        v_msg = f" {doc_cnt}/{tot_docs} To get Rule ID from [{core_id}] {doc_id}..."
        echo_msg(v_prg, v_stp, v_msg, 5)
        if core_id is None or len(core_id) == 0:
            echo_msg(v_prg, v_stp, i, 9)

        rule_id = None
        core_status = i.get("json", {}).get("Core", {}).get("Status")
        df_row = get_rule_ids(r_data=i)
        rule_id = df_row.get("rule_id")
        id_str = df_row.get("rule_ids")
        id_list = []
        if id_str is not None: 
            id_list = id_str.split(",")
        for r_id in id_list:  
            if r_id not in r_ids.keys():
                r_ids[r_id] = {"cnt":0, "ids":[], "status":[]}
            r_ids[r_id]["cnt"] += 1
            r_ids[r_id]["ids"].append(doc_id)
            r_ids[r_id]["status"].append(core_status)
        if rule_id is None or rule_id == "NoRuleID":
            deep_match(i, v_re1, doc_id)
        df_row.update({"doc_cnt": r_ids[r_id]["cnt"]})
        df_row.update({"dup_ids": r_ids[r_id]["ids"]})
        if rule_id is not None: 
            rows.append(df_row)

    # End of for i in doc_list

    v_stp = 4.2
    v_msg = "Get doc cnt and dup doc ids..."
    echo_msg(v_prg, v_stp, v_msg, 3)

    df_log = pd.DataFrame.from_records(rows)
    grouped_data = df_log.groupby("rule_id")
    df_sorted = df_log.sort_values(by='rule_id')

    for rule_id, grp in grouped_data:
        if grp.shape[0] > 1:  
            v_msg = f"RuleID: {rule_id}:\n{grp}"
            echo_msg(v_prg, v_stp, v_msg, 5)
    
    # Sort the r_ids dict object 
    sorted_keys = sorted(r_ids.keys())
    sorted_dict = {k: r_ids[k] for k in sorted_keys}


    v_stp = 4.3 
    if wrt2file == 1: 
        v_stp = 4.31
        load_dotenv()
        log_dir = os.getenv("log_dir")
        tm = dt.datetime.now()
        s_dir = tm.strftime("/%Y/%m/%d/")
        if job_id is None: 
            job_id = tm.strftime("S%H%M%S")
        t_dir = f"{log_dir}{s_dir}/{job_id}"
        create_a_dir(v_prg=v_prg, v_stp=4.32, v_dir=t_dir,wrt2log=wrt2file)

        rst_fn = f"{t_dir}/job-{job_id}-stat.xlsx"
        jsn_fn = f"{log_dir}{s_dir}/{job_id}/job-{job_id}-stat.json"
        v_msg = "Output result to " + rst_fn + "..." 
        echo_msg(v_prg, v_stp, v_msg,2)
        df_sorted.to_excel(rst_fn, index=False)
        v_stp = 4.33
        with open(jsn_fn, 'w') as f:
            v_msg = f"Writing to: {jsn_fn}"
            echo_msg(v_prg, v_stp, v_msg, 3)
            json.dump(sorted_dict, f, indent=4)
    else:
        v_stp = 4.34

    n = len(r_ids)
    for i in sorted_dict:
        m = len(sorted_dict[i]["ids"])
        if m > 1:
            v_msg = f"{i}({m}/{n}): {sorted_dict[i]['ids']}"
            echo_msg(v_prg, v_stp, v_msg, 5)
    # n2 = len(r_key_with_space)
    # print(f"Docs with space keys ({n2}):\n{r_key_with_space}")
    return sorted_dict


# Test cases
if __name__ == "__main__":
    # set input parameters
    os.environ["g_msg_lvl"] = "5"
    v_prg = __name__ + "::get_doc_stats"
    # get_doc_stats(ct="editor_rules_dev_20230411",wrt2file=1)
    get_doc_stats(ct="editor_rules_dev",wrt2file=1)
