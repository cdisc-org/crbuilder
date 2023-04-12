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
#
import os
import re
import json 
import pandas as pd
import datetime as dt
from rulebuilder.get_db_cfg import get_db_cfg
from rulebuilder.echo_msg import echo_msg
from dotenv import load_dotenv
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
    v_msg = f"Get DB configuration ({db_cfg})..."
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
                    echo_msg(v_prg, v_stp, v_msg, 3)

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
    df_log = pd.DataFrame(columns=["rule_id", "core_id",  "user_id", "guid_id", 
                                   "created", "changed", "rule_status", "version", 
                                   "doc_cnt", "dup_ids"])
    v_stp = 4.1
    v_msg = "Processing each doc..."
    echo_msg(v_prg, v_stp, v_msg, 2)
    r_ids = {}                  # contain a list of rule ids 
    r_key_with_space = {}
    rows = []
    v_re1 = r'Id:\s*(\w+)\s*\r'
    tot_docs = len(doc_list)
    doc_cnt  = 0 
    c_id = None                 # last component from Core.Id
    for i in doc_list:
        doc_cnt += 1 
        df_row = {"rule_id": None, "core_id": None,  "user_id": None, "guid_id": None,
              "created": None, "changed": None, "rule_status": None, "version": None,
              "doc_cnt":None, "dup_ids": None}
        doc_id = i.get("id")
        # Core.Id: FDA.SDTMIG.CT2001
        core_id = i.get("json", {}).get("Core", {}).get("Id")
        core_status = i.get("json", {}).get("Core", {}).get("Status")
        if core_id is not None:
            c_id = core_id.split(".")[-1]

        # get rule id
        v_auth = None 
        v_ref  = None  
        r_id   = None
        try: 
            v_stp = 4.11
            v_msg = f" {doc_cnt}/{tot_docs} Trying to get Rule ID from [{core_id}] {doc_id}..."
            echo_msg(v_prg, v_stp, v_msg, 4)
            if core_id is None or len(core_id) == 0:
                echo_msg(v_prg, v_stp, i, 9)
            r_auth = i.get("json", {}).get("Authorities")
            r_ref = r_auth[0].get("Standards")[0].get("References")
            r_id = r_ref[0].get("Rule_Identifier", {}).get("Id")    # rule id 
        except Exception as e:
            v_stp = 4.12
            # print(f"I-Doc: {i}")
            v_msg = f"Error: {e}\n . DocID: {doc_id},  CoreID: {core_id}"
            v_msg += f"\n . Auth: {v_auth}\n . Ref: {v_ref}\n . ID: {r_id}"
            echo_msg(v_prg, v_stp, v_msg, 2)
            deep_match(i, v_re1, doc_id)
            r_id = c_id 
        if r_id is None:
            v_stp = 4.13
            v_msg = "Still trying to get Rule ID..."
            echo_msg(v_prg, v_stp, v_msg, 4)
            r_id = r_ref[0].get("Rule Identifier", {}).get("Id")    # wrong way to have rule id 
            if r_id is not None:
                if r_id not in r_key_with_space.keys():
                    r_key_with_space[r_id] = []
                r_key_with_space[r_id].append(doc_id)
        if r_id is None:                 # if rule id is still None,
            v_stp = 4.14 
            v_msg = "We still did not find Rule ID, and will assign 'NoRuleID'."
            echo_msg(v_prg, v_stp, v_msg, 4)
            r_id = "NoRuleID"            # we assigned "NoRuleID" to it
        if r_id not in r_ids.keys():
            r_ids[r_id] = {"cnt":0, "ids": []}
        if r_id != c_id and c_id is not None and r_id is not None:
            if c_id not in r_ids.keys():
                r_ids[c_id] = {"cnt": 0, "ids": []}
            r_ids[c_id]["cnt"] += 1
            r_ids[c_id]["ids"].append(doc_id)
            df_row.update({"rule_id": c_id})
        r_ids[r_id]["cnt"] += 1
        r_ids[r_id]["ids"].append(doc_id)
        df_row.update({"rule_id": r_id})
        # get a list of IG versions 
        v_vs = []
        v_vers = None
        if r_auth is not None: 
            v_stp = 4.15
            for authority in r_auth:
                for standard in authority['Standards']:
                    v_vv1 = standard.get('Version')
                    if v_vv1 is not None: 
                        v_vs.append(v_vv1)
        if len(v_vs) > 0: 
            v_vers = ", ".join(v_vs) 
        df_row.update({"version": v_vers})

        df_row.update({"core_id": core_id})
        df_row.update({"user_id": i.get("creator", {}).get("id")})
        df_row.update({"guid_id": doc_id})
        df_row.update({"created": i.get("created")})
        df_row.update({"changed": i.get("changed")})
        df_row.update({"rule_status": core_status})
        df_row.update({"doc_cnt": r_ids[r_id]["cnt"]})
        df_row.update({"dup_ids": r_ids[r_id]["ids"]})

        rows.append(df_row)

    # End of for i in doc_list

    v_stp = 4.2
    v_msg = "Get doc cnt and dup doc ids..."
    echo_msg(v_prg, v_stp, v_msg, 2)


    df_log = pd.DataFrame.from_records(rows)
    grouped_data = df_log.groupby("rule_id")

    for rule_id, grp in grouped_data:
        if grp.shape[0] > 1:  
            v_msg = f"RuleID: {rule_id}:\n{grp}"
            echo_msg(v_prg, v_stp, v_msg, 6)

    v_stp = 4.3 
    if wrt2file == 1: 
        v_stp = 4.31
        load_dotenv()
        log_dir = os.getenv("log_dir")
        tm = dt.datetime.now()
        s_dir = tm.strftime("/%Y/%m/%d/")
        if job_id is None: 
            job_id = tm.strftime("%Y%m%d_%H%M%S")
        rst_fn = log_dir + s_dir + f"job-{job_id}-stat.xlsx"
        jsn_fn = log_dir + s_dir + f"job-{job_id}-stat.json"
        v_msg = "Output result to " + rst_fn + "..." 
        echo_msg(v_prg, v_stp, v_msg,2)
        df_log.to_excel(rst_fn, index=False)
        v_stp = 4.32
        with open(jsn_fn, 'w') as f:
            v_msg = f"Writing to: {jsn_fn}"
            echo_msg(v_prg, v_stp, v_msg, 3)
            json.dump(r_ids, f, indent=4)
    else:
        v_stp = 4.33

    n = len(r_ids)
    for i in r_ids:
        m = len(r_ids[i]["ids"])
        if m > 1:
            v_msg = f"{i}({m}/{n}): {r_ids[i]['ids']}"
            echo_msg(v_prg, v_stp, v_msg, 5)
    # n2 = len(r_key_with_space)
    # print(f"Docs with space keys ({n2}):\n{r_key_with_space}")
    return r_ids 


# Test cases
if __name__ == "__main__":
    # set input parameters
    os.environ["g_lvl"] = "5"
    v_prg = __name__ + "::proc_sdtm_rules"
    # rule_list = ["CG0373", "CG0378", "CG0379"]
    rule_list = ["CG0001"]
    # rule_list = []
    q1 = "select * from c where not is_defined(c.creator)"
    q2 = "select * from c"
    get_doc_stats()
