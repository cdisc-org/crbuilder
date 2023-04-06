# Purpose: Create a Container in a Cosmos DB
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/31/2023 (htu) - initial coding based on
#     https://learn.microsoft.com/en-us/python/api/overview/azure/cosmos-readme?view=azure-python#create-a-container
#   04/04/2023 (htu) - converted code into get_db_documents
#   04/05/2023 (htu) - added docstring
#
import os
import pandas as pd
import datetime as dt
from rulebuilder.get_db_cfg import get_db_cfg
from rulebuilder.echo_msg import echo_msg
from dotenv import load_dotenv
from azure.cosmos.exceptions import CosmosResourceNotFoundError


def get_doc_stats(qry: str = None, db: str = 'library', 
                  ct: str = 'editor_rules_dev', wrt2file:int=0):
    """
    Retrieves statistics from a Cosmos DB container.

    Parameters:
    qry (str): the SQL query to execute on the container (default is "SELECT * FROM c")
    db (str): the name of the Cosmos DB database to connect to (default is "library")
    ct (str): the name of the Cosmos DB container to connect to (default is "editor_rules_dev")
    wrt2file (int): whether to write the result to an Excel file (1) or not (0) (default is 0)

    Returns:
    A dictionary containing document statistics, where each key is a rule ID and the corresponding value is a dictionary containing the following keys:
        - cnt: the number of documents with this rule ID
        - ids: a list of document IDs for documents with this rule ID
    """
    v_prg = __name__
    # 1.0 check parameters

    if qry is None:
        qry = "SELECT * FROM c"
    query_options = {'enable_cross_partition_query': True}

    # . 2.0 get DB configuration
    v_stp = 2.0
    v_msg = "Get DB configuration..."
    echo_msg(v_prg, v_stp, v_msg, 2)
    cfg = get_db_cfg(db_name=db, ct_name=ct)
    ctc = cfg["ct_conn"]

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
    df = df_log 
    v_stp = 4.1
    v_msg = "Processing each doc..."
    echo_msg(v_prg, v_stp, v_msg, 2)
    r_ids = {}                  # contain a list of rule ids 
    r_key_with_space = {}
    r_cnt = {}
    rows = []
    for i in doc_list:
        df_row = {"rule_id": None, "core_id": None,  "user_id": None, "guid_id": None,
              "created": None, "changed": None, "rule_status": None, "version": None,
              "doc_cnt":None, "dup_ids": None}

        doc_id = i.get("id")
        core_id = i.get("json", {}).get("Core", {}).get("Id")
        core_status = i.get("json", {}).get("Core", {}).get("Status")
        # get rule id 
        try: 
            r_auth = i.get("json", {}).get("Authorities")
            r_ref = r_auth[0].get("Standards")[0].get("References")
            r_id = r_ref[0].get("Rule_Identifier", {}).get("Id")    # rule id 
        except Exception as e:
            print(f"Error: {e}\n . DocID: {doc_id},  CoreID: {core_id}")
            r_id = None 
        if r_id is None:
            r_id = r_ref[0].get("Rule Identifier", {}).get("Id")    # wrong way to have rule id 
            if r_id is not None:
                if r_id not in r_key_with_space.keys():
                    r_key_with_space[r_id] = []
                r_key_with_space[r_id].append(doc_id)
        if r_id is None:                 # if rule id is still None,
            r_id = "NoRuleID"            # we assigned "NoRuleID" to it
        if r_id not in r_ids.keys():
            r_ids[r_id] = {"cnt":0, "ids": []}
        r_ids[r_id]["cnt"] += 1
        r_ids[r_id]["ids"].append(doc_id)
        df_row.update({"rule_id": r_id})
        # get a list of IG versions 
        v_vs = []
        v_vers = None
        if r_auth is not None: 
            for authority in r_auth:
                for standard in authority['Standards']:
                    v_vs.append(standard['Version'])
        v_vers = ", ".join(v_vs)
        df_row.update({"version": v_vers})

        df_row.update({"core_id": core_id})
        df_row.update({"user_id": i.get("creator", {}).get("id")})
        df_row.update({"guid_id": doc_id})
        df_row.update({"created": i.get("created")})
        df_row.update({"changed": i.get("changed")})
        df_row.update({"status": core_status})

        rows.append(df_row)

    # End of for i in doc_list

    v_stp = 4.2
    v_msg = "Get doc cnt and dup doc ids..."
    echo_msg(v_prg, v_stp, v_msg, 2)


    df_log = pd.DataFrame.from_records(rows)
    grouped_data = df_log.groupby("rule_id")

    for rule_id, grp in grouped_data:
        if grp.shape[0] > 2:  
            print(f"RuleID: {rule_id}:\n{grp}")

    v_stp = 4.3 
    if wrt2file == 1: 
        v_stp = 4.31
        load_dotenv()
        log_dir = os.getenv("log_dir")
        tm = dt.datetime.now()
        job_id = tm.strftime("%Y%m%d_%H%M%S")
        rst_fn = log_dir + "/stat-" + job_id + ".xlsx"
        v_msg = "Output result to " + rst_fn + "..." 
        echo_msg(v_prg, v_stp, v_msg,2)
        df_log.to_excel(rst_fn, index=False)
    else:
        v_stp = 4.32

    n = len(r_ids)
    for i in r_ids:
        m = len(r_ids[i]["ids"])
        if m > 1:
            print(f"{i}({m}/{n}): {r_ids[i]['ids']}")
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
