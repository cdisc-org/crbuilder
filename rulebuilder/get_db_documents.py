# Purpose: Create a Container in a Cosmos DB
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/31/2023 (htu) - initial coding based on
#     https://learn.microsoft.com/en-us/python/api/overview/azure/cosmos-readme?view=azure-python#create-a-container
#   04/04/2023 (htu) - converted code into get_db_documents 
#
import os 
from azure.cosmos import exceptions, PartitionKey
from rulebuilder.get_db_cfg import get_db_cfg
from rulebuilder.echo_msg import echo_msg
from azure.cosmos.exceptions import CosmosResourceNotFoundError


def get_db_documents (qry:str = None, db:str = 'library', ct:str = 'editor_rules_dev', act:str="V"):
    v_prg = __name__
    # 1.0 check parameters

    if qry is None:
        qry = "SELECT * FROM c"
    query_options = {'enable_cross_partition_query': True}


    #. 2.0 get DB configuration
    v_stp = 2.0 
    v_msg = "Get DB configuration..."
    echo_msg(v_prg, v_stp, v_msg, 2)
    cfg = get_db_cfg(db_name=db, ct_name=ct)
    ctc = cfg["ct_conn"]

    # 3.0 get total number of documents in the container
    qry1 = 'SELECT VALUE COUNT(1) FROM c'
    try:
        v_stp = 3.1
        t_docs = list(ctc.query_items(query=qry1,
            enable_cross_partition_query=True
        ))[0]

        v_msg = f"Total number of documents in container '{ct}@{db}': {t_docs}"
        echo_msg(v_prg, v_stp, v_msg, 2)
    except CosmosResourceNotFoundError:
        v_stp = 3.2
        v_msg = f"Could not run query - {qry1}"
        echo_msg(v_prg, v_stp, v_msg, 0)
        return 

    # 4.0 Execute the query
    doc_list = list()
    try:  
        v_stp = 4.1
        doc_list = list(
            ctc.query_items( query=qry,
                enable_cross_partition_query=True
                # ,partition_key=PartitionKey('id')
        ))
    except CosmosResourceNotFoundError:
        v_stp = 4.2
        v_msg = f"Could not run query - {qry}"
        echo_msg(v_prg, v_stp, v_msg, 0)
        return

    # process the results
    v_stp = 4.3
    r_ids = {} 
    r_key_with_space = {}
    for i in doc_list:
        doc_id = i.get("id")
        core_id = i.get("json", {}).get("Core", {}).get("Id")
        try: 
            r_auth = i.get("json", {}).get("Authorities")
            r_ref = r_auth[0].get("Standards")[0].get("References")
            rule_id = r_ref[0].get("Rule_Identifier", {}).get("Id")
            if rule_id is None:
               rule_id = r_ref[0].get("Rule Identifier", {}).get("Id")
               if rule_id is not None:
                    r_key_with_space[doc_id] = rule_id     
        except Exception as e:
            v_msg = f"Error: {e}\n . No rule_id for Doc - {core_id}:{doc_id}"
            echo_msg(v_prg, v_stp, v_msg, 0)
        if rule_id is None:
            rule_id = "NoRuleID" 
        
        if act == "V":      # View
            v_stp = 4.4
            # run the query
            print(f"DocID={doc_id} CoreID={core_id} RuleID={rule_id}")
        if act == "D":      # Delete
            v_stp = 4.5
            try: 
                v_stp = 4.51
                # Delete the existing document
                ctc.delete_item(item=i, partition_key=doc_id)
                v_msg = f" . Document with id {doc_id} deleted."
                echo_msg(v_prg, v_stp, v_msg, 2)
            except CosmosResourceNotFoundError:
                v_stp = 4.52
                v_msg = f"Could not delete doc - {doc_id}"
                echo_msg(v_prg, v_stp, v_msg, 2)
                break 
        if act == "DD":         # find duplicated
            if rule_id not in r_ids.keys():
                r_ids[rule_id] = []
            r_ids[rule_id].append(doc_id)

    if act == "DD": 
        n = len(r_ids)
        for i in r_ids:
            m = len(r_ids[i])
            if m > 1:
                print(f"{i}({m}/{n}): {r_ids[i]}")
        n2 = len(r_key_with_space)
        print(f"Docs with underscore keys ({n2}):\n{r_key_with_space}")


    # End of for i in doc_list


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
    get_db_documents(qry=q2, act="DD")

