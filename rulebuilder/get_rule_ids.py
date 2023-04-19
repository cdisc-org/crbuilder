# Purpose: Create a Container in a Cosmos DB
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   04/18/2023 (htu) - initial coding based on get_doc_stats

from rulebuilder.echo_msg import echo_msg

def get_rule_ids (rule_id = None, r_data = None):
    v_prg = __name__
    v_stp = 1.0
    v_msg = "Getting rule ids in a doc..."
    echo_msg(v_prg, v_stp, v_msg, 2)

    if rule_id is None or r_data is None:
        v_stp = 1.1
        v_msg = "Missing Rule ID or Rule Data."
        echo_msg(v_prg, v_stp, v_msg, 2)
        return None

    doc_id = r_data.get("id")
    core_id = r_data.get("json", {}).get("Core", {}).get("Id")
    core_status = r_data.get("json", {}).get("Core", {}).get("Status")
    c_id = None                 # last component from Core.Id
    if core_id is not None:
        c_id = core_id.split(".")[-1]
    r_auth = r_data.get("json", {}).get("Authorities")

    v_auth = None
    v_ref = None
    r_id = None
    try:
        v_stp = 4.11
        v_msg = f" {doc_cnt}/{tot_docs} Trying to get Rule ID from [{core_id}] {doc_id}..."
        echo_msg(v_prg, v_stp, v_msg, 8)
        if core_id is None or len(core_id) == 0:
            echo_msg(v_prg, v_stp, i, 9)
        r_auth = i.get("json", {}).get("Authorities")
        r_ref = r_auth[0].get("Standards")[0].get("References")
        r_id = r_ref[0].get("Rule Identifier", {}).get("Id")    # rule id
        if r_id is None:
            r_id = r_ref[0].get("Rule_Identifier", {}).get(
                "Id")    # rule id
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
        r_id = r_ref[0].get("Rule Identifier", {}).get(
            "Id")    # wrong way to have rule id
        if r_id is None:
            r_id = r_ref[0].get("Rule_Identifier", {}).get(
                "Id")    # wrong way to have rule id
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
        r_ids[r_id] = {"cnt": 0, "ids": [], "status": []}
    if r_id != c_id and c_id is not None and r_id is not None:
        if c_id not in r_ids.keys():
            r_ids[c_id] = {"cnt": 0, "ids": [], "status": []}
        r_ids[c_id]["cnt"] += 1
        r_ids[c_id]["ids"].append(doc_id)
        df_row.update({"rule_id": c_id})
    r_ids[r_id]["cnt"] += 1
    r_ids[r_id]["ids"].append(doc_id)
    r_ids[r_id]["status"].append(core_status)
    df_row.update({"rule_id": r_id})
    # get a list of IG versions
