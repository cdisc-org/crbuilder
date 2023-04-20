# Purpose: Create a Container in a Cosmos DB
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   04/18/2023 (htu) - initial coding based on get_doc_stats
#   04/19/2023 (htu) - finished coding and testing 

import os 
import sys
import json 
from rulebuilder.echo_msg import echo_msg
# from rulebuilder.read_db_rule import read_db_rule

def get_rule_ids (r_data = None):
    v_prg = __name__
    v_stp = 1.0
    v_msg = "Getting rule ids in a doc..."
    echo_msg(v_prg, v_stp, v_msg, 2)

    df_row = {"rule_id": None, "core_id": None, "combined_id": None, 
              "user_id": None, "guid_id": None,
              "created": None, "changed": None, "rule_status": None, 
              "version": None, 
              "rule_cnt": None, "rule_ids": None, 
              "doc_cnt": None, "dup_ids": None}

    def cvt_list2str (v_list): 
        # print(f"V_LIST 1: {v_list}")
        v_str = None
        if len(v_list) > 0:
            # remove None elements and convert int to str 
            v_list = [str(elem) for elem in v_list if elem is not None]
            v_list = list(set(v_list))          # get unique elements 
            v_list.sort()                       # sort elements 
            # print(f"V_LIST 2: {v_list}")
            if v_list is not None:
                v_str = ",".join(v_list)
        return v_str 

    if r_data is None:
        v_stp = 1.1
        v_msg = "Missing Rule Data."
        echo_msg(v_prg, v_stp, v_msg, 2)
        return df_row 

    v_stp = 1.2
    doc_id = r_data.get("id")
    core_id = r_data.get("json", {}).get("Core", {}).get("Id")
    if core_id is None or len(core_id) == 0:
        echo_msg(v_prg, v_stp, r_data, 9)
    v_msg = f"Trying to get Rule ID from [{core_id}] {doc_id}..."
    echo_msg(v_prg, v_stp, v_msg, 8)

    core_status = r_data.get("json", {}).get("Core", {}).get("Status")
    c_id = None                 # last component from Core.Id
    if core_id is not None:
        # Core.Id: FDA.SDTMIG.CT2001
        c_id = core_id.split(".")[-1]
    
    # 2.0 process json data and get rule id 
    v_stp = 2.0
    r_auth = r_data.get("json", {}).get("Authorities")
    if r_auth is None:
        v_msg = f"No authorities is defined in {doc_id}."
        echo_msg(v_prg, v_stp, v_msg, 1)
        return df_row
    
    v_count = 0
    v_ids = []                              # Rule IDs within a doc
    v_cid = []                              # Unique combined IDs with a doc
    v_vs = []                               # List of versions
    rule_id = None                          # the main rule id 
    for r_org in r_auth:                    # Loop through each Organizations
        org_name = r_org.get("Organization")
        r_standards = r_org.get("Standards")
        for r_std in r_standards:               # Loop through each Standards
            std_name = r_std.get("Name")
            std_version = str(r_std.get("Version"))
            v_vs.append(std_version)
            r_refs = r_std.get("References")
            r_id = None 
            for r_ref in r_refs:                # Loop through each References 
                r_id = r_ref.get("Rule Identifier",{}).get("Id")
                if r_id is None:
                    r_id = r_ref.get("Rule_Identifier",{}).get("Id")
                if r_id is None:
                    r_id = c_id
                if r_id is None:
                    r_id = "NoRuleID"
                v1_id = f"{org_name}:{std_name}:{std_version}:{r_id}"
                v_cid.append(v1_id)
                if r_id is not None and r_id not in v_ids: 
                    v_count += 1
                    v_ids.append(r_id) 
            # End of for r_ref in r_refs
        # End of for r_std in r_standards
    # End of r_org in r_auth
    # determine rule_id
    for a_id in v_ids:
        rule_id = a_id
        if c_id is not None and a_id == c_id:
            rule_id = c_id
            break
    v_stp = 2.2 
    v_msg = f" . Found Rule ID: {rule_id}."
    echo_msg(v_prg, v_stp, v_msg, 1)

    df_row.update({"rule_id": str(rule_id)})
    df_row.update({"core_id": core_id})
    df_row.update({"user_id": r_data.get("creator", {}).get("id")})
    df_row.update({"guid_id": doc_id})
    df_row.update({"created": r_data.get("created")})
    df_row.update({"changed": r_data.get("changed")})
    df_row.update({"rule_status": core_status})
    # df_row.update({"doc_cnt": None})
    # df_row.update({"dup_ids": None})
    df_row.update({"version": cvt_list2str(v_vs)})
    df_row.update({"combined_id": cvt_list2str(v_cid)})
    df_row.update({"rule_ids": cvt_list2str(v_ids)})
    df_row.update({"rule_cnt": v_count})

    return df_row 


# Test cases
if __name__ == "__main__":
    # set input parameters
    os.environ["g_msg_lvl"] = "3"
    v_prg = __name__ + "::get_rule_ids"
    db = "library"
    ct = "editor_rules_dev"
    rule_id = "CG0509"
    # r_json = read_db_rule(rule_id, db_name=db, ct_name=ct)
    doc_id = "8d5482f5-b941-49bd-8509-8408a3f9648f"
    # r_json = read_db_rule(doc_id=doc_id, db_name=db, ct_name=ct)


    row = get_rule_ids(r_data=r_json)
    json.dump(row, sys.stdout, indent=4)
    # print(f"ROW: {row}")
    
