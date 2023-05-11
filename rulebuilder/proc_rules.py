# Purpose: Process all the rules from a df_data 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   04/06/2023 (htu) - ported from proc_sdtm_rules as proc_rules module
#   04/07/2023 (htu) - added r_standard and db_cfg 
#   04/08/2023 (htu) - added log_cfg 
#   04/14/2023 (htu) - added s_pub to select publisher for FDA_VR1_6 at Step 2.5
#   04/17/2023 (htu) - added filter_df method to simplify the record filtering 
#   04/18/2023 (htu) - 
#     1. changed step 3.7 so it will output to dat_fdir 
#     2. added rename_keys at step 3.7 
#     3. added r_json in publish_a_rule 
#   04/19/2023 (htu) - added v_type to check and get "Rule Type" and Rule_Type
#  

import os
import re 
import pandas as pd
import datetime as dt 
from dotenv import load_dotenv
from rulebuilder.echo_msg import echo_msg
from rulebuilder.read_rules import read_rules
from rulebuilder.get_db_cfg import get_db_cfg
from rulebuilder.rename_keys import rename_keys
from rulebuilder.get_doc_stats import get_doc_stats
from rulebuilder.proc_each_yaml import proc_each_yaml
from rulebuilder.create_log_dir import create_log_dir
from rulebuilder.publish_a_rule import publish_a_rule
from rulebuilder.build_rule_yaml import build_rule_yaml
from rulebuilder.output_rule2file import output_rule2file
from rulebuilder.get_existing_rule import get_existing_rule
from rulebuilder.get_rule_constants import get_rule_constants

def proc_rules(r_standard,
               df_data=None, in_rule_folder:str=None, 
                out_rule_folder:str=None, 
                rule_ids: list = ["CG0001"],s_version: list = [], 
                s_class: list = [], s_domain: list = [],
                s_pub: list = [],
                wrt2log: int = 0, pub2db: int = 0,
                get_db_rule: int = 0,
                db_name:str=None, ct_name:str=None,
                db_cfg = None,
                log_cfg = None  
                ) -> None:
    """
    Process all rule definitions in `df_data`, and output a YAML and JSON
    file for each rule to `out_rule_folder`.

    Parameters:
    -----------
    r_standard: str, no default
        Available standards are: FDA_VR1_6, SDTM_V2_0, SEND_V4_0, ADAM_V4_0, etc.
    df_data: DataFrame, default None
        The rule definition data. If None, it is read in from the file specified
        by the `yaml_file` environment variable.
    in_rule_folder: str, default None
        The folder containing existing rules. If None, it is read from the
        `existing_rule_dir` environment variable.
    out_rule_folder: str, default None
        The folder where the new or updated rules will be output to. If None,
        it is read from the `output_dir` environment variable.
    rule_ids: list of str, default ['CG0001']
        A list of rule IDs that will be used to select rule definitions
        to be used to generate YAML and JSON files for the rules. If not
        provided or if it is an empty list, all the rule definitions will be
        used.
    s_version: list of str, default []
        A list of standard versions to filter the rules by. If an empty list,
        no filtering is performed.
    s_class: list of str, default []
        A list of rule classes to filter the rules by. If an empty list, no
        filtering is performed.
    s_domain: list of str, default []
        A list of domains to filter the rules by. If an empty list, no filtering
        is performed.
    wrt2log: int, default 0
        If 1, write logs to a file.
    pub2db: int, default 0
        If 1, publish the rule to a database specified by `db_name` and `ct_name`.
    get_db_rule: int, default 0
        If 1, get the rule from the database specified by `db_name` and `ct_name`.
    db_name: str, default None
        The name of the database to publish/get rules from.
    ct_name: str, default None
        The name of the container to publish/get rules from.

    Returns:
    --------
        None

    Raises:
    -------
    ValueError
        If `pub2db` is 1 and `db_name` or `ct_name` are not specified.
        If the file specified by `yaml_file` does not exist.
        If `in_rule_folder` or `out_rule_folder` do not exist.
    """
    v_prg = __name__ 
    v_stp = 1.0
    v_msg = f"Getting environment variables for {r_standard}..."
    echo_msg(v_prg, v_stp, v_msg,1)
    st_all = dt.datetime.now()
    load_dotenv()

    r_std = os.getenv("r_standard") if r_standard is None else r_standard
    r_std = r_std.upper()

    # now_utc = dt.datetime.now(dt.timezone.utc)
    now_utc = dt.datetime.now()
    w2log = os.getenv("write2log")
    # log_dir = os.getenv("log_dir")
    # job_id  = os.getenv("job_id")
    # s_dir = os.getenv("s_dir")
    w2log = 0 if w2log is None else int(w2log)
    if w2log > wrt2log:
        wrt2log = w2log 
    # 0. set up log first 
    if log_cfg is None: 
        log_cfg = create_log_dir(wrt2log=wrt2log)
    s_dir = log_cfg["s_dir"]
    sub_dir = log_cfg["sub_dir"]
    log_fdir = log_cfg["log_fdir"]
    job_id = log_cfg["job_id"]
    log_dir = log_cfg["log_dir"]
    if job_id is None:
        job_id = now_utc.strftime("J%H%M%S")
    rst_fn = f"{log_dir}/{sub_dir}/job-{job_id}-proc.xlsx"

    # 1. get inputs
    # -----------------------------------------------------------------------
    v_stp = 1.1
    v_msg = "Check input parameters..."
    echo_msg(v_prg, v_stp, v_msg,2)
    yaml_file = os.getenv("yaml_file")

    if df_data is None:
        v_stp = 1.11
        v_msg = "Get Rule Data from " + yaml_file
        echo_msg(v_prg, v_stp, v_msg, 2)
        if not os.path.isfile(yaml_file):
            v_stp = 1.12
            v_msg = "Could not find file - " + yaml_file
            return 
        df_data = read_rules(yaml_file)

    if in_rule_folder is None:
        v_stp = 1.13
        in_rule_folder = os.getenv("existing_rule_dir")

    if out_rule_folder is None:
        v_stp = 1.14
        out_rule_folder = os.getenv("output_dir")

    if get_db_rule == 0 and not os.path.exists(in_rule_folder):
        v_stp = 1.15
        v_msg = "Could not find input rule folder: " + in_rule_folder
        echo_msg(v_prg, v_stp, v_msg,2)
        return 

    if not os.path.exists(out_rule_folder):
        v_stp = 1.2
        v_msg = "Could not find output rule folder: " + out_rule_folder
        echo_msg(v_prg, v_stp, v_msg,2)
        v_msg = "Making dir - " + out_rule_folder
        echo_msg(v_prg, v_stp, v_msg, 2)
        os.makedirs(out_rule_folder)

    num_records_processed = df_data.shape[0]
    v_stp = 1.3
    if num_records_processed < 1:
        v_msg = "No record find in the rule definition data set."
        echo_msg(v_prg, v_stp, v_msg,0)
        return
    else:
        v_msg = "INFO:Total number of records: " + str(num_records_processed)
        echo_msg(v_prg, v_stp, v_msg,2)

    v_stp = 1.4 
    if pub2db > 0 or get_db_rule > 0:
        if db_name is None and ct_name is None and db_cfg is None:
            v_msg = "When publishing to a DB, database and container names are required."
            echo_msg(v_prg, v_stp, v_msg, 0)
            return
        if db_cfg is None:
            db_cfg = get_db_cfg(db_name=db_name,ct_name=ct_name)
        if 'ct_conn' not in db_cfg.keys(): 
            v_msg = "Could not create container connection for C/D: {ct_name}/{db_name}"
            echo_msg(v_prg, v_stp, v_msg, 0)
            return
        v_stp = 1.42
        r_ids = get_doc_stats(db=db_name,ct=ct_name,wrt2file=wrt2log,
                              job_id=job_id,db_cfg=db_cfg)
    else:
        r_ids = {}

    # 1.5     
    r_cst = get_rule_constants(r_std)

    # 2. Group data by Rule ID
    # -----------------------------------------------------------------------
    v_stp = 2.0 
    v_msg = f"Filtering and Grouping data..."
    echo_msg(v_prg, v_stp, v_msg,1)

    def filter_df(s_msg, s_key, s_lst, local_df): 
        num_sel = len(s_lst)
        if num_sel > 0: 
            local_df = local_df[local_df[s_key].isin(s_lst)]
        num_df = local_df.shape[0]
        v_s1 = ", ".join(s_lst)
        v_s2 = f"{num_df}/{num_records_processed}"
        v_msg = f"Select by {s_msg} IG ({num_sel}:{v_s1}): {v_s2} "
        echo_msg(v_prg, v_stp, v_msg,2)
        return local_df

    
    # 2.1 select by standard version
    v_stp = 2.1 
    num_selected = len(s_version)
    df = df_data
    if r_std == "SDTM_V2_0":
        df = filter_df(s_msg="IG Version", s_key="SDTMIG Version",
                       s_lst=s_version, local_df=df)
    #     df = df_data[df_data["SDTMIG Version"].isin(s_version)]
    # v_s1 = ", ".join(s_version)
    # v_s2 = f"{df.shape[0]}/{num_records_processed}"
    # v_msg = f"Select by IG Version ({num_selected}:{v_s1}): {v_s2} "
    # echo_msg(v_prg, v_stp, v_msg,2)

    # 2.2 select by class
    v_stp = 2.2
    if r_std == "SDTM_V2_0":
        df = filter_df(s_msg="Class", s_key="Class", s_lst=s_class, local_df=df)
    # num_selected = len(s_class)
    # if r_std == "SDTM_V2_0":
    #     df = df[df["Class"].isin(s_class)]
    # num_df = df.shape[0]
    # v_s1 = ", ".join(s_class)
    # v_s2 = f"{num_df}/{num_records_processed}"
    # v_msg = f"Select by IG Class ({num_selected}:{v_s1}): {v_s2} "
    # echo_msg(v_prg, v_stp, v_msg, 2)

    # 2.3 select by domain 
    v_stp = 2.3
    # df = filter_df(s_msg="Class", s_key="Class", s_lst=s_class, local_df=df)
    num_selected = len(s_domain)
    if r_std == "SDTM_V2_0":
        # df = df[df["Domain"].isin(s_domain)]
        df = filter_df(s_msg="SDTM Domains", s_key="Domain",
                       s_lst=s_domain, local_df=df)
    elif r_std == "FDA_VR1_6":
        # df = df[df["Domains"].isin(s_domain)]
        df = filter_df(s_msg="FDA Domains", s_key="Domains",
                       s_lst=s_domain, local_df=df)
    # num_df = df.shape[0]
    # v_s1 = ", ".join(s_domain)
    # v_s2 = f"{num_df}/{num_records_processed}"
    # v_msg = f"Select by IG Domain ({num_selected}:{v_s1}): {v_s2} "
    # echo_msg(v_prg, v_stp, v_msg, 2)

    # 2.4 select by rule ids
    v_stp = 2.4
    df = filter_df(s_msg="Rule ID", s_key="Rule ID",
                   s_lst=rule_ids, local_df=df)
    # num_selected = len(rule_ids)
    # df = df if num_selected == 0 else df[df["Rule ID"].isin(rule_ids)]
    # v_ids = ", ".join(rule_ids)
    # v_msg = f" . Select by Rule IDs ({v_ids}): {df.shape[0]}"
    # echo_msg(v_prg, v_stp, v_msg, 2)

    # 2.5 select by Publisher 
    v_stp = 2.5
    if r_std == "FDA_VR1_6":
        df = filter_df(s_msg='FDA Publisher', 
                       s_key='Publisher', s_lst=s_pub, local_df=df)
 
    v_msg  = "INFO:Selected number of rules: " + str(df.shape[0])
    v_msg += "/" + str(num_records_processed)
    echo_msg(v_prg, v_stp, v_msg,2)

    # 3. Loop through each rule 
    # -----------------------------------------------------------------------

    v_stp = 3.0
    v_msg = "Group the rules and looping through each rules..."
    echo_msg(v_prg, v_stp, v_msg,1)

    df_sorted = df.sort_values(by='Rule ID')
    grouped_data = df_sorted.groupby("Rule ID")

    # Loop through each Rule ID and print out required information

    df_log = pd.DataFrame(columns=["rule_id", "core_id",  "user_id", "guid_id", 
                                   "created", "changed", "rule_status", "version", 
                                   "class", "domain", "variable", "rule_type", 
                                   "document", "section", "sensitivity",
                                   "publish_status"])
    # capture input parameters
    ipt_msg = f"Input Parameters:\n . Job ID: {job_id}\n"
    ipt_msg += f" . R_Standard: {r_std}\n"
    ipt_msg += f" . Rule Data: {num_records_processed}\n"
    ipt_msg += f" . Existing Rule Folder: {in_rule_folder}\n"
    ipt_msg += f" . Output Folder: {out_rule_folder} S_Dir: {s_dir}\n" 
    ipt_msg += f" . Output Sub Dir: {sub_dir}\n" 
    ipt_msg += f" . Selection - Rule IDs: {','.join(rule_ids)}\n"
    ipt_msg += f" . Selection - Versions: {','.join(s_version)}\n"
    ipt_msg += f" . Selection - Classes: {','.join(s_class)}\n"
    ipt_msg += f" . Selection - Domains: {','.join(s_domain)}\n"
    ipt_msg += f" . Write2Log ({wrt2log}): {log_dir}\n"
    ipt_msg += f" . Publish to DB ({pub2db}): {db_name}.{ct_name}\n"
    ipt_msg += f" . Get DB Rule ({get_db_rule}): {db_name}.{ct_name}"
 
    rows = []
    num_grps = grouped_data.ngroups
    i_grp = 0
    v_stp = 3.1 
    # v_re = r'([^\d]+)(\d.*)'
    for rule_id, group in grouped_data:
        i_grp += 1 
        st_row = dt.datetime.now()
        log_fn = f"{log_fdir}/{rule_id}-{job_id}.txt" 
        os.environ["log_fn"] = log_fn
        v_msg = f"  {i_grp}/{num_grps}: Rule ID - {rule_id}..."
        echo_msg(v_prg, v_stp, v_msg, 2, i=i_grp, n=num_grps)
        echo_msg(v_prg, v_stp, ipt_msg,3 )

        # if rule_id not in rule_ids: continue 
        num_records = group.shape[0]
        row = {"rule_id": None, "core_id": None,  "user_id": None,"guid_id":None, 
               "created": None, "changed": None, "rule_status": None,"version":None,
               "class":None, "domain": None,"variable":None, "rule_type": None,
               "document": None, "section": None, "sensitivity": None, 
               "publish_status": None}
        row.update({"rule_id":rule_id})

        # 3.1 show rule id and number of records in the group
        v_stp = 3.1
        v_msg = "  Rule ID: (" + rule_id + ") with " + str(num_records) + " records."
        echo_msg(v_prg, v_stp, v_msg,2)
        
        # 3.2 display the group if message level is 5 or over
        v_stp = 3.2
        v_msg = "INFO: Group {" + str(group) + "}"
        echo_msg(v_prg, v_stp, v_msg, 5)
        # print(f"Group: {group}")

        # 3.3 select the records for the group 
        v_stp = 3.3
        v_msg = "Select the records for rule id = " + rule_id 
        echo_msg(v_prg, v_stp, v_msg, 3)
        a_json = {} 
        rule_data = None 
        # rule_data = df_selected[df_data["Rule ID"] == rule_id]
        rule_data = df[df["Rule ID"] == rule_id]
        rule_data = rule_data.reset_index(drop=True)
        d_cnt = 0 
        doc_ids = r_ids.get(rule_id, {}).get("ids")
        d_len = 0 if doc_ids is None else len(doc_ids)

        def proc_each_rule ():
            # 3.4 read in the existing rule
            v_stp = 3.4
            v_msg = f"  . {d_cnt}/{d_len} getting document - {doc_id} for {rule_id}..."
            if d_len > 1: 
                print(v_msg)
            echo_msg(v_prg, v_stp, v_msg, 2)
            rule_obj = get_existing_rule(rule_id, in_rule_folder,
                                         doc_id=doc_id,
                                         get_db_rule=get_db_rule, r_ids=r_ids,
                                         db_name=db_name, ct_name=ct_name,
                                         db_cfg=db_cfg
                                #        ,use_yaml_content=False
                                         )
            echo_msg(v_prg, v_stp, rule_obj, 9)
            # json.dump(rule_obj, sys.stdout, indent=4)

            # 3.5 process the rule
            #
            # a_json = proc_each_sdtm_rule(
            #     rule_data, rule_obj, rule_id, in_rule_folder, cnt_published)
            v_stp = 3.5
            a_json = proc_each_yaml(rule_id, rule_data, rule_obj=rule_obj,
                                    rule_dir=in_rule_folder, r_ids=r_ids,
                                    get_db_rule=get_db_rule, db_name=db_name, ct_name=ct_name)
            a = a_json
            r_status = a.get("status")
            if r_status is None or r_status != "new":
                r_status = a.get("json", {}).get("Core", {}).get("Status")
            v_type = a.get("json", {}).get("Rule Type")
            if v_type is None:
                v_type = a.get("json", {}).get("Rule_Type")
            row.update(
                {"core_id": a.get("json", {}).get("Core", {}).get("Id")})
            row.update({"user_id": a.get("creator", {}).get("id")})
            row.update({"guid_id": a.get("id")})
            row.update({"created": a.get("created")})
            row.update({"changed": a.get("changed")})
            row.update({"rule_status": r_status})
            row.update({"rule_type": v_type})
            row.update({"sensitivity": a.get(
                "json", {}).get("Sensitivity")})

            v_r_v1 = None
            v_r_v2 = None
            v_r_d1 = None
            v_r_s1 = None
            v_c = None
            v_d = None

            if r_std in ("SDTM_V2_0"):
                v_r_v1 = rule_data["SDTMIG Version"].str.cat(sep="; ")
                v_classes = list(
                    set([c for classes in rule_data['Class'] for c in classes]))
                v_c = ", ".join(v_classes)
                v_doms = list(
                    set([d for doms in rule_data['Domain'] for d in doms]))
                v_d = ", ".join(v_doms)
                v_r_v2 = rule_data["Variable"].str.cat(sep="; ")
                v_r_d1 = rule_data["Document"].str.cat(sep="; ")
                v_r_s1 = rule_data["Section"].str.cat(sep="; ")
            elif r_std in ("FDA_VR1_6"):
                r_cst = get_rule_constants(r_std)
                v_vs = r_cst.get("VS")
                # print(f"V_Vs ------------- : {v_vs}")
                v_r_v1 = None
                for j in v_vs:
                    v_ig_version = re.sub(r'([G|R|T])(\d)', r'\1 \2', j)
                    if v_r_v1 is None:
                        v_r_v1 = v_ig_version
                    else:
                        if v_ig_version is not None:
                            v_r_v1 += "," + v_ig_version
                v_c = a.get("json", {}).get("Scope", {}).get(
                    "Classes", {}).get("Include")
                v_d = a.get("json", {}).get("Scope", {}).get(
                    "Domains", {}).get("Include")

            # end of if r_std in ("SDTM_V2_0")
            row.update({"version": v_r_v1})
            row.update({"class": v_c})
            row.update({"domain": v_d})
            row.update({"variable": v_r_v2})
            row.update({"document": v_r_d1})
            row.update({"section": v_r_s1})
            # Append row to list of rows

            # 3.6 build yaml content
            a_json["content"] = None
            # # Only get json for YAML
            # dict_yaml = a_json["json"]
            # print(f"Dict Keys: {dict_yaml.keys()}")
            # # Replace "_" with " " for columns
            # d_yaml = rename_keys(dict_yaml, '_', ' ')
            # a_yaml = yaml.dump(d_yaml, default_flow_style=False)
            a_yaml = build_rule_yaml(rule_data, a_json)

            # 3.7 output the rule to json and yaml files
            # output_rule2file(rule_id, a_json, a_yaml, out_rule_folder)
            rename_keys(a_json, ' ', '_')
            a_json["content"] = a_yaml
            output_rule2file(rule_id, a_json, a_yaml)

            # 3.8 publish the rule
            if pub2db == 1:
                a_row = publish_a_rule(r_json=a_json, 
                    rule_id=rule_id, db_cfg=db_cfg, r_ids=r_ids)
                row.update({"publish_status": a_row["publish_status"]})
            else:
                row.update({"publish_status": "Not published"})

            # 3.9 Append the status record to rows
            rows.append(row)
            et_row = dt.datetime.now()
            st = st_row.strftime("%Y-%m-%d %H:%M:%S")
            et = et_row.strftime("%Y-%m-%d %H:%M:%S")
            v_msg = f"The job {job_id} for {rule_id} was done between: {st} and {et}"
            echo_msg(v_prg, v_stp, v_msg, 2)
        # End of def proc_each_rule 
        # -------------------- proc_each_rule -------------------------------
        doc_id = None
        if d_len == 0:
            proc_each_rule()
        else:  
            for doc_id in doc_ids: 
                d_cnt += 1 
                proc_each_rule()
            # End of for doc_id in doc_ids

    # End of for rule_id, group in grouped_data

    # Collect basic stats and print them out
    v_stp = 4.0 
    v_msg = "Get statistics..."
    n_uniq = grouped_data.ngroups
    v_msg = f"Number of Records Processed: {n_uniq}/{num_records_processed}"
    echo_msg(v_prg, v_stp, v_msg,1)

    v_stp = 4.1
    v_msg = "Output result to " + rst_fn + "..." 
    echo_msg(v_prg, v_stp, v_msg,2)
    df_log = pd.DataFrame.from_records(rows)
    df_log.to_excel(rst_fn, index=False)

    v_stp = 5.0 
    et_all = dt.datetime.now()
    st = st_all.strftime("%Y-%m-%d %H:%M:%S")
    et = et_all.strftime("%Y-%m-%d %H:%M:%S")
    v_msg = f"The job {job_id} was done between: {st} and {et}"
    echo_msg(v_prg, v_stp, v_msg,1)


# Test cases
if __name__ == "__main__":
    # set input parameters 
    os.environ["g_lvl"] = "5"
    v_prg = __name__ + "::proc_rules"
    # rule_list = ["CG0373", "CG0378", "CG0379"]
    rule_list = ["CG0001"]
    # rule_list = []

    # 1. Test with basic parameters
    v_stp = 1.0 
    echo_msg(v_prg, v_stp, "Test Case 01: Basic Parameter",1)

    # proc_rules(rule_ids=["CG0006"], wrt2log=True)
    # proc_rules(rule_ids=["CG0006"], wrt2log=True)
    
    # Expected output:
    # 2. Test publishing rules 
    v_stp = 2.0 
    echo_msg(v_prg, v_stp, "Test Case 02: Publish Rules",1)
    db = 'library'
    ct = 'core_rules_dev'
    # proc_rules(rule_ids=["CG0063"], wrt2log=True,pub2db=1,db_name=db,ct_name=ct)
    # proc_rules(rule_ids=[], wrt2log=True,pub2db=1,db_name=db,ct_name=ct)

    # Publish to dev environment t
    ct = 'editor_rules_dev'
    # proc_rules(rule_ids=["CG0001","CG0015", "CG0063"], wrt2log=True, pub2db=1, 
    #                 get_db_rule=1, db_name=db, ct_name=ct)

    # proc_rules(rule_ids=["CG0002","CG0027","CG0015", "CG0063"], wrt2log=True,
    #                pub2db=1, db_name=db, ct_name=ct)
    # proc_rules(rule_ids=[], wrt2log=True, pub2db=1,
    #                get_db_rule=1, db_name=db, ct_name=ct)


# End of File

