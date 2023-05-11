# Purpose: Combine CDISC and FDA Rules
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   05/07/2023 (htu) - initial coding based on combine_rules
#   05/09/2023 (htu) - added sdtm2html and updated fda_rules2html
#   05/10/2023 (htu) - write out HTML code for each FDA rule

import re 
import json
import pandas as pd
import yaml
from rulebuilder.write_df2html import write_df2html
from rulebuilder.pickle_files import read_pick
from rulebuilder.get_rule_data import (get_fda_32, get_fda_33, 
    get_fda_sdtm, get_current_rule_defs)

def sdtm2html(i_dir:str="./data/source",o_dir:str="./data/output"):
    # 1. read FDA rule definition
    fn_sdtm = "fda_vr1_6.pick"
    # fn_sdtm = "sdtm_v2_0.pick"
    fp_sdtm = f"{i_dir}/pick/{fn_sdtm}"
    fp_html = f"{o_dir}/html/fda_vr1_6_idx.html"
    sud_dir = "sdtm"
    html_sud_dir=f"{o_dir}/html/{sud_dir}"
    df_fda_sdtm = read_pick(fp_sdtm)
    df1 = df_fda_sdtm               # FDA rule definition 
    df1_keys = df1.columns

    # 2. read SDTM rule definition 
    fn_sd2 = "sdtm_v2_0.pick"
    fp_sd2 = f"{i_dir}/pick/{fn_sd2}"
    df2 = read_pick(fp_sd2)
    df2_keys = df2.columns
    df2_grouped = df2.groupby("Rule ID")
    
    # 3. read current rule developed
    df_rule = get_current_rule_defs()
    df3 = df_rule.rename(columns={'CDISC Rule ID': 'Rule ID'})
    df3_keys = df3.columns 
 
    # 4. read p21 rule definition
    df4_32 = get_fda_32()
    df4_33 = get_fda_33()
    df4    = get_fda_sdtm()
    df4_keys = df4.columns

    # 5. create HTML table 
    df_tbl_cols = ["Publisher","Standard","Rule ID","Version","Definitioin"]
    r_tbl_cols = ["Rule ID","Key","Value"]
       
    j_cnt = 0
    j_ver = "SDTMIG Version"
    k_pub = "Publisher ID"
    k_id = "Rule ID"
    tot_j = df2_grouped.ngroups
    for r_id, group in df2_grouped:
        fn_rule = f"{html_sud_dir}/{r_id}.html"
        num_records = group.shape[0]
        j_cnt += 1
        print(f"{j_cnt}/{tot_j}: - {r_id}...")
        # 5.1 get SDTMIG 
        r_tbl = pd.DataFrame(columns=r_tbl_cols) 
        r_tbl_rows = []
        r_cell = ""
        df_tbl = pd.DataFrame(columns=df_tbl_cols)
        df_tbl_row = {"Publisher":None,"Standard":"SDTMIG","Rule ID":None,
                      "Version":None,"Definition":None}
        df_tbl_rows = []
        r2_def = ""
        for row_i, row in group.iterrows():
            i_cnt = row_i + 1
            r_ver = row.get(j_ver)
            fn_rver = f"{html_sud_dir}/{r_id}-{r_ver}.html"
            r_tbl_ver = pd.DataFrame(columns=r_tbl_cols)
            r_tbl_ver_rows = []
            r2_def = f"{r2_def}<table id='{r_id}-sdtmig-{r_ver}'>"
            print(f"  {i_cnt}/{num_records}: - {r_ver}...")
            for k2 in df2_keys:
                v2 = str(row.get(k2))
                if bool(v2 and v2.strip()) and v2 not in ("nan","X"): 
                    r_tbl_ver_row = {"Rule ID":r_id,"Key":k2, "Value":v2}
                    r_cell = f"{k2}:{v2}" if r_cell == "" else f"{r_cell}<br>{k2}:{v2}"
                    r2_def = f"{r2_def}<tr><td>{k2}</td><td>{v2}</td></tr>"
                    r_tbl_ver_rows.append(r_tbl_ver_row)
            r2_def = f"{r2_def}</table>"
            r_cell = f"<code>{r_cell}</code>"
            r_tbl_ver = pd.DataFrame.from_records(r_tbl_ver_rows)
            write_df2html(r_tbl_ver,fn_rver,f"Rule ID {r_id} Versioin {r_ver}")
            df_tbl_row.update({"Publisher":"CDISC","Standard":"SDTMIG"})
            df_tbl_row.update({"Rule ID":r_id,"Version":r_ver,"Definition":r2_def})
        # End of for row_i, row in group.iterrows()
        df_tbl_rows.append(df_tbl_row)

        # 5.2 Get FDA CR Deinition
        df_tbl_row = {"Publisher":None,"Standard":"SDTMIG","Rule ID":None,
                      "Version":None,"Definition":None}
        r1_def = ""
        for i, r1 in df1.iterrows():
            f_id = r1.get("FDA Validator Rule ID")
            f_pub = r1.get("Publisher")
            p_ids = r1.get(k_pub)
            r_c1 = ""
            r1_def = f"{r1_def}<table id='{r_id}-fdacr-{i}'>"
            if r_id in p_ids:
                for k1 in df1_keys:
                    v1 = str(r1.get(k1))
                    if bool(v1 and v1.strip()) and v1 not in ("nan","X"): 
                        r_c1 = f"{k1}:{v1}" if r_c1 == "" else f"{r_c1}<br>{k1}:{v1}"
                        r1_def = f"{r1_def}<tr><td>{k1}</td><td>{v1}</td></tr>"
                # End of for k1 in df1_keys
                break
            r1_def = f"{r1_def}</table>"
            # End of r_id in p_ids
        # End of for i, r1 in df_tbl.iterrows()
        r_c1 = f"<code>{r_c1}</code>"
        df_tbl_row.update({"Publisher":"FDA","Standard":"FDA CR"})
        df_tbl_row.update({"Rule ID":r_id, "Version":"1.6"})
        df_tbl_row.update({"Definition":r1_def})
        df_tbl_rows.append(df_tbl_row)

        # 5.3 Get Rule Development
        df_tbl_row = {"Publisher":None,"Standard":"SDTMIG","Rule ID":None,
                      "Version":None,"Definition":None}
        r3_def = ""
        for i, r3 in df3.iterrows():
            r3_id = r3.get("Rule ID")
            r3_pub = r3.get("Organization")
            r3_std = r3.get("Standard Name")
            r3_ver = r3.get("Standard Version")
            r3_cell = ""
            if r_id == r3_id:
                r3_def = f"{r3_def}<table id='{r_id}-cdisc-{i}'>"
                for k3 in df3_keys:
                    v3 = str(r3.get(k3))
                    if bool(v3 and v3.strip()) and v3 not in ("nan","X"): 
                        r3_cell = f"{k3}:{v3}" if r3_cell == "" else f"{r3_cell}<br>{k3}:{v3}"
                        r3_def = f"{r3_def}<tr><td>{k3}</td><td>{v3}</td></tr>"
                # End of for k1 in df1_keys
                break
            # End of r_id in p_ids
        # End of for i, r1 in df_tbl.iterrows()
        r3_cell = f"<code>{r3_cell}</code>"
        r3_def = f"{r3_def}</table>"
        df_tbl_row.update({"Publisher":r3_pub,"Standard":r3_std})
        df_tbl_row.update({"Rule ID":r_id})
        df_tbl_row.update({"Version":r_ver,"Definition":r3_def})
        df_tbl_rows.append(df_tbl_row)

        # 5.4a Get P21 Rule Definition
        df_tbl_row = {"Publisher":None,"Standard":"SDTMIG","Rule ID":None,
                      "Version":None,"Definition":None}
        r4_def = ""
        for i, r4 in df4_32.iterrows():
            r4_ids = str(r4.get("PublisherID"))
            r4_cell = ""
            if r_id in r4_ids:
                r4_def = f"{r4_def}<table id='{r_id}-p21.32-{i}'>"
                for k4 in df4_keys:
                    v4 = str(r4.get(k4))
                    if bool(v4 and v4.strip()) and v4 not in ("nan","X"): 
                        r4_cell = f"{k4}:{v4}" if r4_cell == "" else f"{r4_cell}<br>{k4}:{v4}"
                        r4_def = f"{r4_def}<tr><td>{k4}</td><td>{v4}</td></tr>"
                # End of for k1 in df1_keys
                break
            # End of r_id in p_ids
        # End of for i, r1 in df_tbl.iterrows()
        r4_cell = f"<code>{r4_cell}</code>"
        r4_def = f"{r4_def}</table>"
        df_tbl_row.update({"Publisher":"FDA P21","Standard":"SDTM"})
        df_tbl_row.update({"Rule ID":r_id})
        df_tbl_row.update({"Version":"3.2","Definition":r4_def})
        df_tbl_rows.append(df_tbl_row)

        # 5.4b Get P21 Rule Definition
        df_tbl_row = {"Publisher":None,"Standard":"SDTMIG","Rule ID":None,
                      "Version":None,"Definition":None}
        r4_def = ""
        for i, r4 in df4_33.iterrows():
            r4_ids = str(r4.get("PublisherID"))
            r4_cell = ""
            if r_id in r4_ids:
                r4_def = f"{r4_def}<table id='{r_id}-p21.33-{i}'>"
                for k4 in df4_keys:
                    v4 = str(r4.get(k4))
                    if bool(v4 and v4.strip()) and v4 not in ("nan","X"): 
                        r4_cell = f"{k4}:{v4}" if r4_cell == "" else f"{r4_cell}<br>{k4}:{v4}"
                        r4_def = f"{r4_def}<tr><td>{k4}</td><td>{v4}</td></tr>"
                # End of for k1 in df1_keys
                break
            # End of r_id in p_ids
        # End of for i, r1 in df_tbl.iterrows()
        r4_cell = f"<code>{r4_cell}</code>"
        r4_def = f"{r4_def}</table>"
        df_tbl_row.update({"Publisher":"FDA P21","Standard":"SDTM"})
        df_tbl_row.update({"Rule ID":r_id})
        df_tbl_row.update({"Version":"3.3","Definition":r4_def})
        df_tbl_rows.append(df_tbl_row)
        df_tbl = pd.DataFrame.from_records(df_tbl_rows)
        write_df2html(df_tbl,fn_rule,f"Rule ID {r_id}")
    # End of for r_id, group in df2_grouped

def get_rr_def(f_id, df3): 
    if f_id not in df3.index: 
        return None
    core_id = df3.loc[f_id,"CORE-ID"]
    r_sc = str(df3.loc[f_id,"Scope"])
    r_op = str(df3.loc[f_id,"Operations"])
    r_ck = str(df3.loc[f_id,"Check"]) 
    if isinstance(r_sc, pd.Series): 
        # r_scope = json.dumps(r_sc.to_dict())
        # r_opera = json.dumps(df3.loc[f_id,"Operations"].to_dict())
        # r_check = json.dumps(df3.loc[f_id,"Check"].to_dict())
        r_scope = r_sc.to_dict()
        r_opera = r_op.to_dict()
        r_check = r_ck.to_dict()
    else: 
        # r_scope = json.dumps(r_sc) 
        # r_opera = json.dumps(df3.loc[f_id,"Operations"])
        # r_check = json.dumps(df3.loc[f_id,"Check"])
        r_scope = r_sc.replace("'", '"')
        r_opera = r_op.replace("'", '"')
        r_check = r_ck.replace("'", '"')
    # print(f"Scope: {r_scope}\nOpera: {r_opera}\nCheck: {r_check}")
    rr_def = {}
    rr_def["CORE-ID"] = core_id
    v1 = str(r_scope)
    if bool(v1 and v1.strip()) and v1 not in ("nan","X"): 
        try:
            rr_def["Scope"] = json.loads(v1)
        except json.decoder.JSONDecodeError:
            rr_def["Scope"] = v1 
            print(f"ERR: {core_id} Scope: {v1}")
    v2 = str(r_opera)
    if bool(v2 and v2.strip()) and v2 not in ("nan","X"):
        try: 
            rr_def["Operations"] = json.loads(v2)
        except json.decoder.JSONDecodeError:
            rr_def["Operations"] = v2 
            print(f"ERR: {core_id} Operations: {v2}")
    v4 = str(r_check)
    if bool(v4 and v4.strip()) and v4 not in ("nan","X"): 
        try:
            rr_def["Check"] = json.loads(v4)
        except json.decoder.JSONDecodeError:
            rr_def["Check"] = v4 
            print(f"ERR: {core_id} Check: {v4}")
    return rr_def

def fda_rules2html(f_ids:list=[],i_dir:str="./data/source",o_dir:str="./data/output"):
    # 1. read rule definition
    fn_sdtm = "fda_vr1_6.pick"
    # fn_sdtm = "sdtm_v2_0.pick"
    fp_sdtm = f"{i_dir}/pick/{fn_sdtm}"
    fp_html = f"{o_dir}/html/fda_vr1_6_idx.html"
    fp_csv  = f"{o_dir}/csvs/fda_vr1_6_idx.csv"
    sud_dir = "sdtm"
    html_sud_dir=f"{o_dir}/html/{sud_dir}"
    df_fda_sdtm = read_pick(fp_sdtm)
    # FDA Validator Rule ID,Publisher,Rule ID,Publisher ID,FDA Validator Rule Message,FDA Validator Rule Description,Domains,SDTMIG3.1.2,SDTMIG3.1.3,SDTMIG3.2,SDTMIG3.3,SENDIG3.0,SENDIG3.1,SENDIG3.1.1,SENDIG-AR1.0,SENDIG-DART1.1
    # CT2001,FDA,FDAB017,FDAB017,Variable value not found in non-extensible codelist,Variable must be populated with terms from its CDISC controlled terminology codelist. New terms cannot be added into non-extensible codelists.,ALL,X,X,X,X,X,X,X,X,X
    # CT2002,FDA,FDAB017,FDAB017,Variable value not found in extensible codelist,"Variable should be populated with terms from its CDISC controlled terminology codelist. New terms can be added as long as they are not duplicates, synonyms or subsets of existing standard terms.",ALL,X,X,X,X,X,X,X,X,X
    df1 = df_fda_sdtm               # FDA rule definition 
    df1_keys = df1.columns
    # print(f"FDA CR Definition DF1 Keys: {df1_keys}")
    # FDA CR Definition DF1 Keys: Index(['FDA Validator Rule ID', 'Publisher', 'Publisher ID',
    #    'FDA Validator Rule Message', 'FDA Validator Rule Description',
    #    'Domains', 'SDTMIG3.1.2', 'SDTMIG3.1.3', 'SDTMIG3.2', 'SDTMIG3.3',
    #    'SENDIG3.0', 'SENDIG3.1', 'SENDIG3.1.1', 'SENDIG-AR1.0',
    #    'SENDIG-DART1.1'],
    #   dtype='object')


    # 2. read SDTM rule definition 
    fn_sd2 = "sdtm_v2_0.pick"
    fp_sd2 = f"{i_dir}/pick/{fn_sd2}"
    df2 = read_pick(fp_sd2)
    df2_keys = df2.columns
    df2_grouped = df2.groupby("Rule ID")
    # print(f"SDTM Rule Definition DF2 Keys: {df2_keys}")
    # ,Rule ID,SDTMIG Version,Rule Version,Class,Domain,Variable,Condition,Rule,Document,Section,Item,Cited Guidance,Release Notes
    # 0,CG0001,3.2,1,ALL,ALL,DOMAIN,Not custom domain,DOMAIN = valid Domain Code published by CDISC,IG v3.2,2.6,3.d,"Check the CDISC Controlled Terminology [see Appendix C - Controlled Terminology] for reserved two-character domain identifiers or abbreviations. If one has not been assigned by CDISC, then the sponsor may select the unique two-character domain code to be used consistently throughout the submission.",
    # 1,CG0001,3.3,1,ALL,ALL,DOMAIN,Not custom domain,DOMAIN = valid Domain Code published by CDISC,IG v3.3,2.6,3.e,"Determine the domain code, one that is not a domain code in the CDISC Controlled Terminology codelist ""SDTM Domain Abbreviations"" available at http://www.cancer.gov/research/resources/terminology/cdisc. If it desired to have this domain code as part of CDISC controlled terminology, then submit a request to https://ncitermform.nci.nih.gov/ncitermform/?version=cdisc. The sponsor-selected, two-character domain code should be used consistently throughout the submission.",
    # 2,CG0001,3.4,1,ALL,ALL,DOMAIN,Not custom domain,DOMAIN = valid Domain Code published by CDISC,IG v3.4,3.2.2,,Using SDTM-specified standard domain names and prefixes where applicable,Section and cited guidance updated.  Orginal cited guidance was for a custom domain.
    
    # SDTM Rule Definition DF2 Keys: Index(['Rule ID', 'SDTMIG Version', 'Rule Version', 'Class', 'Domain',
    #    'Variable', 'Condition', 'Rule', 'Document', 'Section', 'Item',
    #    'Cited Guidance', 'Release Notes'],
    #   dtype='object')

    # 3. read current rule developed
    df_rule = get_current_rule_defs()
    df3 = df_rule.rename(columns={'CDISC Rule ID': 'Rule ID'})
    df3_keys = df3.columns 
    df3.set_index("Rule ID", inplace=True)
    df3_fn = f"{i_dir}/csvs/cdisc_dev_rule2.csv"
    df3.to_csv(df3_fn)
    # print(f"Current Rule Development DF3 Keys: {df3_keys}") 
    # Current Rule Development DF3 Keys: Index(['CORE-ID', 'Rule ID', 'Organization', 'Standard Name',
    #    'Standard Version', 'Status', 'Sensitivity', 'Rule Type',
    #    'Executability', 'Scope', 'Error Message', 'Output Variables',
    #    'Description', 'Check', 'Operations'],
    #   dtype='object')

    # 4. read p21 rule definition
    df4 = get_fda_sdtm()
    df4_32 = get_fda_32()
    df4_33 = get_fda_33()
    df4_keys = df4.columns
    df4_32.set_index("ID", inplace=True)
    df4_33.set_index("ID", inplace=True)
    df5_cols = df4_33.columns 
    df5_keys = df4_33.keys()
    # print(f"P21 DF4 Keys: {df4_keys}")
    # P21 DF4 Keys: Index(['Rule Type', 'Category', 'ID', 'PublisherID', 'Message', 'Description',
    #    'Variable', 'Terms', 'Delimiter', 'PairDelimiter', 'PairedVariable',
    #    'When', 'Type', 'Test', 'Properties', 'GroupBy', 'Optional', 'Where',
    #    'From', 'Matching', 'Search', 'WhereFailure', 'Target', 'IgnoreContext',
    #    'MatchExact', 'If', 'Match', 'Minimum', 'Count', 'SDTMIG Version'],
    #   dtype='object')

    # 5. Process and link 
    df_tbl = df1 
    tot_i = df_tbl.shape[0]
    tot_j = df2_grouped.ngroups
    is_match = None
    k_pub = "Publisher ID"
    k_id = "Rule ID"
    j_ver = "SDTMIG Version"
    df_tbl.insert(6,"CDISC Condition", None)
    df_tbl.insert(7,"CDISC Rule", None)
    df_tbl.insert(8,"Cited Guidance", None) 

    df_tbl.insert( 9,"Core ID", None)
    df_tbl.insert(10,"Organization", None)
    df_tbl.insert(11,"Standard Name", None)
    df_tbl.insert(12,"CDISC Rule Type", None)
    df_tbl.insert(13,"CDISC Rule Status", None)    
    df_tbl.insert(14,"CDISC Description", None)
    df_tbl.insert(15,"CDISC Definition", None)       # Scope, Check and Operation

    df_tbl.insert(16,"P21 Rule Type", None)
    df_tbl.insert(17,"P21 Category", None)
    df_tbl.insert(18,"P21 Message", None)
    df_tbl.insert(19,"P21 Description", None)
    df_tbl.insert(20,"P21 Rule Definition", None)
    r_tbl_cols = ["FDA ID","Rule ID", "Key","Value"]
    f_id_key = "FDA Validator Rule ID"
    for i, r1 in df_tbl.iterrows():
        f_id = r1.get(f_id_key)
        if len(f_ids) > 0 and f_id not in f_ids:
            continue 
        h_fn = f"{o_dir}/html/fda/{f_id}.html"
        r_tbl = pd.DataFrame(columns=r_tbl_cols) 
        r_tbl_rows = []
        p_ids = r1.get(k_pub)
        r_tbl_rows.append({"FDA ID":"-----", "Rule ID":"-----",
            "Key": "-----", "Value": "-- FDA Comformance Rule --"})
        for k1 in df1_keys:
            v1 = str(r1.get(k1))
            r_tbl_rows.append({"FDA ID":f_id, "Rule ID":p_ids,
                               "Key": k1, "Value": v1})
        if f_id in df3.index:   
            r_tbl_rows.append({"FDA ID":"-----", "Rule ID":"-----",
                "Key": "-----", "Value": "-- CDISC Rules --"})
            for k3 in df3.columns:
                v3 = str(df3.loc[f_id, k3])
                if bool(v3 and v3.strip()) and v3 not in ("nan","X"): 
                    # print(f"FID={f_id}: {k3}={v3}")
                    r_tbl_rows.append({"FDA ID":f_id, "Rule ID":p_ids,
                                "Key": k3, "Value": v3})
        if f_id in df4_32.index: 
            r_tbl_rows.append({"FDA ID":"-----", "Rule ID":"-----",
                "Key": "-----", "Value": "-- P21 SDTM V3.2 --"})
            for k4 in df4_32.columns:
                v4 = str(df4_32.loc[f_id, k4])
                if bool(v4 and v4.strip()) and v4 not in ("nan","X"): 
                    r_tbl_rows.append({"FDA ID":f_id, "Rule ID":p_ids,
                                "Key": k4, "Value": v4})
        if f_id in df4_33.index: 
            r_tbl_rows.append({"FDA ID":"-----", "Rule ID":"-----",
                "Key": "-----", "Value": "-- P21 SDTM V3.3 --"})
            for k4 in df4_33.columns:
                v4 = str(df4_33.loc[f_id, k4])
                if bool(v4 and v4.strip()) and v4 not in ("nan","X"): 
                    r_tbl_rows.append({"FDA ID":f_id, "Rule ID":p_ids,
                                "Key": k4, "Value": v4})                 
        p_id_lst1 = p_ids.split(",")
        p_id_lst = [element.strip() for element in p_id_lst1]
        tot_k = len(p_id_lst)
        k_cnt = 0
        i_cnt = i + 1
        c_condition = ""
        c_rule = ""
        c_cited_guidance = ""
        print(f"{i_cnt}/{tot_i} - {f_id}: {p_ids}")
        # 5.1 link to SDTM definition
        for p_id in p_id_lst: 
            k_cnt += 1
            print(f"  {k_cnt}/{tot_k}:{p_id}")
            j_cnt = 0
            c_condition = "" if c_condition == "" else f"{c_condition}<hr>"
            c_rule = "" if c_rule == "" else f"{c_rule}<hr>"
            c_cited_guidance = "" if c_cited_guidance == "" else f"{c_cited_guidance}<hr>"
            for r_id, group in df2_grouped:
                # r_id = r2.get(k_id)
                num_records = group.shape[0]
                j_cnt += 1
                # print(f"    {j_cnt}/{tot_j}:{r_id}")
                if r_id != p_id:
                    is_match = "No"
                    continue
                else:
                    is_match = "Yes"
                print(f"      {j_cnt}/{tot_j}:{is_match} - {r_id}/{p_id} in ({p_ids})")

                # replace r_id in p_ids with a html link to a file
                df_tbl.iloc[i][k_pub] = re.sub(r_id, 
                    f"<a href='./{sud_dir}/{r_id}.html' target='_blank'>{r_id}</a>", df_tbl.iloc[i][k_pub])
                for v_i, row in group.iterrows():
                    c_cdn = row.get("Condition")
                    c_rul = row.get("Rule")
                    c_cit = row.get("Cited Guidance")
                    r_ver = row.get(j_ver)
                    c_key = f"SDTMIG{r_ver}"
                    r_tbl_rows.append({"FDA ID":"-----", "Rule ID":"-----",
                                    "Key": "-----", 
                                    "Value": f"-- SDTM Rule Definition ({r_ver}) --"})       
                    for k2 in df2_keys:
                        v2 = str(row.get(k2))
                        if bool(v2 and v2.strip()) and v2 not in ("nan","X"): 
                            r_tbl_rows.append({"FDA ID":f_id, "Rule ID":p_id,
                                    "Key": k2, "Value": v2})
                        
                    c_condition = f"{c_condition}{r_id}/{r_ver}: {c_cdn}<br>"
                    c_rule = f"{c_rule}{r_id}/{r_ver}: {c_rul}<br>"
                    c_cited_guidance = f"{c_cited_guidance}{r_id}/{r_ver}: {c_cit}<br>"
                    if v_i > 0:
                        c_condition = f"{c_condition}<br>"
                        c_rule = f"{c_rule}<br>"
                        c_cited_guidance = f"{c_cited_guidance}<br>"
                    if c_key in df1_keys:
                        c_val = df_tbl.iloc[i][c_key]
                        df_tbl.iloc[i][c_key] = re.sub(c_val, 
                            f"<a href='./{sud_dir}/{r_id}-{r_ver}.html' target='_blank'>{c_val}</a>", 
                            df_tbl.iloc[i][c_key])
                    # End of if c_key in df1_keys
                # End of for v_i, row in group.iterrows()
            # End of for r_id, group in df2_grouped
        # End of for p_id in p_id_lst
        df_tbl.iloc[i]["CDISC Condition"] = c_condition
        df_tbl.iloc[i]["CDISC Rule"] = c_rule
        df_tbl.iloc[i]["Cited Guidance"] = c_cited_guidance

        # 5.2 link to CDISC rule development
        df_tbl.iloc[i]["Core ID"] = df3.loc[f_id,"CORE-ID"]
        df_tbl.iloc[i]["Organization"] = df3.loc[f_id,"Organization"]
        df_tbl.iloc[i]["Standard Name"] = df3.loc[f_id,"Standard Name"]
        df_tbl.iloc[i]["CDISC Rule Type"] = df3.loc[f_id,"Rule Type"]
        df_tbl.iloc[i]["CDISC Rule Status"] = df3.loc[f_id,"Status"]
        r_err = df3.loc[f_id,"Error Message"]
        r_dsc = df3.loc[f_id,"Description"]
        df_tbl.iloc[i]["CDISC Description"] = f"Error Message: {r_err}<br><br>Description: {r_dsc}"
    
        rr_def = get_rr_def(f_id, df3)
        # json_dict = json.dumps(rr_def)
        yaml_str = yaml.dump(rr_def, default_flow_style=False)
        # print(f"JSON: {rr_def}\nYAML: {yaml_str}")
        # text = yaml_str.replace('\n','<br>').replace(' ','&nbsp;')
        text = yaml_str.replace('\n','<br>')
        rr_defs = f"{f_id}:<br><code>{yaml_str}</code>"

        for p_id in p_id_lst:
            rr_dd = get_rr_def(p_id, df3)
            if rr_dd is None: 
                continue    
            yaml_str = yaml.dump(rr_dd, default_flow_style=False)
            # text = yaml_str.replace('\n','<br>').replace(' ', '&nbsp;')
            # text = yaml_str.replace('\n','<br>')
            rr_defs = f"{rr_defs}<br><br>{p_id}:<br><code>{yaml_str}</code>"

        df_tbl.iloc[i]["CDISC Definition"] = rr_defs 

        # 5.3 link to P21 ruke definition
        skipped_cols = ["Rule Type","Category","PublisherID","Message","Description"]
        if f_id in df4_33.index: 
            df_tbl.iloc[i]["P21 Rule Type"] = df4_33.loc[f_id,"Rule Type"]
            df_tbl.iloc[i]["P21 Category"] = df4_33.loc[f_id,"Category"]
            df_tbl.iloc[i]["P21 Message"] = df4_33.loc[f_id,"Message"]
            df_tbl.iloc[i]["P21 Description"] = df4_33.loc[f_id,"Description"]
            
            r4_def = f"<table id='{f_id}-p21.33-{i}'>"
            for k4 in df5_cols:
                v4 = str(df4_33.loc[f_id,k4])
                if k4 not in skipped_cols:
                    if bool(v4 and v4.strip()) and v4 not in ("nan","X"): 
                        r4_def = f"{r4_def}<tr><td>{k4}</td><td>{v4}</td></tr>"
            r4_def = f"{r4_def}</table>"
            df_tbl.iloc[i]["P21 Rule Definition"] = r4_def
        
        # write out each FDA rule
        df_tbl.iloc[i][f_id_key] = re.sub(f_id, 
            f"<a href='./fda/{f_id}.html' target='_blank'>{f_id}</a>", df_tbl.iloc[i][f_id_key])
        r_tbl = pd.DataFrame.from_records(r_tbl_rows)
        write_df2html(r_tbl,h_fn)
    # End of for i, r1 in df_tbl.iterrows()
    write_df2html(df_tbl, fp_html)
    print(f"Writing to {fp_csv}...")
    df_tbl.to_csv(fp_csv, index=False)
    return df_tbl 
    

def print_df (r_id:str=None, k_id:str=None, df:dict=None):
    r_xlsx_cols = ["Rule ID","Publisher","FDA Validator Rule ID",
        "Publisher ID", 
        "DTMIG Version", "Class", "Domain", "Variable",
        "FDA Validator Rule Message", "Condition", "Cited Guidance"],
    
    r_rule_cols = ["CORE-ID","CDISC Rule ID","Status",
        "Organization","Standard Name","Standard Version",
        "Rule Type","Scope","Error Message","Description",
        "Check","Operation"]
    r_p21_cols =[]
    k1 = k_id
    df_sorted = df.sort_values(by=k1)
    grouped_df = df_sorted.groupby(k1)
    for rule_id, group in grouped_df:
        if r_id is not None and r_id != rule_id:
            continue 
        rule_data = df[df[k1] == rule_id]
        rule_data = rule_data.reset_index(drop=True)
        for i, row in rule_data.iterrows():
            for k in row.keys():
                print(f"{k}: {row[k]}")


# Test cases
if __name__ == "__main__":
    # combine_sdtm()
    df = fda_rules2html([])
    # sdtm2html()
    # print_df("CG0021", df=df)
