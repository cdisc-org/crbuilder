# Purpose: Link CDISC rules and write result to html file
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   05/09/2023 (htu) - initial coding based on scripts/rule_html
#

import os 
import re 
import glob 
import pandas as pd
from rulebuilder.echo_msg import echo_msg
from rulebuilder.write_df2html import write_df2html
from rulebuilder.pickle_files import read_pick, write_pick
from rulebuilder.get_rule_data import (get_fda_32, get_fda_33, 
    get_fda_sdtm, get_current_rule_defs)

def cdisc_rule2html(r_ids:list=[],i_dir:str="./data/source",o_dir:str="./data/output"):
    v_prg = __name__
    # 1. read rule definition
    fn_sdtm = "fda_vr1_6.pick"
    # fn_sdtm = "sdtm_v2_0.pick"
    fp_sdtm = f"{i_dir}/pick/{fn_sdtm}"
    fp_html = f"{o_dir}/html/cdisc_v2_0_idx.html"
    fp_csv  = f"{o_dir}/csvs/cdisc_v2_0_idx.csv"
    sud_dir = "sdtm"
    html_sud_dir=f"{o_dir}/html/{sud_dir}"
    df_fda_sdtm = read_pick(fp_sdtm)
    # FDA Validator Rule ID,Publisher,Rule ID,Publisher ID,FDA Validator Rule Message,FDA Validator Rule Description,Domains,SDTMIG3.1.2,SDTMIG3.1.3,SDTMIG3.2,SDTMIG3.3,SENDIG3.0,SENDIG3.1,SENDIG3.1.1,SENDIG-AR1.0,SENDIG-DART1.1
    # CT2001,FDA,FDAB017,FDAB017,Variable value not found in non-extensible codelist,Variable must be populated with terms from its CDISC controlled terminology codelist. New terms cannot be added into non-extensible codelists.,ALL,X,X,X,X,X,X,X,X,X
    # CT2002,FDA,FDAB017,FDAB017,Variable value not found in extensible codelist,"Variable should be populated with terms from its CDISC controlled terminology codelist. New terms can be added as long as they are not duplicates, synonyms or subsets of existing standard terms.",ALL,X,X,X,X,X,X,X,X,X
    df1 = df_fda_sdtm               # FDA rule definition 
    f_id_key = "FDA Validator Rule ID"
    df1.set_index(f_id_key, inplace=True)
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
    df2i = df2.set_index("Rule ID", inplace=False)
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
    df3i = df3.set_index("Rule ID", inplace=False)
    # df3_fn = f"{i_dir}/csvs/cdisc_dev_rule2.csv"
    # df3.to_csv(df3_fn)
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
    def append_row (r_id, keys, row, r_msg:str=None):
        r_tbl_rows.append({"Rule ID":f"-- {r_id} --",
                        "Key": "-----", 
                        "Value": f"-- {r_msg} --"})       
        for k2 in keys:
            v2 = str(row.get(k2))
            if bool(v2 and v2.strip()) and v2 not in ("nan","X"): 
                r_tbl_rows.append({"Rule ID":r_id,
                        "Key": k2, "Value": v2})
    # End of def append_row

    df_tbl = df3
    tot_i = df_tbl.shape[0]
    k_pub = "Publisher ID"
    k_id = "Rule ID"
    j_ver = "SDTMIG Version"

    df_tbl.insert(7,"P21 Rule Type", None)
    df_tbl.insert(8,"P21 Category", None)
    df_tbl.insert(9,"P21 Message", None)
    df_tbl.insert(10,"P21 Description", None)
    df_tbl.insert(11,"P21 Rule Definition", None)
    r_tbl_cols = ["Rule ID", "Key","Value"]
    f_id_key = "FDA Validator Rule ID"
    p_files = glob.glob(f"{i_dir}/pick/*-IG*")
    v_stp = 5.1
    for i, r1 in df_tbl.iterrows():
        r_id = r1.get(k_id)
        if len(r_ids) > 0 and r_id not in r_ids:
            continue 
        v_msg = f"Processing {r_id}..."
        echo_msg(v_prg, v_stp, v_msg, 3, i=i+1, n= tot_i)
        h_fn = f"{o_dir}/html/cdisc/{r_id}.html"
        r_tbl = pd.DataFrame(columns=r_tbl_cols) 
        r_tbl_rows = []
        append_row(r_id,df3_keys,r1, "CDISC CORE Rules")

        # r_tbl_rows.append({"Rule ID":f"-- {r_id} --",
        #     "Key": "-----", "Value": "-- CDISC CORE Rules --"})
        # for k1 in df3_keys:     # df3 is assigned to df_tbl 
        #     v1 = str(r1.get(k1))
        #     r_tbl_rows.append({"Rule ID":r_id,
        #                        "Key": k1, "Value": v1})
        
        # Check FDA comformance rules
        if r_id in df1.index:   
            append_row(r_id,df1.columns,df1.loc[r_id], "FDA Comformance Rules")
            # r_tbl_rows.append({"Rule ID":"-----",
            #     "Key": "-----", "Value": "-- FDA Comformance Rules --"})
            # for k1 in df1.columns:
            #     v1 = str(df1.loc[r_id, k1])
            #     if bool(v1 and v1.strip()) and v1 not in ("nan","X"): 
            #         # print(f"FID={f_id}: {k3}={v3}")
            #         r_tbl_rows.append({"Rule ID":r_id,
            #                     "Key": k1, "Value": v1})
            # check associated publisher ids
            p_ids = df1.loc[r_id, k_pub]
            p_id_lst = p_ids.split(",")
            p_id_lst = [element.strip() for element in p_id_lst]    # remove spaces
            for p_id in p_id_lst: 
                if p_id == r_id:
                    continue
                if p_id not in df3i.index:
                    continue 
                append_row(p_id,df3i.columns,df3i.loc[p_id], p_ids)
                # r_tbl_rows.append({"Rule ID":f"-- {p_id} --",
                #     "Key": "-----", "Value": f"-- {p_ids} --"})
                # for k1 in df3i.columns:     # df3 is assigned to df_tbl 
                #     v1 = str(df3i.loc[p_id,k1])
                #     r_tbl_rows.append({"Rule ID":p_id,
                #                     "Key": k1, "Value": v1})
            # End of for p_id in p_id_lst
        # End of if r_id in df1.index

        # Check all the p21 files 
        for p_fn in p_files: 
            df = read_pick(p_fn) 
            df.set_index("ID", inplace=True)
            if r_id not in df.index:
                continue 
            df_tbl.iloc[i]["P21 Rule Type"] = df.loc[r_id,"Rule Type"]
            df_tbl.iloc[i]["P21 Category"] = df.loc[r_id,"Category"]
            df_tbl.iloc[i]["P21 Message"] = df.loc[r_id,"Message"]
            df_tbl.iloc[i]["P21 Description"] = df.loc[r_id,"Description"]

            fn_with_extension = os.path.basename(p_fn)
            fn_no_ext = os.path.splitext(fn_with_extension)[0]
            append_row(r_id,df.columns,df.loc[r_id], fn_no_ext)

            # r_tbl_rows.append({"Rule ID":"-----",
            #     "Key": "-----", "Value": f"-- {p_fn} --"})
            # for k4 in df.columns:
            #     v4 = str(df.loc[r_id, k4])
            #     if bool(v4 and v4.strip()) and v4 not in ("nan","X"): 
            #         r_tbl_rows.append({"Rule ID":r_id,
            #                     "Key": k4, "Value": v4})
        # End of for p_fn in p_files

        # Check SDTM definition
        if r_id in df2i.index: 
            group = df2i.loc[r_id]
            if isinstance(group,pd.Series):
                # print(f"GROUP: {group}")
                r_ver = group.get(j_ver)
                r_msg = f"SDTM Rule Definition ({r_ver})"
                append_row(r_id,df2_keys,group, r_msg)
            else: 
                for row_i,row in group.iterrows():
                    r_ver = row.get(j_ver)
                    r_msg = f"SDTM Rule Definition ({r_ver})"
                    append_row(r_id,df2_keys,row,r_msg)
            # End of for v_i, row in group.iterrows()
        # End of if r_id in df2i.index

        # write out each FDA rule
        df_tbl.iloc[i][k_id] = re.sub(str(r_id), 
            f"<a href='./cdisc/{str(r_id)}.html' target='_blank'>{str(r_id)}</a>", str(df_tbl.iloc[i][k_id]))
        r_tbl = pd.DataFrame.from_records(r_tbl_rows)
        write_df2html(r_tbl,h_fn)
    # End of for i, r1 in df_tbl.iterrows()
    write_df2html(df_tbl, fp_html)
    print(f"Writing to {fp_csv}...")
    df_tbl.to_csv(fp_csv, index=False)
    return df_tbl 
    

# Test cases
if __name__ == "__main__":
    os.environ["g_msg_lvl"] = "3"
    # df = cdisc_rule2html(["SD0011","CG0063"])
    df = cdisc_rule2html([])
    # sdtm2html()

