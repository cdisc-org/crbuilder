# Purpose: Combine CDISC and FDA Rules
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   04/29/2023 (htu) - initial coding

import re 
import pandas as pd
import yaml
import pickle
from bs4 import BeautifulSoup

def read_pick(fn):
    # Open the file for reading binary data
    with open(fn, 'rb') as f:
        # Deserialize the object from the file
        data = pickle.load(f)
    # Create a DataFrame from the loaded data
    df = pd.DataFrame(data)
    # print(df) 
    return df   

def get_fda_sdtm(i_dir:str="./data/source",o_dir:str="./data/output"):
    fn_s3_2 = "SDTM-IG 3.2 (FDA).pkl"
    fp_s3_2 = f"{i_dir}/pick/{fn_s3_2}"
    df_s3_2 = read_pick(fp_s3_2)
    df_s3_2["SDTMIG Version"] = "3.2"

    fn_s3_3 = "SDTM-IG 3.3 (FDA).pkl"
    fp_s3_3 = f"{i_dir}/pick/{fn_s3_3}"
    df_s3_3 = read_pick(fp_s3_3)
    df_s3_3["SDTMIG Version"] = "3.3"

    df_sdtm_fda = pd.concat([df_s3_2, df_s3_3])
    return df_sdtm_fda

def get_current_rule_defs (i_dir:str="./data/source",o_dir:str="./data/output"): 
    fn_rule = "R193830-20230428.csv"
    fp_rule = f"{i_dir}/rule/{fn_rule}"
    df_rule = pd.read_csv(fp_rule)
    return df_rule

def cb_rules(typ:str,df1, df2, df3, i_dir:str="./data/source",o_dir:str="./data/output"): 
    if typ.lower() == "sdtm": 
        df_c1 = pd.merge(df1,df2,how="left",
                        left_on=["Rule ID","SDTMIG Version"], 
                        right_on=["PublisherID","SDTMIG Version"])
        df_c2 = pd.merge(df_c1,df3,how="left", 
                        left_on=["Rule ID"], 
                        right_on=["CDISC Rule ID"])
    elif typ.lower() == "fdacr":
        # 2.1 read in SDTM definition 
        fn_sdtm = "sdtm_v2_0.pick"
        fp_sdtm = f"{i_dir}/pick/{fn_sdtm}"
        df_sdtm = read_pick(fp_sdtm)

        # 2.2 expand Publisher IDs and add "Rule ID" for df1 - FDA Rule definition
        # create a new column in each dataframe containing the key value to match on
        k1 = "Publisher ID"
        k2 = "Rule ID"

        # split the 'PublisherIDs' column into multiple rows
        s = df1[k1].str.split(',', expand=True).stack().reset_index(level=1, drop=True)
        s.name = k2

        # create a new dataframe with the 'PublisherIDs' column dropped and the 'Rule ID' column added
        # new_df1 = df1.drop(k1, axis=1).join(s)
        new_df1 = df1.join(s)
        # print(new_df2)
        col_3 = new_df1.pop(k2)
        new_df1.insert(2, k2, col_3)

        # 2.3 expand "PublisherID in p21 FDA rule definitions
        #  
        k1 = "PublisherID"
        k2 = "Rule ID"

        # split the 'PublisherIDs' column into multiple rows
        # print (f"DF2 Keys: {df2[0].keys()}")
        s = df2[k1].str.split(',', expand=True).stack().reset_index(level=1, drop=True)
        s.name = k2

        # create a new dataframe with the 'PublisherIDs' column dropped and the 'Rule ID' column added
        # new_df1 = df1.drop(k1, axis=1).join(s)
        new_df2 = df2.join(s)
        # print(new_df2)
        col_3 = new_df2.pop(k2)
        new_df2.insert(2, k2, col_3)

        # left join df1 to the new_df1 based on the 'Rule ID' column
        df_c1 = pd.merge(new_df1, df_sdtm, how='left', on=k2)
        df_c2 = pd.merge(df_c1,new_df2,how="left", on=k2)
        df_c2 = pd.merge(df_c2,df3,how="left", 
                         left_on=k2,right_on=["CDISC Rule ID"])

    fn_c1 = f"{typ}-c1_rules.csv" 
    fp_c1 = f"{o_dir}/rule/{fn_c1}"
    print(f"Writing to {fp_c1}...")
    df_c1.to_csv(fp_c1, index=False)

    fn_c2 = f"{typ}-c2_rules.csv" 
    fp_c2 = f"{o_dir}/rule/{fn_c2}"
    print(f"Writing to {fp_c2}...")
    df_c2.to_csv(fp_c2, index=False)
    return df_c2


def combine_sdtm(i_dir:str="./data/source",o_dir:str="./data/output"):
    # 1. read rule definition
    fn_sdtm = "sdtm_v2_0.pick"
    fp_sdtm = f"{i_dir}/pick/{fn_sdtm}"
    df_sdtm = read_pick(fp_sdtm)

    # 2. read current rule developed
    df_rule = get_current_rule_defs()

    # 3. read p21 rule definition
    df_sdtm_fda = get_fda_sdtm()

    # 4. Merge dataframes
    cb_rules("sdtm",df_sdtm,df_sdtm_fda,df_rule)

def combine_fda_rules(f_rule_id:str=None, i_dir:str="./data/source",o_dir:str="./data/output"):
    # 1. read rule definition
    fn_sdtm = "fda_vr1_6.pick"
    # fn_sdtm = "sdtm_v2_0.pick"
    fp_sdtm = f"{i_dir}/pick/{fn_sdtm}"
    fp_html = f"{o_dir}/html/fda_vr1_6.html"
    df_sdtm = read_pick(fp_sdtm)
    df1 = df_sdtm               # FDA rule definition 

    # 2.1 expand Publisher IDs and add "Rule ID" for df1 - FDA Rule definition
    # df = df.rename(columns={'FDA Validator Rule ID': 'Rule ID'})

    # create a new column in each dataframe containing the key value to match on
    k1 = "Publisher ID"
    k2 = "Rule ID"
    # split the 'Publisher ID' column into multiple rows
    s = df1[k1].str.split(',', expand=True).stack().reset_index(level=1, drop=True)
    s.name = k2

    # create a new dataframe with the 'PublisherIDs' column dropped and the 'Rule ID' column added
    # new_df1 = df1.drop(k1, axis=1).join(s)
    new_df1 = df1.join(s)
    # print(new_df2)
    col_3 = new_df1.pop(k2)
    new_df1.insert(2, k2, col_3)
    df1_keys = new_df1.columns
    print(f"FDA Rule Definition DF1 Keys: {df1_keys}")
    fp1 = f"{o_dir}/csvs/fda_vr1_6.csv"
    new_df1.to_csv(fp1,index=False)


    # 2.2 read SDTM rule definition 
    fn_sd2 = "sdtm_v2_0.pick"
    fp_sd2 = f"{i_dir}/pick/{fn_sd2}"
    df_sd2 = read_pick(fp_sd2)
    df2_keys = df_sd2.columns
    print(f"SDTM Rule Definition DF2 Keys: {df2_keys}")
    fp2 = f"{o_dir}/csvs/sdtm_v2_0.csv"
    df_sd2.to_csv(fp2, index=False)

    
    # 2.3 read current rule developed
    df_rule = get_current_rule_defs()
    df3 = df_rule.rename(columns={'CDISC Rule ID': 'Rule ID'})
    df3_keys = df3.columns 
    print(f"Current Rule Development DF3 Keys: {df3_keys}") 
    fp3 = f"{o_dir}/csvs/cdisc_r1_0.csv"
    df3.to_csv(fp3, index=False)


    # 2.4 read p21 rule definition
    df_sdtm_fda = get_fda_sdtm()
    df4_key1 = df_sdtm_fda.columns
    print(f"P21 DF4 Key1: {df4_key1}")

    df4 = df_sdtm_fda
    k1 = "PublisherID"
    k2 = "Rule ID"
    # split the 'Publisher ID' column into multiple rows
    s = df4[k1].str.split(',', expand=True).stack().reset_index(level=1, drop=True)
    s.name = k2

    # create a new dataframe with the 'PublisherIDs' column dropped and the 'Rule ID' column added
    # new_df1 = df1.drop(k1, axis=1).join(s)
    new_df4 = df4.join(s)
    # print(new_df2)
    col_3 = new_df4.pop(k2)
    new_df4.insert(4, k2, col_3)
    df4_key2 = new_df4.columns 
    print(f"P21 DF4 Key2: {df4_key2}") 
    fp4 = f"{o_dir}/csvs/fda_p21.csv"
    new_df4.to_csv(fp4, index=False)


    k1 = "FDA Validator Rule ID"
    df_sorted = new_df1.sort_values(by=k1)
    grouped_df = df_sorted.groupby(k1)
    df_tbl = pd.DataFrame(columns=["Publisher","Standard","FDA ID","Rule ID",
                                    "Key","Value"])   
    rows = []
    tot_groups = grouped_df.ngroups
    grp_cnt = 0 
    for rule_id, group in grouped_df:
        grp_cnt += 1
        if f_rule_id is not None and f_rule_id != rule_id:
            continue 
        print(f"{grp_cnt}/{tot_groups} - {rule_id}...")
        # 3.1 add FDA rule definition 
        rule_data = new_df1[new_df1[k1] == rule_id]
        rule_data = rule_data.reset_index(drop=True)
        r_pb = rule_data.iloc[0].get("Publisher")
        r_id = rule_data.iloc[0].get("Rule ID")
        row = {"Publisher":r_pb, "Standard": "FDA VR1.6",
               "FDA ID":f_rule_id,"Rule ID":r_id,
               "Key":None,"Value":None}
        for k in df1_keys:
            v = rule_data.iloc[0].get(k)
            print(f"  DF1: {k}={v}")
            if bool(v and v.strip()) and v not in ("nan","X"):
                row.update({"Key":k,"Value":v})
                print(f"  DF1 ROW[0]={row}")
                rows.append(row)            
        # 3.2 add SDTM definitions 
        row.update({"Publisher":"CDISC"})
        for i, r1 in rule_data.iterrows():
            r_id = r1.get("Rule ID")
            row.update({"Rule ID": r_id})
            r_data = df_sd2[df_sd2["Rule ID"] == r_id]
            r_data = r_data.reset_index(drop=True)
            for j, r2 in r_data.iterrows(): 
                r_st = f"SDTM IG {r2.get('SDTMIG Version')}"
                row.update({"Standard":r_st})
                for k2 in df2_keys:
                    v = r2.get(k2)
                    print(f"  DF2: {k2}={v}")
                    # Only if value is not blank and not "nan" or "X"
                    if bool(v and v.strip()) and v not in ("nan","X"): 
                        row.update({"Key": k2, "Value": v})
                        print(f"  DF2 ROW[{i}:{j}]={row}")
                        rows.append(row)
                        # print(f"{k2}: {v}")

        # 3.3 add Current Rule definitions
        for i, r1 in rule_data.iterrows():
            r_id = r1.get("Rule ID")
            row.update({"Rule ID": r_id})
            r_data = df3[df3["Rule ID"] == r_id]
            r_data = r_data.reset_index(drop=True)
            for j, r2 in r_data.iterrows(): 
                r_pb = r2.get("Organization")
                r_st = r2.get("Standard Name")
                row.update({"Publisher":r_pb, "Standard": f"D3 {r_st}"})
                for k2 in df3_keys:
                    v = str(r2.get(k2))
                    print(f"  DF3 {k2}={v}")
                    # Only if not blank and not in nan or X
                    if bool(v and v.strip()) and v not in ("nan","X"):
                        row.update({"Key": k2, "Value": v})
                        print(f"  DF3 ROW[{i}:{j}]={row}")
                        rows.append(row)
                        
        # 3.4 add P21 Rule definitions 
        for i, r1 in rule_data.iterrows():
            r_id = r1.get("Rule ID")
            row = {"Publisher":"FDA", "Standard": None,
               "FDA ID":f_rule_id,"Rule ID":r_id,
               "Key":None,"Value":None}
            r_data = new_df4[new_df4["Rule ID"] == r_id]
            r_data = r_data.reset_index(drop=True)
            print(f"{r_data}")
            for j, r2 in r_data.iterrows(): 
                for k2 in df4_key2:
                    v = str(r2.get(k2))
                    print(f"  DF4 {k2}={v}")
                    row.update({"Standard": "P21 FDA CR"})
                    if bool(v and v.strip()) and v not in ("nan","X"): 
                        row.update({"Key": k2,"Value": v})
                        print(f"  DF4 ROW[{i}:{j}]={row}")
                        rows.append(row)

    df_tbl = pd.DataFrame.from_records(rows)

    # print(df_tbl)

    # convert the dataframe to an HTML table
    html_table = df_tbl.to_html()

    # create a BeautifulSoup object
    soup = BeautifulSoup(html_table, 'html.parser')

    if not soup.html:
        soup.append(soup.new_tag('html'))
    if not soup.head:
        soup.html.append(soup.new_tag('head'))    # add some additional styling to the table
    table_style = soup.new_tag('style')
    table_style.string = 'table {border-collapse: collapse;} th, td {border: 1px solid black; padding: 5px;}'

    # append the styling to the head of the HTML document
    soup.head.append(table_style)

    # print the final HTML table
    # print(soup.prettify())

    # create a file object for writing
    with open(fp_html, 'w') as f:
        # write the prettified HTML code to the file
        f.write(soup.prettify())

    # 4. Merge dataframes
    # df4 = cb_rules("fdacr",df1=df_sdtm,df2=df_sdtm_fda,df3=df_rule)
    # df4.set_index('Rule ID')
    # print(f"DF_TBL: {df_tbl.describe()}")
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
    df = combine_fda_rules("SD0003")
    # print_df("CG0021", df=df)
