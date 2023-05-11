# Purpose: Get rule data from various sources
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   05/09/2023 (htu) - initial coding extracted from scripts/rule_html
#
import pandas as pd 
import pickle
from rulebuilder.pickle_files import read_pick

def get_fda_sdtm(i_dir:str="./data/source",o_dir:str="./data/output"):
    df_s3_2 = get_fda_32(i_dir,o_dir)
    df_s3_3 = get_fda_33(i_dir,o_dir)
    df_sdtm_fda = pd.concat([df_s3_2, df_s3_3])
    return df_sdtm_fda

def get_fda_32(i_dir:str="./data/source",o_dir:str="./data/output"):
    # This pkl file is from parse_xml_files
    fn_s3_2 = "SDTM-IG 3.2 (FDA).pkl"
    fp_s3_2 = f"{i_dir}/pick/{fn_s3_2}"
    df_s3_2 = read_pick(fp_s3_2)
    df_s3_2["SDTMIG Version"] = "3.2"
    return df_s3_2

def get_fda_33(i_dir:str="./data/source",o_dir:str="./data/output"):
    # This pkl file is from parse_xml_files
    fn_s3_3 = "SDTM-IG 3.3 (FDA).pkl"
    fp_s3_3 = f"{i_dir}/pick/{fn_s3_3}"
    df_s3_3 = read_pick(fp_s3_3)
    df_s3_3["SDTMIG Version"] = "3.3"
    return df_s3_3

def get_current_rule_defs (i_dir:str="./data/source",o_dir:str="./data/output"): 
    # This CSV file is from get_rule_list
    fn_rule = "R193830-20230428.csv"
    fp_rule = f"{i_dir}/rule/{fn_rule}"
    df_rule = pd.read_csv(fp_rule)
    return df_rule

