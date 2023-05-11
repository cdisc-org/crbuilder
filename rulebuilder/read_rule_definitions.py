# Purpose: Read Rule Definitions from source folder
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   04/10/2023 (htu) - initial coding by extracting from rbuilder.read_rule_definitions
#

import os
import re
import json 
import pickle
import pandas as pd
import ruamel.yaml as yaml
from dotenv import load_dotenv
from rulebuilder.echo_msg import echo_msg

def read_rule_definitions (r_std:str=None,r_dir:str=None,r_file:str=None, r_sheet:str = None ):
    v_prg = __name__ + ".read_rule_definitions"
    v_stp = 1.0
    v_msg = f"Reading rule definition for {r_std}..."
    echo_msg(v_prg, v_stp, v_msg, 2)
    r_std = "SDTM_V2_0" if r_std is None else r_std
    load_dotenv()

    r_dir = os.getenv("r_dir") if r_dir is None else r_dir 
    r_dir = "." if r_dir is None else r_dir 
    sheet_name = r_sheet
    fn_xlsx = r_std.lower() + ".xlsx" if r_file is None else r_file 
    fn_yaml = r_std.lower() + ".yaml"
    fn_pick = r_std.lower() + ".pick"
    fp_xlsx = r_dir + "/data/source/xlsx/" + fn_xlsx
    fp_yaml = r_dir + "/data/source/yaml/" + fn_yaml
    fp_pick = r_dir + "/data/source/pick/" + fn_pick


    # 1.1 read from a pickled file
    if os.path.isfile(fp_pick):
        v_stp = 1.1
        v_msg = f" . from {fp_pick}..."
        echo_msg(v_prg, v_stp, v_msg, 3)
        with open(fp_pick, 'rb') as f:
            # Deserialize the object from the file
            data = pickle.load(f)
        # Create a DataFrame from the loaded data
        df = pd.DataFrame(data)
        v_msg = f" . The dataset from pick has {df.shape[0]} records. "
        echo_msg(v_prg, v_stp, v_msg, 2)
    elif os.path.isfile(fp_yaml):
        v_stp = 1.2
        v_msg = f" . from {fp_yaml}..."
        echo_msg(v_prg, v_stp, v_msg, 3)
        with open(fp_yaml, "r") as f:
            data = yaml.safe_load(f)
        # Create DataFrame from YAML data
        df = pd.DataFrame(data)
        v_msg = f" . The dataset from yaml has {df.shape[0]} records. "
        echo_msg(v_prg, v_stp, v_msg, 3)
    elif os.path.isfile(fp_xlsx):
        v_stp = 1.3
        v_msg = f" . from {fp_xlsx}..."
        echo_msg(v_prg, v_stp, v_msg, 3)

        s_name = sheet_name
        if fp_xlsx.startswith('FDA'):
            df = pd.read_excel(fp_xlsx, sheet_name=s_name,
                                header=1, engine='openpyxl')
        else:
            df = pd.read_excel(fp_xlsx, sheet_name=s_name, engine='openpyxl')
        # remove newline breaks in column names
        df.columns = df.columns.str.replace('\n', '')
        df.columns = df.columns.str.lstrip()           # remove leading spaces
        df.columns = df.columns.str.rstrip()           # remove trailing spaces
        # remove newline breaks in data
        df = df.fillna('')                              # fill na with ''
        df = df.applymap(lambda x: re.sub(r'\n\n+', '\n', str(x)))

        # Convert the dataframe to a dictionary
        df = df.to_dict(orient='records')
        v_msg = f" . The dataset from xlsx has {df.shape[0]} records. "
        echo_msg(v_prg, v_stp, v_msg, 2)
    else:
        v_stp = 1.4
        v_msg = "We did not find any rule definition file to be read."
        echo_msg(v_prg, v_stp, v_msg, 0)
        return pd.DataFrame()
    
    if r_std.upper() == "FDA_VR1_6":
        df = df.rename(
            columns={'FDA Validator Rule ID': 'Rule ID'})
    elif r_std.upper() == "SEND_V4_0":
        df = df.rename(
            columns={'CDISC SEND Rule ID': 'Rule ID'})
    elif r_std.upper() == "ADAM_V4_0":
        df = df.rename(
            columns={'Check Number': 'Rule ID'})
    
    return df 


# Test cases
if __name__ == "__main__":
    os.environ["g_msg_lvl"] = "3"
    v_prg = __name__ + "::read_rule_definitions"
    # 1. Test with basic parameters
    v_stp = 1.0
    echo_msg(v_prg, v_stp, "Test Case 01: Basic Parameter", 1)
    df = read_rule_definitions()
    print(df)

    # 2. Test with basic parameters
    v_stp = 2.0
    echo_msg(v_prg, v_stp, "Test Case 01: Basic Parameter", 1)
    df = read_rule_definitions("FDA_VR1_6")
    print(df)

    # 3. Test with basic parameters
    v_stp = 3.0
    echo_msg(v_prg, v_stp, "Test Case 01: Basic Parameter", 1)
    df = read_rule_definitions("SEND_V4_0")
    print(df)

    # 4. Test with basic parameters
    v_stp = 4.0
    echo_msg(v_prg, v_stp, "Test Case 01: Basic Parameter", 1)
    df = read_rule_definitions("ADAM_V4_0")
    print(df)
