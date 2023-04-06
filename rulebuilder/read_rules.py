# Purpose: Read in all the rule definitions
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/17/2023 (htu) - Extracted out from sdtmrulebuilder 
#                    - added docstring and echo_msg 
#

import os 
import pandas as pd
import ruamel.yaml as yaml
from dotenv import load_dotenv
from rulebuilder.echo_msg import echo_msg


def read_rules(yaml_file=None):
    """
    ==========
    read_rules
    ==========
    This method reads rule definitions in a YAML file.

    Parameters:
    -----------
    yaml_file: str
        A yaml file name with full path 

    returns
    -------
        df_yaml:  content from the YAML file in data frame

    """
    v_prg = __name__ + "::read_rules"
    v_step = 1.0
    if yaml_file is None:
       v_step = 1.1
       echo_msg(v_prg,v_step, "No YAML file name is provided.",0)
       return None
    if not os.path.isfile(yaml_file):
       v_step = 1.2
       echo_msg(v_prg, v_step, "File - " + yaml_file + " does not exist.", 0)
       return None
    v_msg = "Reading from " + yaml_file + "..."
    echo_msg(v_prg, v_step, v_msg, 2)
    # 1.2 Read rule definition file (YAML file)
    with open(yaml_file, "r") as f:
        yaml_data = yaml.safe_load(f)
    # print(yaml_data[0])

    # Create DataFrame from YAML data
    df_yaml = pd.DataFrame(yaml_data)
    return df_yaml 


# Test cases
if __name__ == "__main__":
    os.environ["g_lvl"] = "3"
    # 1. Test with basic parameters
    v_prg = __name__ + "::read_rules"
    v_step = 1.0
    echo_msg(v_prg, v_step, "Test Case 01: basic parameters", 1)
    rr = read_rules()
    print(f"Expected: {rr}\n")
    # Expected display: No YAML file name is provided.
    # Expected output: None 

    # 2. Test reading a file 
    v_step = 2.0
    echo_msg(v_prg, v_step, "Test Case 02: read a file", 1)
    load_dotenv()
    yaml_file = os.getenv("yaml_file")
    rr = read_rules(yaml_file) 
    print(rr.iloc[0])
    print(f"Shape[0]: {rr.shape[0]}")
    rr = rr[rr["SDTMIG Version"].isin(["3.2", "3.3"]) and rr["Class"].isin(["EVT"])]
    print(f"Shape[0]: {rr.shape[0]}") 
    # print the first row in the data frame 