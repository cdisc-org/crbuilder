# Purpose: Read from and write to pickle files
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   05/09/2023 (htu) - initial coding 
#

import pickle
import pandas as pd
from rulebuilder.echo_msg import echo_msg

def write_pick(df, f_name): 
    v_prg = "write_pick"
    # Open a file for writing binary data
    with open(f_name, 'wb') as f:
        v_msg = f"  Writing to file: {f_name}..."
        echo_msg(v_prg, 1.0, v_msg, 3)
        # Serialize the object and write it to the file
        pickle.dump(df, f)
# End of write_pick


def read_pick(fn):
    v_prg = "read_pick"
    v_msg = f"  Reading from {fn}..."
    echo_msg(v_prg, 1.0, v_msg, 4)
    # Open the file for reading binary data
    with open(fn, 'rb') as f:
        # Deserialize the object from the file
        data = pickle.load(f)
    # Create a DataFrame from the loaded data
    df = pd.DataFrame(data)
    # print(df) 
    return df   
# End of read_pick

