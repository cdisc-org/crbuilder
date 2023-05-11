# Purpose: Check and create a directory 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   04/19/2023 (htu) - extracted from create_log_dir 
#

import os 
from rulebuilder.echo_msg import echo_msg

def create_a_dir (v_prg, v_stp, v_dir, wrt2log:int=1):
    if not os.path.exists(v_dir):
        v_msg = f"Dir does not exist: {v_dir}"
        echo_msg(v_prg, v_stp, v_msg, 3)
        if wrt2log > 0:
            v_msg = f"Making dir - {v_dir}"
            echo_msg(v_prg, v_stp, v_msg, 3)
            os.makedirs(v_dir)
