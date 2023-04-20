# Purpose: Run all the rules for all the standards in a batch 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   04/19/2023 (htu) - initial coding 
#
import os
from rulebuilder.copy_rules import copy_rules
from rulebuilder.delete_rules import delete_rules
from rulebuilder.create_log_dir import create_log_dir

if __name__ == "__main__":
    # set input parameters
    os.environ["g_msg_lvl"] = "3"
    os.environ["write2log"] = "0"
    v_prg = __name__ + "::batch_run"
    r_str = "ALL"
    t_db = 'library'
    # t_ct = 'editor_rules_dev'
    t_ct = "core_rules_dev"
    delete_rules(r_str, ct_name=t_ct, db_name=t_db, write2file=1)

    f_db = 'library'
    t_db = 'library'
    f_ct = 'editor_rules_dev_20230411'
    # t_ct = 'editor_rules_dev'
    t_ct = "core_rules_dev"
    copy_rules(r_str, f_ct, t_ct, f_db, t_db, write2file=1)
