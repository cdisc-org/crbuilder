# Purpose: Get json.Executability Check for a rule
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/14/2023 (htu) - ported from proc_rules_sdtm as get_executability module
#   04/13/2023 (htu) - added rule_obj, r_std, r_cst
#    
import os 
from rulebuilder.echo_msg import echo_msg

def get_executability(rule_data, rule_obj: dict = {}, r_std: str = None, r_cst=None):
    v_prg = __name__
    v_stp = 1.0
    v_msg = "Getting executability..."
    echo_msg(v_prg, v_stp, v_msg, 3)

    r_std = os.getenv("r_standard") if r_std is None else r_std
    r_std = r_std.upper()
    # 0: "Fully Executable"
    # 1: "Partially Executable"
    # 2: "Partially Executable - Possible Over-reporting"
    # 3: "Partially Executable - Possible Under-reporting"

    df = rule_data

    r_json = rule_obj.get("json", {}).get("Executability")
    if r_json is not None:
        return r_json

    r_json = "Fully Executable"
    return r_json