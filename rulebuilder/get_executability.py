# Purpose: Get json.Executability Check for a rule
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/14/2023 (htu) - ported from proc_rules_sdtm as get_executability module
#    


def get_executability(rule_data):
    # 0: "Fully Executable"
    # 1: "Partially Executable"
    # 2: "Partially Executable - Possible Overreporting"
    # 3: "Partially Executable - Possible Underreporting"

    r_json = "Partially Executable"
    return r_json