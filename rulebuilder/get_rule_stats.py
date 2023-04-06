# Purpose: Read an existing rule into a json data format 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/29/2023 (htu) - initial coding based on get  get_existing_rule 
#    

import os
import sys
import json
import numpy as np
from dotenv import load_dotenv
from rulebuilder.echo_msg import echo_msg
from rulebuilder.read_rules import read_rules
from rulebuilder.get_existing_rule import get_existing_rule


def get_rule_stats(rule_data, in_rule_folder):
    """
    Get statistics about existing rules.

    :param rule_data: The rule definitions in a pandas DataFrame.
    :param in_rule_folder: The folder where the rule files are located.
    :return: A dictionary containing the rule statistics.
    """
    v_prg = __name__
    v_stp = 1.0
    g_lvl = int(os.getenv("g_lvl"))
    v_msg = "Getting existing rule..."
    echo_msg(v_prg, v_stp, v_msg, 3)

    grouped_data = rule_data.groupby("Rule ID")
    listed_files = os.listdir(in_rule_folder)
    r_cnt = {}
    r_cnt["total_rule_records"] = rule_data.shape[0]
    r_cnt["total_files"] = len(listed_files)
    r_cnt["total_rules"] = 0
    r_cnt["rules"] = []
    r_cnt["Version"] = {}
    r_cnt["Class"] = {}
    r_cnt["Domain"] = {}
    r_cnt["Status"] = {}

    for rule_id, group in grouped_data:
        r_cnt["total_rules"] += 1
        v_version = group.iloc[0].get("SDTMIG Version").strip()
        v_class = group.iloc[0].get("Class").strip()
        v_domain = group.iloc[0].get("Domain").strip()

        # r_cnt["rules"].append(rule_id)
        r_cnt["Version"].setdefault(v_version, 0)
        r_cnt["Version"][v_version] += 1
        for i in v_class.split(","):
            k = i.strip()
            r_cnt["Class"].setdefault(k, 0)
            r_cnt["Class"][k] += 1
        for i in v_domain.split(","):
            k = i.strip()
            r_cnt["Domain"].setdefault(k, 0)
            r_cnt["Domain"][k] += 1
        json_data = get_existing_rule(rule_id, in_rule_folder)
        v_status = json_data.get("json", {}).get("Core", {}).get("Status")
        if v_status is None:
            v_status = json_data.get("status")
        r_cnt["Status"].setdefault(v_status, 0)
        r_cnt["Status"][v_status] += 1
        
    return r_cnt


if __name__ == "__main__":
    load_dotenv()
    yaml_file  = os.getenv("yaml_file")
    output_dir = os.getenv("output_dir") 
    rule_dir = os.getenv("existing_rule_dir")
    os.environ["g_lvl"] = "2"
    # Test case 1: Rule exists in the folder and use_yaml_content is True
    rd = read_rules(yaml_file)
    r_1 = get_rule_stats(rd, rule_dir)
    print(f"Result 1:{r_1}")
    r_1 = {k: int(v) if isinstance(v, np.int64) else v for k, v in r_1.items()}
    json.dump(r_1, sys.stdout, indent=4)

# End of Tests