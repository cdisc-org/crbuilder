# Purpose: Output json content to a json and a yaml file 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/14/2023 (htu) - ported from proc_rules_sdtm as output_rule2file module
#   03/15/2023 (htu) - added yaml_data as required input variable 
#   03/21/2023 (htu) - added docstring and test cases  
#   03/22/2023 (htu) - added v_status for file name
#   03/23/2023 (htu) - added echo_msg to display dir and file creation 
#   03/24/2023 (htu) - added "status" to track if a rule a new rule
#   04/04/2023 (htu) - added rename_keys for json file 
#

import os
import json
from rulebuilder.echo_msg import echo_msg
from rulebuilder.rename_keys import rename_keys
from ruamel.yaml import YAML


def output_rule2file(rule_id, json_data, yaml_data, output_dir) -> None:
    """
    Writes YAML and JSON data to files in the specified output directory.

    Args:
    rule_id (str): Identifier for the rule.
    json_data (dict): Dictionary containing JSON data.
    yaml_data (str): String containing YAML data.
    output_dir (str): Path to the output directory where files will be written.

    Returns:
    None.
    """
    v_prg = __name__
    # Form a file name 
    v_status = json_data.get("status")
    json_data.pop("status")             # remove the status fro dict 
    if v_status != "new": 
        v_status = json_data.get("json", {}).get("Core", {}).get("Status")
    yaml_fn = rule_id + "-" + v_status + ".yaml"
    # Write YAML data to a file
    yaml_path = output_dir + '/rules_yaml'
    
    fn_yaml = yaml_path + '/' + yaml_fn
    if not os.path.exists(yaml_path):
        v_stp = 2.1
        v_msg = "Making dir - " + yaml_path 
        echo_msg(v_prg, v_stp, v_msg, 3)
        os.makedirs(yaml_path)
    with open(fn_yaml, 'w') as f:
        v_stp = 2.2
        v_msg = "Writing to: " + fn_yaml  
        echo_msg(v_prg, v_stp, v_msg, 3)
        f.write(yaml_data)

    # Write JSON data to a file
    j_data = json_data
    # we need to change the spaces in keys to underscores 
    rename_keys(j_data, ' ', '_')
    j_data["content"] = yaml_data
    j_path = output_dir + '/rules_json'
    j_fn = rule_id + "-" + v_status + ".json"
    fn_json = j_path + '/' + j_fn
    if not os.path.exists(j_path):
        v_stp = 3.1
        v_msg = "Making dir - " + j_path  
        echo_msg(v_prg, v_stp, v_msg, 3)
        os.makedirs(j_path)
    with open(fn_json, 'w') as f:
        v_stp = 3.2
        v_msg = "Writing to: " + fn_json  
        echo_msg(v_prg, v_stp, v_msg, 3)
        json.dump(j_data, f, indent=4)


if __name__ == '__main__':
    # Test case 1
    rule_id = "test_rule"
    json_data = {"key": "value"}
    yaml_data = "key: value\n"
    output_dir = "./output"
    output_rule2file(rule_id, json_data, yaml_data, output_dir)

    # Test case 2
    rule_id = "another_rule"
    json_data = {"name": "John", "age": 30}
    yaml_data = "name: John\nage: 30\n"
    output_dir = "./output"
    output_rule2file(rule_id, json_data, yaml_data, output_dir)


