# Purpose: Process and build a rule in YAML format 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/21/2023 (htu) - initial coding 
#     04. Variable, Condition, and Rule columns should be mapped to comments in
#         the beginning of the YAML file (comments start with #) (see table 
#         above). These won't appear in the json property in the json file 
#         because json does not allow comments.
#   03/24/2023 (htu) - added for loop and commented out and added docstring 
#   03/29/2023 (htu) - used new rename_keys function to reserve comments
#   03/30/2023 (htu) - check and test the commentMap
#   04/05/2023 (htu) - added logic to skip adding comments 
# 


import os
import io
import ruamel.yaml as yaml 
from ruamel.yaml import YAML
from rulebuilder.echo_msg import echo_msg
from rulebuilder.read_rules import read_rules
from rulebuilder.rename_keys import rename_keys 
from rulebuilder.get_creator_id import get_creator_id
from rulebuilder.proc_each_sdtm_rule import proc_each_sdtm_rule

def build_rule_yaml (df_rule_data, js_rule_data):
    """
    Given a pandas DataFrame `df_rule_data` containing information about a SDTM rule,
    and a dictionary `js_rule_data` containing the JSON representation of that rule,
    returns a string containing the YAML representation of the rule, including comments
    based on the values in `df_rule_data`.

    Args:
        df_rule_data (pandas.DataFrame): A DataFrame containing information about a SDTM rule.
        js_rule_data (dict): A dictionary containing the JSON representation of the rule.

    Returns:
        str: A string containing the YAML representation of the rule, including comments
        based on the values in `df_rule_data`.
    """
    y1 = YAML()
    y1.indent(mapping=2, sequence=4, offset=2)
    y1.preserve_quotes = True

    # Only get json for YAML
    df_data = df_rule_data 
    dict_yaml = js_rule_data.get("json")
    # print(f"Dict Keys: {dict_yaml.keys()}")
    # Replace "_" with " " for columns
    d_yaml = dict_yaml
    # print(f"--------------- D_YAML1 ---------------")
    # print(type(d_yaml))
    # y1.dump(d_yaml, sys.stdout)
    rename_keys(d_yaml, '_', ' ')
    # print(f"--------------- D_YAML2 ---------------")
    # print(type(d_yaml))
    # y1.dump(d_yaml, sys.stdout)
    
    s_cmts  = "# Variable: " + df_data.iloc[0]["Variable"] + "\n"
    s_cmts += "# Condition: " + df_data.iloc[0]["Condition"] + "\n"
    s_cmts += "# Rule: " + df_data.iloc[0]["Rule"] + "\n"

    # convert YAML into string
    s_yl = yaml.YAML()
    s_yl.indent(mapping=2, sequence=4, offset=2)

    # Create an in-memory text stream
    text_stream = io.StringIO()

    # Dump the OrderedDict object into the text stream
    s_yl.dump(d_yaml, text_stream)

    # Reset the text stream position
    text_stream.seek(0)

    # Read the YAML-formatted string from the text stream
    ts = text_stream.read()
    s_yaml = ts if ts.startswith("# Variable: ") else s_cmts + ts 
    # print(s_yaml)

    # a_yaml = yaml.dump(d_yaml, default_flow_style=False)
    # print(a_yaml)

    return s_yaml


# Test cases
if __name__ == "__main__":
    # set input parameters 
    os.environ["g_lvl"] = "1"
    v_prg = __name__ + "::proc_sdtm_rules"
    creator_url = "https://rule-editor.cdisc.org/.auth/me"
    r_dir = "/Volumes/HiMacData/GitHub/data/core-rule-builder"
    yaml_file =  r_dir + "/data/target/SDTM_and_SDTMIG_Conformance_Rules_v2.0.yaml"
    existing_rule_dir = r_dir + "/data/output/json_rules1"
    output_dir = r_dir + "/data/output"
    cnt_published = 0
    df_data = read_rules(yaml_file)
    creator_id = get_creator_id(creator_url)
    json_obj = {
        "id": "example-id",
        "created": "2023-03-08T12:00:00Z",
        "changed": "2023-03-08T12:00:00Z",
        "creator": {"id": creator_id},
        "content": "example-content",
        "json": {}
    }
    # 1. Test with basic parameters
    v_stp = 1.0 
    echo_msg(v_prg, v_stp, "Test Case 01: Basic Parameter",1)
    rule_id = "CG0001"
    rule_data = df_data[df_data["Rule ID"] == rule_id]
    a_json = proc_each_sdtm_rule(
        rule_data, json_obj, rule_id, existing_rule_dir, cnt_published)
    build_rule_yaml(rule_data,a_json)

   # 2. Test with basic parameters
    v_stp = 2.0
    echo_msg(v_prg, v_stp, "Test Case 02: Basic Parameter", 1)
    rule_id = "CG0378"
    rule_data = df_data[df_data["Rule ID"] == rule_id]
    a_json = proc_each_sdtm_rule(
        rule_data, json_obj, rule_id, existing_rule_dir, cnt_published)
    build_rule_yaml(rule_data, a_json)
