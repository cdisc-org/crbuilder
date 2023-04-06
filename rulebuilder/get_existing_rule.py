# Purpose: Read an existing rule into a json data format 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/14/2023 (htu) - ported from proc_rules_sdtm as get_existing_rule module
#   03/23/2023 (htu) - added "status" 
#   03/29/2023 (htu) - added use_yaml_content for json 
#   03/30/2023 (htu) - added echo_msg 
#   04/04/2023 (htu) - added creator.id for new rule
#   04/05/2023 (htu) - added db_cfg, db_name, ct_name and r_ids and reading from a DB
#    

import os
import sys
from datetime import datetime, timezone
import uuid
import json 
from ruamel.yaml import YAML, parser, scanner
# from io import StringIO
from transformer.transformer import Transformer
from yaml import safe_load
from dotenv import load_dotenv
from rulebuilder.echo_msg import echo_msg
from rulebuilder.get_db_cfg import get_db_cfg
from rulebuilder.read_a_rule import read_a_rule
from rulebuilder.read_db_rule import read_db_rule
from rulebuilder.get_doc_stats import get_doc_stats


def get_existing_rule(rule_id, in_rule_folder, 
                      get_db_rule:int = 0, db_cfg = None, r_ids = None, 
                      db_name:str=None, ct_name:str=None,
                      use_yaml_content:bool=True):
    """
    Get an existing rule based on the given rule_id from a specified folder.
    * If the rule file is found, it returns a dictionary containing the 
      rule's metadata.
    * If the rule file is not found, it creates a new rule with a UUID and 
      sets its status to "new".


    :param rule_id: The ID of the rule to find.
    :param in_rule_folder: The folder where the rule files are located.
    :param use_yaml_content: A boolean indicating whether to process and 
           return the YAML content of the rule.
    :return: A dictionary containing the rule's metadata and, 
             if use_yaml_content is True, the processed YAML content.
    """
    v_prg = __name__
    v_stp = 1.0
    g_lvl = int(os.getenv("g_lvl"))
    v_msg = "Checking input parameters..."
    echo_msg(v_prg, v_stp, v_msg, 2)

    # 2 get json data either from db or rule folder
    v_stp = 2.0
    if get_db_rule == 1: 
        v_stp = 2.1
        v_msg = f"Getting rule doc from {ct_name}.{db_name}..."
        echo_msg(v_prg, v_stp, v_msg, 2)
        json_data = read_db_rule(rule_id=rule_id, db_cfg=db_cfg,r_ids=r_ids,
                                 db_name=db_name,ct_name=ct_name)
    else: 
        v_stp = 2.2
        v_msg = f"Getting rule file from {in_rule_folder}..."
        echo_msg(v_prg, v_stp, v_msg, 2)
        json_data = read_a_rule(rule_id=rule_id,rule_dir=in_rule_folder)

    rule_guid = json_data.get('id')

    v_stp = 3.0
    v_msg = "Building r_json object..."
    echo_msg(v_prg, v_stp, v_msg, 4)

    now_utc = datetime.now(timezone.utc)
    default_id = "dd0f9aa3-68f9-4825-84a4-86c8303daaff"
    if rule_guid is None:
        v_stp = 3.1 
        v_msg = "Did not find GUID for the rule."
        echo_msg(v_prg, v_stp, v_msg, 5)
        r_json = {
            "id": str(uuid.uuid4()),
            "created": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "changed": now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "creator": {
                "id": default_id
            },
            "json": {},
            "status": "new"
        }
    else:
        v_stp = 3.2 
        v_msg = "Found GUID for the rule."
        echo_msg(v_prg, v_stp, v_msg, 5)
        r_json = json_data
        r_json["changed"] = now_utc.strftime("%Y-%m-%dT%H:%M:%SZ")
        r_json["status"] = "exist"
        r1 = r_json.get("json")
        if r1 is None or r1 == "null":
            r_json["json"] = {}
    
    yaml_loader = YAML()
    yaml_loader.indent(mapping=2, sequence=4, offset=2)
    yaml_loader.preserve_quotes = True

    v_c = r_json.get("content")
    v_stp = 3.3 
    v_msg = "V Content Obj Type: " + str(type(v_c)) 
    echo_msg(v_prg, v_stp, v_msg, 5)

    y_content = {} if v_c is None else yaml_loader.load(v_c)
    v_stp = 3.4 
    v_msg = "Y Content Obj Type: " + str(type(y_content))
    echo_msg(v_prg, v_stp, v_msg, 5)

    if g_lvl >= 5:
        print("========== Rule Content ==========")
        yaml_loader.dump(y_content, sys.stdout)

    if rule_guid is not None and use_yaml_content: 
        v_stp = 3.3
        v_msg = "Use YAML Content to build rule object."
        echo_msg(v_prg, v_stp, v_msg, 5)
        # print(f"JSON Content: {r_json['content']}")

        y_content = yaml_loader.load(r_json["content"]) or {}
         
        r_json["json"] = Transformer.spaces_to_underscores(
            safe_load(r_json["content"]))

    return r_json 


if __name__ == "__main__":
    load_dotenv()
    yaml_file  = os.getenv("yaml_file")
    output_dir = os.getenv("output_dir") 
    rule_dir = os.getenv("existing_rule_dir")
    os.environ["g_lvl"] = "5"
    # Test case 1: Rule exists in the folder and use_yaml_content is True
    rule_id = "CG0063"
    # r_1 = get_existing_rule(rule_id, rule_dir)
    # print(f"Result 1:")
    # json.dump(r_1,sys.stdout,indent=4)


    # Test case 2: Rule exists in the folder and use_yaml_content is False
    db = "library"
    ct = 'core_rules_dev'
    r_2 = get_existing_rule(
         rule_id, rule_dir, use_yaml_content=False,get_db_rule=1, db_name=db, ct_name=ct )
    # print(f"Result 2: {r_2}")
    json.dump(r_2, sys.stdout, indent=4)

    # Test case 3: Rule does not exist in the folder
    # non_existing_rule_id = "non_existing_rule"
    # r_3 = get_existing_rule(non_existing_rule_id, rule_dir)
    # print(f"Result 3: {r_3}")
