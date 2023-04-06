# Purpose: Process a CDISC Core Rule 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/29/2023 (htu) - initial coding based on proc_each_sdtm_rule 
#   04/05/2023 (htu) - 
#     1. added rule_dir, get_db_rule, db_name, ct_name, and r_ids 
#     2. updated calling to get_existing_rule parameters in Step 1.3
#    

import os 
import json 
import sys
import yaml
import ruamel.yaml as ry
from rulebuilder.echo_msg import echo_msg
from rulebuilder.get_existing_rule import get_existing_rule
from rulebuilder.get_rule_guid import get_rule_guid
from rulebuilder.get_core import get_core
from rulebuilder.get_desc import get_desc
from rulebuilder.get_jmsg import get_jmsg
from rulebuilder.get_rtype import get_rtype
from rulebuilder.get_sensitivity import get_sensitivity
# from rulebuilder.get_authorities import get_authorities
from rulebuilder.get_scope import get_scope
from rulebuilder.get_executability import get_executability
from rulebuilder.get_check import get_check
# from rulebuilder.rbuilder import RuleBuilder
from dotenv import load_dotenv
from io import StringIO

from rulebuilder.read_rules import read_rules
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap
from rulebuilder.get_yaml_authorities import get_yaml_authorities

def proc_each_yaml (rule_id:str=None,rule_data = None, rule_obj = None,
                    rule_dir:str =None, r_ids = None, 
                    get_db_rule:int = 0,
                    db_name: str = None, ct_name:str=None):
    v_prg = __name__
    
    v_stp = 1.0
    echo_msg(v_prg, v_stp, "Setting parameters...", 1)
    load_dotenv()
    yaml_file = os.getenv("yaml_file")
    if rule_dir is None: 
        rule_dir = os.getenv("existing_rule_dir")
    g_lvl = int(os.getenv("g_lvl"))

    # 1.1 check parameters 
    v_stp = 1.1
    if rule_id is None:
        v_msg = "Rule ID is required."
        echo_msg(v_prg, v_stp, v_msg, 2)
        return None
    else:
        v_msg = "Rule ID - " + rule_id + " will be processed."
        echo_msg(v_prg, v_stp, v_msg, 2)

    # 1.2 get rule data
    v_stp = 1.2
    if rule_data is None:
        v_stp = 1.2
        v_msg = "Get Rule Data from " + yaml_file 
        echo_msg(v_prg, v_stp, v_msg, 2)
        rule_data = read_rules(yaml_file)
        rule_data = rule_data[rule_data["Rule ID"]==rule_id]
        rule_data = rule_data.reset_index(drop=True)
    # print(f"Rule Data: {rule_data}")

    # 1.3 get rule object - existing rule 
    v_stp = 1.3
    v_msg = "Get rule object from rule folder: " + rule_dir
    echo_msg(v_prg, v_stp, v_msg, 2)

    y1 = YAML()
    y1.indent(mapping=2, sequence=4, offset=2)
    y1.preserve_quotes = True

    # print (f"Rule Data: {rule_data}")
    # print(f"Schema Data: {rule_tmp.keys()}")
    if rule_obj is None: 
        rule_obj = get_existing_rule(rule_id, in_rule_folder=rule_dir, 
                                     get_db_rule=get_db_rule, r_ids=r_ids,
                                     db_name=db_name,ct_name=ct_name,
                                     use_yaml_content=False)
        
    if g_lvl >= 5:
        print("---------- Existing Rule Object ----------")
        # yaml_loader.dump(y_content, sys.stdout)
        # print(rule_obj)
        yaml.dump(rule_obj, sys.stdout)
        print("---------- Existing Rule Object(json.Check) ----------")
        y1.dump(rule_obj.get("json",{}).get("Check"), sys.stdout)
    
    # 1.4 get rule parameters
    v_stp = 1.4
    v_msg = "Get rule parameters"
    echo_msg(v_prg, v_stp, v_msg, 2)

    v_status = rule_obj.get("json", {}).get("Core", {}).get("Status")
    v_creator_id = rule_obj.get("creator", {}).get("id")
    v_msg = "  Rule_ID: " + str(rule_id) + "; Status: " + str(v_status)
    v_msg += "; CreatorID: " + str(v_creator_id) 
    echo_msg(v_prg, v_stp, v_msg, 3)
    if v_creator_id is not None:
       rule_obj["creator"]["id"] = v_creator_id
    
    # 1.5 get rule content
    v_stp = 1.5
    v_msg = "Get rule content"
    echo_msg(v_prg, v_stp, v_msg, 3)

 
    # c1 = yaml.safe_load(rule_obj.get("content"))
    c0 = rule_obj.get("content")
    c1 = {} if c0 is None else y1.load(c0)
    
    if g_lvl >= 5:
        print("---------- Existing Rule Content (C1) ----------")
        # yaml_loader.dump(y_content, sys.stdout)
        # print(c1)
        y1.dump(c1, sys.stdout)

    if c1 is not None:  
        v_stp = 1.51
        v_msg = "Rule Obj Conent is not None"
        echo_msg(v_prg, v_stp, v_msg, 3)
        # y2 = y1.load(c1)
        y2 = c1
    else:
        v_stp = 1.52
        v_msg = "Rule Obj Conent is None"
        echo_msg(v_prg, v_stp, v_msg, 3)
        y2 = CommentedMap()
    y2 = CommentedMap() if y2 is None else y2
    y_status = y2.get("Core",{}).get("Status")
    if v_status != y_status:
        v_stp = 1.6
        v_msg = "Json Core Status (" + str(v_status) + " != " + str(y_status)
        echo_msg(v_prg, v_stp, v_msg, 3)

    if g_lvl >= 5:
        print("---------- Existing Rule Content (Y2) ----------")
        # yaml_loader.dump(y_content, sys.stdout)
        # print(c1)
        y1.dump(y2, sys.stdout)

    # yt = ry.dump_all(y2, Dumper=ry.RoundTripDumper)

    y_autho = get_yaml_authorities(rule_data,y2)
    if g_lvl >= 5:
        print("---------- Rule Authorities (Y_AUTHO) ----------")
        # yaml_loader.dump(y_content, sys.stdout)
        # print(c1)
        y1.dump(y_autho, sys.stdout)
        print("---------- Rule Authorities (json.Check) ----------")
        y1.dump(y2.get("Check"), sys.stdout)

    # yaml_text = ry.dump_all(y_autho, Dumper=ry.RoundTripDumper)
    # print(f"YAML AUTHO: {yaml_text}")

    y2["Authorities"] = y_autho 

    # v_autho = get_authorities(rule_data, exist_rule_data=rule_obj)
    # if v_status == "Published":
    #     cnt_published += 1
    #     rule_obj["json"]["Authorities"] = v_autho
    #     return rule_obj
    
    # get rule GUID 
    rule_obj["id"] = get_rule_guid(rule_obj)
    
    # format the date and time as a string in the format "2023-03-08T12:00:00Z"
    rule_obj["created"] = rule_obj.get("created")
    # rule_obj["changed"] = json_exist_rule.get("changed") 
    
    # # get json Core 
    # rule_obj["json"]["Core"] = get_core(
    #      rule_id, exist_rule_data=rule_obj)
    y2["Core"] = get_core(
         rule_id, exist_rule_data=rule_obj)

    # # get json Description
    # # print(f"Rule Data: {rule_data.iloc[0]['Condition']}")
    # rule_obj["json"]["Description"] = get_desc(
    #     rule_data, exist_rule_data=rule_obj)
    y2["Description"] = get_desc(
         rule_data, exist_rule_data=rule_obj)

    # # get json Message 
    v_jmsg = get_jmsg(rule_data, exist_rule_data=rule_obj)
    # rule_obj["json"]["Outcome"] = {"Message": v_jmsg}
    y2["Outcome"] = {"Message": v_jmsg}

    # # get json Rule_Type
    # rule_obj["json"]["Rule_Type"] = get_rtype(
    #     rule_data, exist_rule_data=rule_obj)
    y2["Rule_Type"] = get_rtype(
         rule_data, exist_rule_data=rule_obj)

    # # get json Sensitivity
    # rule_obj["json"]["Sensitivity"] = get_sensitivity(
    #     rule_data, exist_rule_data=rule_obj)
    y2["Sensitivity"] = get_sensitivity(
        rule_data, exist_rule_data=rule_obj)

    # # get json Authorities 
    # rule_obj["json"]["Authorities"] = v_autho

    # # get json Scope       
    # rule_obj["json"]["Scope"] = get_scope(rule_data)
    y2["Scope"] = get_scope(rule_data)

    # # get json Exeutability 
    # rule_obj["json"]["Executability"] = get_executability(rule_data)
    y2["Executability"] = get_executability(rule_data)

    # # get checks
    # # rule_obj["json"]["Check"] = {"Check": get_check(rule_data)}  
    # rule_obj["json"]["Check"] = get_check(
    #     rule_data, exist_rule_data=rule_obj)
    if g_lvl >= 5:
        print("---------- Final check 1 (json.Check) ----------")
        y1.dump(y2.get("Check"), sys.stdout)

    y2["Check"] = get_check(
        rule_data, exist_rule_data=y2)


    # # print out the result 
    # # print(json.dumps(rule_obj, indent=4))
    rule_obj["json"] = y2
    # rule_obj["content"] = ry.dump(y2, Dumper=ry.RoundTripDumper)

    # use StringIO to preserve the comment and load y2 to content 
    content = StringIO()
    y1.dump( y2, content,)
    rule_obj["content"] = content.getvalue()
    content.close()

    # rule_obj["content"] = ry.round_trip_load(y2)
    if g_lvl >= 5:
        print("---------- Final check 2 (json.Check) ----------")
        y1.dump(y2.get("Check"), sys.stdout)

    return rule_obj 


# Test cases
if __name__ == "__main__":
    os.environ["g_lvl"] = "3"
    v_prg = __name__ + "::proc_each_yaml"
    # 1. Test with basic parameters
    v_stp = 1.0
    echo_msg(v_prg, v_stp, "Test Case 01: Basic Parameter", 1)
    # rb = RuleBuilder()
    # r_json = rb.build_a_rule("CG0001")
    # json.dump(r_json,indent=4)

    # # 2. Test with basic parameters
    # v_stp = 1.0
    # echo_msg(v_prg, v_stp, "Test Case 02: Basic Parameter", 1)
    # rb = RuleBuilder()
    # r_json = rb.build_a_rule("CG0156")
    # json.dump(r_json, indent=4)
    jd = {
        "id": "9959136a-523f-4546-9520-ffa22cda8867",
        "changed": "2023-03-19T20:59:01.002Z",
        "content": "Check:\n  all:\n    - name: --DY\n      operator: non_empty\n    - name: --DTC\n      operator: is_complete_date\n    - name: RFSTDTC\n      operator: is_complete_date\n    - name: --DY # checking failure criteria\n      operator: not_equal_to\n      value: $val_dy # this is created in the operations statement that follows\nOperations:\n  - name: RFSTDTC\n    domain: DM\n    operator: dy # does dy op take into acct +1?\n    id: $val_dy # ex in devops 2538 used -- but that threw an error\nCore:\n  Id: CDISC.SDTMIG.CG0006\n  Version: '1'\n  Status: Draft\nDescription: Raise an error when --DY is not calculated as per the study day algorithm\n  as a non-zero integer value when the date portion of --DTC is complete and the date\n  portion of DM.RFSTDTC is a complete date AND --DY is not empty\nMatch Datasets:\n  - Name: DM\n    Keys:\n      - USUBJID\nOutcome:\n  Message: --DY is not calculated correctly even though the date portion of --DTC\n    is complete, the date portion of DM.RFSTDTC is a complete date, and --DY is not\n    empty.\n  Output Variables:\n    - --DY\n    - --DTC\n    - RFSTDTC\nRule Type: Record Data\nSensitivity: Record\nExecutability: Fully Executable\nAuthorities:\n  - Organization: CDISC\n    Standards:\n      - Name: SDTMIG\n        Version: '3.4'\n        References:\n          - Origin: SDTM and SDTMIG Conformance Rules\n            Rule Identifier:\n              Id: CG0006\n              Version: '2'\n            Version: '2.0'\n            Citations:\n              - Cited Guidance: The Study Day value is incremented by 1 for each date\n                  following RFSTDTC. Dates prior to RFSTDTC are decreased by 1, with\n                  the date preceding RFSTDTC designated as Study Day -1 (there is\n                  no Study Day 0)....All Study Day values are integers. Thus to calculate\n                  Study Day, --DY = (date portion of --DTC) - (date portion of RFSTDTC)\n                  + 1 if --DTC is on or after RFSTDTC, --DY = (date portion of --DTC)\n                  - (date portion of RFSTDTC) if --DTC precedes RFSTDTC. This algorithm\n                  should be used across all domains.\n                Document: SDTMIG v3.4\n                Section: 4.4.4\nScope:\n  Classes:\n    Include:\n      - ALL\n  Domains:\n    Include:\n      - ALL\n",
        "created": "2022-04-11T04:35:12Z",
        "creator": {
            "id": "6ae4d700-ba2d-4fad-9812-f33baa02bc82"
        },
    }
    y = YAML()
    y.indent(mapping=2, sequence=4, offset=2)
    y.preserve_quotes = True
    d1 = y.load(jd.get("content"))
    y1 = d1.get("Operations")
    y2 = d1["Authorities"]
    y2[0]["Standards"][0].yaml_add_eol_comment(
        "This is SDTMIG 3.4", 'Version')
    # y.dump(d1, sys.stdout)
    # y.dump(y2[0]["Standards"][0], sys.stdout)

    rule_id = "CG0006888"
    # y3 = proc_each_yaml(rule_id)


    rule_id = "CG0006"
    y3 = proc_each_yaml(rule_id)
    y.dump(y3, sys.stdout)
    # y4 = y.load(y3["content"])

    #y.dump(y4, sys.stdout)

# End of File

