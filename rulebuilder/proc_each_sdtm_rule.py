# Purpose: Process a CDISC Core Rule 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/14/2023 (htu) - ported from proc_rules_sdtm as proc_each_sdtm_rule module
#   03/21/2023 (htu) - 
#     1. resolved: 07. Replace Check: Check: null with Check: null
#     2. added v_status and return existing rule if it is Published
#   03/22/2023 (htu) - added logic to use json_exist_rule
#   03/23/2023 (htu) - added test cases and get creator_id from existing rule
#   03/24/2023 (htu) - added json_exist_rule to get_check
#   03/29/2023 (htu) - changed from json_exist_rule to rule_obj
#    

import os 
import json 
from rulebuilder.echo_msg import echo_msg
from rulebuilder.get_existing_rule import get_existing_rule
from rulebuilder.get_rule_guid import get_rule_guid
from rulebuilder.get_core import get_core
from rulebuilder.get_desc import get_desc
from rulebuilder.get_jmsg import get_jmsg
from rulebuilder.get_rtype import get_rtype
from rulebuilder.get_sensitivity import get_sensitivity
from rulebuilder.get_authorities import get_authorities
from rulebuilder.get_scope import get_scope
from rulebuilder.get_executability import get_executability
from rulebuilder.get_check import get_check
# from rulebuilder.rbuilder import RuleBuilder


def proc_each_sdtm_rule (rule_data,rule_tmp, rule_id: str, in_rule_folder,cnt_published):
    v_prg = __name__
    v_stp = 1.0
    echo_msg(v_prg, v_stp, "Setting parameters...", 2)

    # print (f"Rule Data: {rule_data}")
    # print(f"Schema Data: {rule_tmp.keys()}")
    rule_obj = rule_tmp         # it is from existing rule now 
    
    # get existing rule if it exists
    # json_exist_rule = get_existing_rule(rule_id, in_rule_folder)
    v_status = rule_obj.get("json", {}).get("Core", {}).get("Status")
    v_creator_id = rule_obj.get("creator", {}).get("id")
    v_msg = "  Rule_ID: " + str(rule_id) + "; Status: " + str(v_status)
    v_msg += "; CreatorID: " + str(v_creator_id) 
    echo_msg(v_prg, v_stp, v_msg, 3)
    if v_creator_id is not None:
       rule_obj["creator"]["id"] = v_creator_id

    v_autho = get_authorities(rule_data, exist_rule_data=rule_obj)
    if v_status == "Published":
        cnt_published += 1
        rule_obj["json"]["Authorities"] = v_autho
        return rule_obj
    
    # get rule GUID 
    rule_obj["id"] = get_rule_guid(rule_obj)
    
    # format the date and time as a string in the format "2023-03-08T12:00:00Z"
    rule_obj["created"] = rule_obj.get("created")
    # rule_obj["changed"] = json_exist_rule.get("changed") 
    
    # get json Core 
    rule_obj["json"]["Core"] = get_core(
        rule_id, exist_rule_data=rule_obj)
    
    # get json Description
    # print(f"Rule Data: {rule_data.iloc[0]['Condition']}")
    rule_obj["json"]["Description"] = get_desc(
        rule_data, exist_rule_data=rule_obj)

    # get json Message 
    v_jmsg = get_jmsg(rule_data, exist_rule_data=rule_obj)
    rule_obj["json"]["Outcome"] = {"Message": v_jmsg}

    # get json Rule_Type
    rule_obj["json"]["Rule_Type"] = get_rtype(
        rule_data, exist_rule_data=rule_obj)
    
    # get json Sensitivity
    rule_obj["json"]["Sensitivity"] = get_sensitivity(
        rule_data, exist_rule_data=rule_obj)

    # get json Authorities 
    rule_obj["json"]["Authorities"] = v_autho

    # get json Scope       
    rule_obj["json"]["Scope"] = get_scope(rule_data)

    # get json Exeutability 
    rule_obj["json"]["Executability"] = get_executability(rule_data)

    # get checks
    # rule_obj["json"]["Check"] = {"Check": get_check(rule_data)}  
    rule_obj["json"]["Check"] = get_check(
        rule_data, exist_rule_data=rule_obj)

    # print out the result 
    # print(json.dumps(rule_obj, indent=4))
    
    return rule_obj


# Test cases
if __name__ == "__main__":
    os.environ["g_lvl"] = "3"
    v_prg = __name__ + "::proc_each_sdtm_rule"
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

# End of File

