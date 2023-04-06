# Purpose: Rule Builder Class 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/23/2023 (htu) - initial coding
#   

import os
# from abc import ABC, abstractmethod
from abc import ABC
from rulebuilder.echo_msg import echo_msg
from rulebuilder.read_rules import read_rules
from rulebuilder.get_creator_id import get_creator_id
from rulebuilder.proc_sdtm_rules import proc_sdtm_rules
from rulebuilder.proc_each_sdtm_rule import proc_each_sdtm_rule

class RuleBuilder(ABC):
    def __init__(self, 
                 r_dir: str = "/Volumes/HiMacData/GitHub/data/core-rule-builder",
                 i_fn: str = "SDTM_and_SDTMIG_Conformance_Rules_v2.0.yaml",
                 core_base_url: str = "https://raw.githubusercontent.com/cdisc-org/conformance-rules-editor/main/public/schema/CORE-base.json",
                 creator_url: str = "https://rule-editor.cdisc.org/.auth/me"
                ):
        self.r_dir = r_dir
        self.i_fn = i_fn
        self.core_base_url = core_base_url
        self.creator_url = creator_url
        self.yaml_file = r_dir + "/data/target/" + i_fn 
        self.output_dir = r_dir + "/data/output"
        self.existing_rule_dir = r_dir + "/data/output/json_rules1"
        self.stat_cnts = {"total": 0, "renamed": 0, "skipped": 0, "dupped": 0, 
                          "ruleid_used": 0, "coreid_used": 0}
        self.rule_data = read_rules(self.yaml_file)
        creator_id = get_creator_id(creator_url)
        self.rule_obj = {
            "id": "example-id",
            "created": "2023-03-08T12:00:00Z",
            "changed": "2023-03-08T12:00:00Z",
            "creator": {"id": creator_id},
            "content": "example-content",
            "json": {}
        }

    def build_a_rule(self, rule_id):
        df_data = self.rule_data
        rule_data = df_data[df_data["Rule ID"] == rule_id]
        num_of_published = 0
        a_json = proc_each_sdtm_rule(rule_id=rule_id,
            rule_data=rule_data, rule_tmp = self.rule_obj, 
            in_rule_folder = self.existing_rule_dir, cnt_published = num_of_published)
        return a_json 

    def process(self,r_ids:list=["CG0001"]):
        v_prg = __name__

        df_data = self.rule_data
        json_obj = self.rule_obj

        # 1. Test with basic parameters
        v_stp = 1.0
        echo_msg(v_prg, v_stp, "Test Case 01: Basic Parameter", 1)
        proc_sdtm_rules(df_data, json_obj, r_ids,
                        self.existing_rule_dir,  self.output_dir)


# Test cases
if __name__ == "__main__":
    os.environ["g_lvl"] = "3"
    v_prg = __name__ + "::rbuilder"
    # 1. Test with basic parameters
    v_stp = 1.0
    echo_msg(v_prg, v_stp, "Test Case 01: Basic Parameter", 1)
    rb = RuleBuilder()
    # rb.process()
        
    # 2. Test with basic parameters
    v_stp = 1.0
    echo_msg(v_prg, v_stp, "Test Case 02: Basic Parameter", 1)
    rb = RuleBuilder()
    # rb.process(r_ids=["CG0180"])
    # rb.process(r_ids=["CG0196"])
    # rb.process(r_ids=["CG0017"])        # Class with Not (AP) 
    # rb.process(r_ids=["CG0156"])
    # rb.process(r_ids=["CG0165","CG0319"])  # these two do not exist
    # rb.process(r_ids=[])    # to process all 
    rb.process(r_ids=["CG0006"])
# End of File
