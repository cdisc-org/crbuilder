# Purpose: Rule Builder Class 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/23/2023 (htu) - initial coding
#   04/06/2023 (htu) - added r_standard, rule_files and read_rule_definitions 
#   04/07/2023 (htu) - added r_standard in process method 
#   

import os
import re 
# from abc import ABC, abstractmethod
from abc import ABC
import pickle 
import pandas as pd 
import ruamel.yaml as yaml
from dotenv import load_dotenv
from rulebuilder.echo_msg import echo_msg
from rulebuilder.proc_rules import proc_rules
from rulebuilder.proc_each_sdtm_rule import proc_each_sdtm_rule

class RuleBuilder(ABC):
    def __init__(self, 
                 r_standard: str = "SDTM_V2_0",
                 r_dir: str = None,
                 i_fn: str = "SDTM_and_SDTMIG_Conformance_Rules_v2.0.yaml",
                 core_base_url: str = "https://raw.githubusercontent.com/cdisc-org/conformance-rules-editor/main/public/schema/CORE-base.json",
                 creator_url: str = "https://rule-editor.cdisc.org/.auth/me"
                ):
        """
        Initialize a new instance of the RuleBuilder class.

        Args:
            r_standard (str): The name of the standard for which the rule definitions are being processed (default: "SDTM_V2_0").
            r_dir (str): The directory where the rule definitions are stored.
            i_fn (str): The name of the file that contains the rule definitions for the specified standard.
            core_base_url (str): The URL for the CORE base JSON schema.
            creator_url (str): The URL for the rule editor.
        """
        rule_files = {
            "FDA_VR1_6": {"file_name": 'FDA_VR_v1.6.xlsx',
                          "rule_sheet": 'FDA Validator Rules v1.6',
                          "yaml":"fda_vr1_6.yaml", "pick":"fda_vr1_6.pick"},
            "SDTM_V2_0": {"file_name": 'SDTM_and_SDTMIG_Conformance_Rules_v2.0.xlsx',
                          "rule_sheet": 'SDTMIG Conformance Rules v2.0'},
            "SEND_V4_0": {"file_name": 'SEND_Conformance_Rules_v4.0.xlsx',
                          "rule_sheet": 'v3.0_v3.1_v3.1.1_DARTv1.1'},
            "ADAM_V4_0": {"file_name": 'ADaM_Conformance_Rules_v4.0.xlsx',
                          "rule_sheet": 'ADaM Conformance Rules v4.0'},

        } 
        self.rule_files = rule_files 
        v_prg = __name__ + ".init"
        v_stp = 1.0
        if r_standard is None:
            v_stp = 1.1
            v_msg = "Standard name is not provided."
            echo_msg(v_prg, v_stp, v_msg, 0)
            return 
        r_std = r_standard.upper()
        self.r_standard = r_std
        v_msg = f"Initializing rule builder for {r_std}..."
        echo_msg(v_prg, v_stp, v_msg, 1)

        v_stp = 1.2 
        load_dotenv()
        r_dir = os.getenv("r_dir") if r_dir is None else r_dir
        self.r_dir = r_dir
        self.sheet_name = rule_files.get(r_std,{}).get("rule_sheet")
        self.fn_xlsx = rule_files.get(r_std, {}).get("file_name")
        self.fn_yaml = r_std.lower() + ".yaml"
        self.fn_pick = r_std.lower() + ".pick"
        self.i_fn = self.fn_xlsx                            # keep for compatability
        self.fp_xlsx = r_dir + "/data/source/xlsx/" + self.fn_xlsx
        self.fp_yaml = r_dir + "/data/source/yaml/" + self.fn_yaml
        self.fp_pick = r_dir + "/data/source/pick/" + self.fn_pick

        self.core_base_url = core_base_url
        self.creator_url = creator_url

        self.yaml_file = r_dir + "/data/target/" + i_fn 
        self.output_dir = r_dir + "/data/output"
        self.existing_rule_dir = r_dir + "/data/output/orig_rules"
        self.stat_cnts = {"total": 0, "renamed": 0, "skipped": 0, "dupped": 0, 
                          "ruleid_used": 0, "coreid_used": 0}
        v_stp = 1.3 
        # self.rule_data = read_rules(self.yaml_file)
        self.rule_data = self.read_rule_definitions()
  
    def read_rule_definitions (self):
        """
        Read the rule definitions for the specified standard from a pickled file, a YAML file, or the original Excel file.

        Returns:
            A pandas DataFrame containing the rule definitions.
        """
        v_prg = __name__ + ".read_rule_definition"
        v_stp = 1.0
        v_msg = f"Reading rule definition for {self.r_standard}..."
        echo_msg(v_prg, v_stp, v_msg, 1)
        # 1.1 read from a pickled file 
        fn = self.fp_pick
        v_stp = 1.1
        if os.path.isfile(fn):
            v_msg = f" . from {fn}..."
            echo_msg(v_prg, v_stp, v_msg, 2)
            with open(fn, 'rb') as f:
                # Deserialize the object from the file
                data = pickle.load(f)
            # Create a DataFrame from the loaded data
            df = pd.DataFrame(data)
            v_msg = f" . The dataset has {df.shape[0]} records. "
            echo_msg(v_prg, v_stp, v_msg, 2)
            return df 
        else:
            v_msg = f" . Could not find pickled file: {fn}."
        echo_msg(v_prg, v_stp, v_msg, 2)

        
        # 1.2 read from a yaml file 
        fn = self.fp_yaml 
        v_stp = 1.2
        if os.path.isfile(fn):
            v_msg = f" . from {fn}..."
            echo_msg(v_prg, v_stp, v_msg, 2)
            with open(fn, "r") as f:
                data = yaml.safe_load(f)
            # Create DataFrame from YAML data
            df = pd.DataFrame(data)
            v_msg = f" . The dataset has {df.shape[0]} records. "
            echo_msg(v_prg, v_stp, v_msg, 2)
            return df
        else:
            v_msg = f" . Could not find yaml file: {fn}."
        echo_msg(v_prg, v_stp, v_msg, 2)
        
        # 1.3 read from the original xlsx file 
        fn = self.fp_xlsx
        v_stp = 1.3
        if os.path.isfile(fn):
            v_msg = f" . from {fn}..."
            echo_msg(v_prg, v_stp, v_msg, 2)

            s_name = self.sheet_name
            if fn.startswith('FDA'):
                df = pd.read_excel(fn, sheet_name=s_name,
                           header=1, engine='openpyxl')
            else:
                df = pd.read_excel(fn, sheet_name=s_name, engine='openpyxl')
            # remove newline breaks in column names
            df.columns = df.columns.str.replace('\n', '')
            df.columns = df.columns.str.lstrip()           # remove leading spaces
            df.columns = df.columns.str.rstrip()           # remove trailing spaces
            # remove newline breaks in data
            df = df.fillna('')                              # fill na with ''
            df = df.applymap(lambda x: re.sub(r'\n\n+', '\n', str(x)))

            # Convert the dataframe to a dictionary
            df = df.to_dict(orient='records')
            v_msg = f" . The dataset has {df.shape[0]} records. "
            echo_msg(v_prg, v_stp, v_msg, 2)
            return df
        else:
            v_msg = f" . Could not find xlsx file: {fn}."
        echo_msg(v_prg, v_stp, v_msg, 2)
        
        v_msg = "We did not find any rule definition file to be read."
        echo_msg(v_prg, v_stp, v_msg, 0)
        return pd.DataFrame()


    def build_a_rule(self, rule_id):
        """
        Build a single rule JSON object for the specified rule ID.

        Args:
            rule_id (str): The ID of the rule to build.

        Returns:
            A JSON object representing the specified rule.
        """
        df_data = self.rule_data
        rule_data = df_data[df_data["Rule ID"] == rule_id]
        num_of_published = 0
        a_json = proc_each_sdtm_rule(rule_id=rule_id,
            rule_data=rule_data, rule_tmp = self.rule_obj, 
            in_rule_folder = self.existing_rule_dir, cnt_published = num_of_published)
        return a_json 

    def process(self, r_standard: str = None, 
                r_ids=None, s_version: list = [],
                s_class: list = [], s_domain: list = [],           
                wrt2log: int = 1, pub2db: int = 0,
                get_db_rule: int = 1,
                db_name: str = None, ct_name: str = "core_rules_dev"
                ):
        """
        Process the rule definitions for the specified standard.

        Args:
            r_standard (str): The name of the standard for which the rule definitions are being processed (default: "SDTM_V2_0").
            r_ids (list): A list of rule IDs to include (default: None).
            s_version (list): A list of versions to include (default: []).
            s_class (list): A list of classes to include (default: []).
            s_domain (list): A list of domains to include (default: []).
            wrt2log (int): A flag indicating whether to write output to a log file (default: 1).
            pub2db (int): A flag indicating whether to publish rules to a database (default: 0).
            get_db_rule (int): A flag indicating whether to get rules from a database (default: 1).
            db_name (str): The name of the database to use (default: None).
            ct_name (str): The name of the container to use (default: "core_rules_dev").
        """
        v_prg = __name__ + ".process"
        v_stp = 1.0
        v_msg = "Processing CORE rule definitions..."
        if r_standard is None:
            r_standard = self.r_standard
        if r_standard is None:
            v_stp = 1.1
            v_msg = "No rule standard is provided."
            echo_msg(v_prg, v_stp, v_msg, 0)
            return
        if r_ids is None:
            v_stp = 1.2
            v_msg = "No rule id is provided. "
            echo_msg(v_prg, v_stp, v_msg, 0)
            return 
        if r_ids == "ALL":
            r_ids = [] 

        v_stp = 1.3
        v_msg = "Checking parameters..." 
        echo_msg(v_prg, v_stp, v_msg, 2)

        if db_name is None:
            db_name = os.getenv("DEV_COSMOS_DATABASE")
        if ct_name is None:
            ct_name = os.getenv("DEV_COSMOS_CONTAINER")


        # 2. Call to the rule processor 
        v_stp = 2.0
        echo_msg(v_prg, v_stp, f"Calling to proc_rules", 1)
        load_dotenv() 
        proc_rules(r_standard = r_standard,
                    df_data=self.rule_data, 
                    in_rule_folder=self.existing_rule_dir,
                    out_rule_folder=self.output_dir,
                    rule_ids=r_ids, 
                    s_version=s_version,
                    s_class=s_class, 
                    s_domain=s_domain,
                    wrt2log=wrt2log, 
                    pub2db=pub2db,
                    get_db_rule=get_db_rule,
                    db_name=db_name, ct_name=ct_name
                    )


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
    # rb = RuleBuilder()
    # rb.process(r_ids=["CG0180"])
    # rb.process(r_ids=["CG0196"])
    # rb.process(r_ids=["CG0017"])        # Class with Not (AP) 
    # rb.process(r_ids=["CG0156"])
    # rb.process(r_ids=["CG0165","CG0319"])  # these two do not exist
    # rb.process(r_ids=[])    # to process all 
    rb.process(r_ids=["CG0006"])
# End of File
