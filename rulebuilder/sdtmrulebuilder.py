# Purpose: Build a CDISC Core Rule based on rule definition in a standard 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/14/2023 (htu) - initial coding 
#   03/15/2023 (htu) - change "import yaml" to "import ruamel.yaml as yaml"
#   03/16/2023 (htu) - extracted get_creator_id out and created _get_creator_id 
#                    - extracted get_schema out and created _get_schema 
#   03/17/2023 (htu) - extracted read_rules out and created _read_rules 
#   

import ssl
from .proc_sdtm_rules   import proc_sdtm_rules
from .get_creator_id    import get_creator_id
from .get_schema        import get_schema
from .read_rules        import read_rules


class SDTMRuleBuilder:
    def __init__(self,
                yaml_file="./data/target/SDTM_and_SDTMIG_Conformance_Rules_v2.0.yaml",
                core_base_url = "https://raw.githubusercontent.com/cdisc-org/conformance-rules-editor/main/public/schema/CORE-base.json",
                creator_url="https://rule-editor.cdisc.org/.auth/me",
                existing_rule_dir="./data/output/json_rules1",
                output_dir="./data/output"
                 ):
        """
        ==========
        __init__
        ==========
        This method initializes the class.

        Parameters:
        -----------
        yaml_file: str
            a YAML file containing rule definitions 
        core_base_url: str
            a URL pointing to Core Rule Schema  
        creator_url: str
            a URL pointing to a authoritication page to get user GUID
        existing_rule_dir: str
            a folder containing all the existing rules
        output_dir: str
            a folder where the new or updated rules will be output to
        
        returns
        -------
            None 

        Raises
        ------
        ValueError
            None
        
        """
        self.yaml_file          = yaml_file 
        self.core_base_url      = core_base_url
        self.creator_url        = creator_url
        self.existing_rule_dir  = existing_rule_dir
        self.output_dir         = output_dir

    def _get_schema(self,base_url=None):
        if base_url is None:
            base_url = self.core_base_url
        return get_schema(base_url)
        

    def _get_creator_id(self,creator_url=None):
        if creator_url is None: 
            creator_url = self.creator_url
        return get_creator_id(creator_url)
    

    def _read_rules(self, yaml_file=None):
        if yaml_file is None:
            yaml_file = self.yaml_file 
        return read_rules(yaml_file)


    def build(self, rule_list=None):
        ssl._create_default_https_context = ssl._create_unverified_context
        # 1. get all the input variables
        creator_id = self.get_creator_id()
        df_yaml = self.read_rules()

        # 2. Build a rule template

        # Create the main JSON object with the specified properties
        json_obj = {
            "id": "example-id",
            "created": "2023-03-08T12:00:00Z",
            "changed": "2023-03-08T12:00:00Z",
            # "creator": {"id": str(uuid.uuid4())},
            "creator": {"id": creator_id},
            "content": "example-content",
            "json": {
                # "properties": json_data["properties"]
                "Check": None,
                "Core": None,
                "Description": None,
                "Outcome": {},
                "Rule_Type": None,
                "Sensitivity": None,
                "Authorities": [],
                "Scope": {},
                "Executability": None
            }
        }
        rule_list = ("CG0373", "CG0378", "CG0379") if rule_list is None else rule_list 
        proc_sdtm_rules(df_yaml, json_obj, rule_list,
                        self.existing_rule_dir,  self.output_dir)



# End of File 