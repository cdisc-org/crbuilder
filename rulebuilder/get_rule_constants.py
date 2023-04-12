# Purpose: Get Rule Authority Constants
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   04/10/2023 (htu) - initial coding
#
import os 

def get_rule_constants (r_std:str=None):  

    r_std = os.getenv("r_standard") if r_std is None else r_std
    r_std = r_std.upper()
    r_auth = {
    "FDA_VR1_6": {
        "Authorities": {
            "Organization": "FDA", 
            "Standards.Name": "SDTMIG",
            "Standards.References.Origin": "FDA Validator Rules",
            "Standards.References.Version": "1.6",
            "Standard.References.Rule_Identifier.Version": "1"
        },
        "Core": {
            "Status":   "Draft",
            "Version":  "1"
        },
        "Executability": "Fully Executable",
        "VS": ["SDTMIG3.1.2", "SDTMIG3.1.3", "SDTMIG3.2", "SDTMIG3.3",
                "SENDIG3.0", "SENDIG3.1", "SENDIG3.1.1", "SENDIG-AR1.0",
                "SENDIG-DART1.1"]

    },
    "SDTM_V2_0": {
        "Authorities": {
            "Organization": "CDISC",
            "Standards.Name": "SDTMIG",
            "Standards.References.Origin": "SDTM and SDTMIG Conformance Rules",
            "Standards.References.Version": "2.0"
            },
        "Core": {
            "Status":   "Draft", 
            "Version":  "1"
            },
        "Executability": "Fully Executable"
    },
    "SEND_V4_0": {
        "Authorities": {
            "Organization": "CDISC",
            "Standards.Name": "SENDIG",
            "Standards.References.Origin": "SEND Conformance Rules",
            "Standards.References.Version": "4.0"
        },
        "Core": {
            "Status":   "Draft",
            "Version":  "1"
        },
        "Executability": "Fully Executable"
        },
    "ADAM_V4_0": {
        "Authorities": {
            "Organization": "CDISC",
            "Standards.Name": "ADAMIG",
            "Standards.References.Origin": "ADaM Conformance Rules",
            "Standards.References.Version": "4.0"
        },
        "Core": {
            "Status":   "Draft",
            "Version":  "1"
        },
        "Executability": "Fully Executable"
    }
    }
    return r_auth[r_std]