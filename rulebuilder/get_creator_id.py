# Purpose: Get Creator ID 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/16/2023 (htu) - Extracted out from sdtmrulebuilder 
#                    - added docstring and echo_msg 
#
import os 
import json
import urllib.request
from rulebuilder.echo_msg import echo_msg 

def get_creator_id(creator_url=None):
    """
    ===============
    get_creator_id 
    ===============

    Parameters:
    -----------
    creator_url: str
        a URL pointing to a page containing creator id
    
    Return:
    -------
        a string containing creator ID

    """
    ctr_url = creator_url 
    default_id = "dd0f9aa3-68f9-4825-84a4-86c8303daaff"
    if ctr_url is None: 
        return default_id 

    try:
        req = urllib.request.Request(ctr_url)
        prg = __name__ + "::get_creator_id"
        v_step = 1.0
        v_msg = None
        echo_msg(prg, v_step, "Getting creator ID...", 3)
        with urllib.request.urlopen(req, timeout=10) as rsp:
            v_step = 2.0 
            data = rsp.read().decode('utf-8')
            creator_data = json.loads(data)
            cp = creator_data.get("clientPrincipal")
            creator_id = cp.get("userId") if cp is not None else None
            user_detail = cp.get("userDetails") if cp is not None else None
            v_msg = print(f"INFO: User Details - {user_detail}")
            echo_msg(prg,2, v_msg, 5)
            if cp is None or creator_id is None:
                creator_id = default_id
            return creator_id
    except urllib.error.URLError as e:
        print(f" . Error opening URL: {ctr_url}\n . Error message: {e.reason}")
        print("The above message can be ignored. ")
        echo_msg(prg, v_step, v_msg, 0)
        return default_id

# Test cases
if __name__ == "__main__":
    os.environ["g_lvl"] = "3"
    # 1. Test with basic parameters
    prg = "get_creator_id"
    echo_msg(prg, 1.0, "Starting test case 1",1)
    ctr_id = get_creator_id()
    print(f"Expected: {ctr_id}\n")
    # Expected output: "dd0f9aa3-68f9-4825-84a4-86c8303daaff"

    # 2. Test with a URL 
    echo_msg(prg, 2.0, "Starting test case 2",1)
    creator_url = "https://rule-editor.cdisc.org/.auth/me"
    ctr_id = get_creator_id(creator_url)
    print(f"Expected: {ctr_id}\n")


# End of File

