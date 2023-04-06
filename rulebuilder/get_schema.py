# Purpose: Get Core Rule Schema
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/16/2023 (htu) - Extracted out from sdtmrulebuilder 
#                    - added docstring and echo_msg 
#


from rulebuilder.echo_msg import echo_msg 
import os 
import json 
import urllib.request
import ssl 


def get_schema(base_url=None):
    """
    ==========
    get_schema 
    ==========
    This method initializes the class.

    Parameters:
    ----------- 
    base_url: str
        Core base URL 

    returns
    -------
        json_data:  containing core rule schema in json format

    """
    v_prg = __name__ + "::get_schema"
    v_step = 1.0
    v_msg = "Getting Base Schema..."
    echo_msg(v_prg, v_step, v_msg, 2)
    url = base_url 
    if url is None:
        v_msg = "URL is empty"
        echo_msg(v_prg, v_step, v_msg, 2)
        return {}
    try:
        ssl._create_default_https_context = ssl._create_unverified_context
        req = urllib.request.Request(url)
        v_step = 2.1
        v_msg = print(f"Openning {url}...")
        echo_msg(v_prg, v_step, v_msg, 3)
        # Fetch the contents of the URL and parse it as JSON
        with urllib.request.urlopen(req, timeout=10) as rsp:
            v_step = 2.2
            raw_data = rsp.read().decode()
            json_data = json.loads(raw_data)
            return json_data
    except urllib.error.URLError as e:
        print(f"Error opening URL: {url}\nError message: {e.reason}")
        echo_msg(v_prg, v_step, v_msg, 0)
        return {}


# Test cases
if __name__ == "__main__":
    os.environ["g_lvl"] = "3"
    # 1. Test with basic parameters
    v_prg = __name__ + "::get_schema"
    echo_msg(v_prg, 1.0, "Starting test case 1",1)
    rr = get_schema()
    print(f"Expected: {rr}\n")
    # Expected output: "dd0f9aa3-68f9-4825-84a4-86c8303daaff"

    # 2. Test with a URL 
    echo_msg(v_prg, 2.0, "Starting test case 2",1)
    ur = "https://raw.githubusercontent.com/cdisc-org/conformance-rules-editor/main/public/schema/CORE-base.json"
    rr = get_schema(ur)
    print(f"Expected: {rr}\n")

