# Purpose: Get or generate a GUID 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/14/2023 (htu) - ported from proc_rules_sdtm as get_rule_guid module
#   03/22/2023 (htu) - added docstring and test cases
#    

import uuid


def get_rule_guid(json_data):
    """
    Returns a unique identifier (GUID) for a rule within the JSON object.

    Args:
        json_data (dict): A JSON object containing rules.

    Returns:
        str: A unique identifier for a rule.

    """
    rule_guid = json_data.get('id')
    if rule_guid is None:
        rule_guid = str(uuid.uuid4())
    return rule_guid


if __name__ == "__main__":
    # Test case 1: Test with a JSON object that contains an "id" field
    json_data_1 = {
        "id": "123",
        "name": "Rule 1",
        "description": "This is the first rule"
    }

    guid_1 = get_rule_guid(json_data_1)
    assert guid_1 == "123"

    # Test case 2: Test with a JSON object that does not contain an "id" field
    json_data_2 = {
        "name": "Rule 2",
        "description": "This is the second rule"
    }

    guid_2 = get_rule_guid(json_data_2)
    assert isinstance(guid_2, str) and len(guid_2) == 36

    print("All tests are successful!")