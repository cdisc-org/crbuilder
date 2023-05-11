# Purpose: Gather all rules and extract out info and put into one XLSX file
# History: MM/DD/YYYY (developer) - description
#   03/06/2023 (htu) - iniital coding
#   03/07/2023 (htu) - added code to make the detination folder
#   03/13/2023 (htu) - added core_id and rule_id
#
import os
import json
import pandas as pd
from jsonrenamer.json_file_renamer import JsonFileRenamer

renamer = JsonFileRenamer('./data/output/json_rules','./data/output/json_rules1')

# Set folder paths
json_folder = "./data/output/json_rules"
xlsx_folder = "./data/target"
xlsx_file   = "current_rules.xlsx"

# Create an empty dataframe
df = pd.DataFrame(columns=["core_id","rule_id","id", "created", "changed", "json.Core.Id", "json.Core.Version",
                           "json.Core.Status", "json.Rule_Type", "json.Sensitivity",
                           "json.Description", "json.Outcome", "json.Authorities",
                           "json.Scope", "json.Scope.Classes.Include", "json.Scope.Domains.Include",
                           "json.Check", "content"])

# Loop through all JSON files in the folder
rows = []
for filename in os.listdir(json_folder):
    if filename.endswith(".json"):
        # Read JSON file
        with open(os.path.join(json_folder, filename)) as f:
            data = json.load(f)

        # Extract required fields if they exist
        row = {"core_id": None, "rule_id": None, "id": None, "created": None, "changed": None, 
               "json.Core.Id": None,
               "json.Core.Version": None, "json.Core.Status": None, "json.Rule_Type": None,
               "json.Sensitivity": None, "json.Description": None, "json.Outcome": None,
               "json.Authorities": None, "json.Scope": None, "json.Scope.Classes.Include": None,
               "json.Scope.Domains.Include": None, "json.Check": None, "content": None}
        core_id = renamer.get_core_id(data)
        rule_id = renamer.get_rule_id(data)
    
        row.update({"core_id": core_id})
        row.update({"rule_id": rule_id})
        row.update({"id": data.get("id")})
        row.update({"created": data.get("created")})
        row.update({"changed": data.get("changed")})
        row.update({"json.Core.Id": data.get(
            "json", {}).get("Core", {}).get("Id")})
        row.update({"json.Core.Version": data.get(
            "json", {}).get("Core", {}).get("Version")})
        row.update({"json.Core.Status": data.get(
            "json", {}).get("Core", {}).get("Status")})
        row.update({"json.Rule_Type": data.get("json", {}).get("Rule_Type")})
        row.update({"json.Sensitivity": data.get(
            "json", {}).get("Sensitivity")})
        row.update({"json.Description": data.get(
            "json", {}).get("Description")})
        row.update({"json.Outcome": data.get("json", {}).get("Outcome")})
        row.update({"json.Authorities": data.get(
            "json", {}).get("Authorities")})
        row.update({"json.Scope": data.get("json", {}).get("Scope")})
        row.update({"json.Scope.Classes.Include": data.get("json", {}).get(
            "Scope", {}).get("Classes", {}).get("Include")})
        row.update({"json.Scope.Domains.Include": data.get("json", {}).get(
            "Scope", {}).get("Domains", {}).get("Include")})
        row.update({"json.Check": data.get("json", {}).get("Check")})
        row.update({"content": data.get("content")})

        # Append row to list of rows
        rows.append(row)

# Create dataframe from list of rows
df = pd.DataFrame.from_records(rows)

# Write dataframe to xlsx file
if not os.path.exists(xlsx_folder):
    os.makedirs(xlsx_folder)

opf_name = os.path.join(xlsx_folder, xlsx_file)
df.to_excel(opf_name, index=False)
print (f"Output to {opf_name}")



