# Purpose: Gather all rules and extract out info and put into one XLSX file
# History: MM/DD/YYYY (developer) - description
#   03/06/2023 (htu) - iniital coding 
#   03/06/2023 (htu) - installed python-dotenv and made load_dotenv to work
#   03/07/2023 (htu) - added code to make the detination folder
#
import os
import json
import pandas as pd

from openpyxl import Workbook
from openpyxl.chart import BarChart, Reference

# Set folder paths
json_folder = "./data/output/json_rules"
xlsx_folder = "./data/target"
xlsx_file = "current_rules.xlsx"

# Create an empty dataframe
df = pd.DataFrame(columns=["id", "created", "changed", "json.Core.Id", "json.Core.Version",
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
        row = {"id": None, "created": None, "changed": None, "json.Core.Id": None,
               "json.Core.Version": None, "json.Core.Status": None, "json.Rule_Type": None,
               "json.Sensitivity": None, "json.Description": None, "json.Outcome": None,
               "json.Authorities": None, "json.Scope": None, "json.Scope.Classes.Include": None,
               "json.Scope.Domains.Include": None, "json.Check": None, "content": None}
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
# df.to_excel(os.path.join(xlsx_folder, xlsx_file), index=False)


# Create a pivot table and chart of json.Core.Status
if not os.path.exists(xlsx_folder):
    os.makedirs(xlsx_folder)

book = Workbook()
with pd.ExcelWriter(os.path.join(xlsx_folder, xlsx_file), engine='openpyxl') as writer:
    writer.book = book
    writer.sheets = {ws.title: ws for ws in book.worksheets}

    # Write data to sheet 1
    df.to_excel(writer, sheet_name='Current Rules', index=False)

    # Create pivot table on json.Core.Status and write to sheet 2
    pivot_table = pd.pivot_table(
        df, values='id', index='json.Core.Status', aggfunc='count')
    pivot_table.to_excel(writer, sheet_name='Pivot Table')

    # Create chart on pivot table and write to sheet 2
    chart = BarChart()
    data = Reference(writer.sheets['Pivot Table'], min_col=2,
                     min_row=1, max_row=len(pivot_table.index)+1)
    categories = Reference(
        writer.sheets['Pivot Table'], min_col=1, min_row=2, max_row=len(pivot_table.index)+1)
    chart.add_data(data, titles_from_data=True)
    chart.set_categories(categories)
    chart.title = 'Rule Status Distribution'
    chart.y_axis.title = 'Number of Rules'
    writer.sheets['Pivot Table'].add_chart(chart, 'A5')

    writer.save()
