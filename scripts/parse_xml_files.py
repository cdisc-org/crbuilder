# Purpose: Parse XML file to get rule categories 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   04/28/2023 (htu) - initial coding
#

import os 
import glob
import pandas as pd
import xml.etree.ElementTree as ET

def parse_xml_files (
        xml_fn:str="SDTM-IG 3.3 (FDA).xml", 
        i_dir:str="./data/source/xmls/2204.0",
        o_dir:str="./data/output/csvs"
):
    if "/" in xml_fn: 
        ifn_path = xml_fn
        ifn = os.path.basename(xml_fn)
    else:
        ifn = xml_fn
        ifn_path = f"{i_dir}/{xml_fn}"
    ofn_csv = f"{o_dir}/{ifn}.csv"

    # Parse the XML file
    tree = ET.parse(ifn_path)
    root = tree.getroot()

    # Create an empty list to store the data
    data = []
    row_list = ["Condition", "Find", "Lookup", "Match", "Metadata", "Regex", 
                "Required", "Unique", "Schematron", "Property", "Varlength",
                "Varorder"
                ]
    key_1 = "Rule Type"
    key_2 = "Category"
    r_types = []
    # Loop through each element in the XML file and extract the required attributes
    for elem in root.iter():
        if 'val:ValidationRules' in elem.tag:
            continue
        row = {}
        e_items = elem.items()
        e_tag = elem.tag.split("}")[1]
        if e_tag not in r_types:
            r_types.append(e_tag)
        if e_tag not in row_list: 
            continue 
        row[key_1] = e_tag

        # print(f"{e_tag}:{e_items}")
        for k, v in e_items:
            if "}" in k:
                k = k.split("}")[1]
            # print(f"{e_tag} - {k}: {v}")
            row[k] = v
        data.append(row)

    # print(f"Available Types:{r_types}")
    data_len = len(data)
    print(f"Total records from {xml_fn}: {data_len}") 

    # Create a DataFrame from the extracted data
    df = pd.DataFrame(data)
    col_2 = df.pop(key_2)
    df.insert(1, key_2, col_2)
    df_sorted = df.sort_values(
        [key_1, key_2, "ID"], ascending=[True, True, True])

    # Write the DataFrame to a CSV file
    df_sorted.to_csv(ofn_csv, index=False)


# Test cases
if __name__ == "__main__":
    # Define the path to the folder containing the XML files
    folder_path = './data/source/xmls/2204.0'

    # Use glob to search for XML files in the folder
    xml_files = glob.glob(folder_path + '/*.xml')

    # Print the list of XML file names
    for f in xml_files: 
        # print(f)
        parse_xml_files(f)
    