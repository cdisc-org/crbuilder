# Purpose: Read from source rule documents and convert them to YAML
# History: MM/DD/YYYY (developer) - description
#   02/24/2023 (htu) - initial coding 
#   02/25/2023 (htu) - added cvt_xlsx2yaml 
#   02/27/2023 (htu) - tested and made it to work
#   03/06/2023 (htu) - added rule_mapping_files and get_rule_mapping
#   03/07/2023 (htu) - added code to remove leading and trailing spaces in column names
#
#  python -c "import pandas as pd; print(pd.__version__)"
#  python -c "import openpyxl as xl; print(xl.__version__)"
#  pip install --force-reinstall -v "openpyxl==3.1.0"
#
#
import pandas as pd
import yaml
import os
import re

# define source data files
source_data_files = {
    "FDA_VR_1.6": { "file_name": 'FDA_VR_v1.6.xlsx', 
                   "rule_sheet":'FDA Validator Rules v1.6' },
    "SDTM_V2":{"file_name": 'SDTM_and_SDTMIG_Conformance_Rules_v2.0.xlsx', 
              "rule_sheet":'SDTMIG Conformance Rules v2.0'},
    "SEND_V4":{"file_name": 'SEND_Conformance_Rules_v4.0.xlsx', 
              "rule_sheet":'v3.0_v3.1_v3.1.1_DARTv1.1'},
    "ADAM_V4":{"file_name": 'ADaM_Conformance_Rules_v4.0.xlsx', 
              "rule_sheet":'ADaM Conformance Rules v4.0'},

}
rule_mapping_files = {
    "file_name": "Rule Mapping and Categories.xlsx",
    "rul_sheet": "Rules",
    "mapping_sheet": "Mapping",
    "category_sheet": "Categories"
}


def cvt_xlsx2yaml(file_name: str, sheet_name: str, root_dir: str = '.'):
    # define paths and file names
    rt_dir = root_dir
    fn_src  = file_name                             # source file name
    rs_name = sheet_name                            # rule sheet name 
    fn_base, fn_ext = os.path.splitext(fn_src)      # split source file name to base and extension
    fn_tgt = fn_base + '.yaml'                      # target file name

    dir_data = rt_dir + '/' + 'data'                # define data directory
    dir_src  = dir_data + '/' + 'source'            # define source directory under data dir
    dir_tgt  = dir_data + '/' + 'target'            # define tareget directory under data dir

    fn_src_path  = dir_src + '/' + fn_src           # define source file full path
    fn_tgt_path  = dir_tgt + '/' + fn_tgt           # define target file full path

    # Load the source data into a Pandas dataframe
    print(f"  Reading from {fn_src_path}...")
    # df = pd.read_excel(fn_src_path, sheet_name=None, engine='openpyxl')
    # df.items()

    if file_name.startswith('FDA'):
        df = pd.read_excel(fn_src_path, sheet_name=rs_name,header=1, engine='openpyxl')
    else: 
       df = pd.read_excel(fn_src_path, sheet_name=rs_name, engine='openpyxl') 
    # remove newline breaks in column names
    df.columns = df.columns.str.replace('\n', '')                   
    df.columns = df.columns.str.lstrip();           # remove leading spaces
    df.columns = df.columns.str.rstrip();           # remove trailing spaces
    # remove newline breaks in data
    df = df.fillna('')                              # fill na with ''
    df = df.applymap(lambda x: re.sub(r'\n\n+', '\n', str(x)))

    # Convert the dataframe to a dictionary
    data = df.to_dict(orient='records')

    # Write the dictionary to a YAML file
    print(f"  Writing to {fn_tgt_path}...")
    with open(fn_tgt_path, 'w') as f:
        yaml.dump(data, f)


def get_rule_mappiong(file_name: str, sheet_name: str):
    cvt_xlsx2yaml(file_name, sheet_name)


# for key, value in source_data_files.items():
#    print(f"  -- Processing: {key}...")
#    print(f"     cvt_xlsx2yaml('{value['file_name']}', '{value['rule_sheet']}')")
#    cvt_xlsx2yaml(value['file_name'], value['rule_sheet'])

# cvt_xlsx2yaml('FDA_VR_v1.6.xlsx', 'FDA Validator Rules v1.6')
cvt_xlsx2yaml('SDTM_and_SDTMIG_Conformance_Rules_v2.0.xlsx', 'SDTMIG Conformance Rules v2.0')


# get_rule_mappiong(
#    rule_mapping_files['file_name'], rule_mapping_files['mapping_sheet'])
