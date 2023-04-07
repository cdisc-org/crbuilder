# Purpose: Read from source rule documents and convert them to YAML
# History: MM/DD/YYYY (developer) - description
#   04/06/2023 (htu) - initial coding based on proc_xlsx 
#     and added pickle output 
#
#  python -c "import pandas as pd; print(pd.__version__)"
#  python -c "import openpyxl as xl; print(xl.__version__)"
#  pip install --force-reinstall -v "openpyxl==3.1.0"
#


import re 
import pandas as pd
import yaml
import pickle

# define source data files
source_data_files = {
    "FDA_VR1_6": { "file_name": 'FDA_VR_v1.6.xlsx', 
                   "rule_sheet":'FDA Validator Rules v1.6' },
    "SDTM_V2_0":{"file_name": 'SDTM_and_SDTMIG_Conformance_Rules_v2.0.xlsx', 
              "rule_sheet":'SDTMIG Conformance Rules v2.0'},
    "SEND_V4_0":{"file_name": 'SEND_Conformance_Rules_v4.0.xlsx', 
              "rule_sheet":'v3.0_v3.1_v3.1.1_DARTv1.1'},
    "ADAM_V4_0":{"file_name": 'ADaM_Conformance_Rules_v4.0.xlsx', 
              "rule_sheet":'ADaM Conformance Rules v4.0'},

}

rule_mapping_files = {
    "file_name": "Rule Mapping and Categories.xlsx",
    "rul_sheet": "Rules",
    "mapping_sheet": "Mapping",
    "category_sheet": "Categories"
}

def read_xlsx(f_name:str, s_name:str, r_dir:str = None):
    if r_dir is None: 
        s_dir = "./data/source/xlsx"
    else:
        s_dir = r_dir 
    f_path = s_dir + "/" + f_name 

    print(f"  Reading from {f_path}...\n  . Sheet: {s_name}")

    if f_name.startswith('FDA'):
        df = pd.read_excel(f_path, sheet_name=s_name,
                           header=1, engine='openpyxl')
    else:
       df = pd.read_excel(f_path, sheet_name=s_name, engine='openpyxl')
    # remove newline breaks in column names
    df.columns = df.columns.str.replace('\n', '')
    df.columns = df.columns.str.lstrip()           # remove leading spaces
    df.columns = df.columns.str.rstrip()           # remove trailing spaces
    # remove newline breaks in data
    df = df.fillna('')                              # fill na with ''
    df = df.applymap(lambda x: re.sub(r'\n\n+', '\n', str(x)))

    # Convert the dataframe to a dictionary
    data = df.to_dict(orient='records')
    return data 

def output2yaml(df, f_name):
    with open(f_name, 'w') as f:
        print(f"Writing to file: {f_name}...")
        # Serialize the object and write it to the file
        yaml.dump(df, f)

def output2pick(df, f_name): 
    # Open a file for writing binary data
    with open(f_name, 'wb') as f:
        print(f"Writing to file: {f_name}...")
        # Serialize the object and write it to the file
        pickle.dump(df, f)

def read_pick(fn):
    # Open the file for reading binary data
    with open(fn, 'rb') as f:
        # Deserialize the object from the file
        data = pickle.load(f)
    # Create a DataFrame from the loaded data
    df = pd.DataFrame(data)
    print(df) 
    return df   

def cvt_xlsx(std: str = "SDTM_V2_0", r_dir: str = None, 
                  s_files: dict = source_data_files):
    
    if r_dir is None:
        s_dir = "./data/source"
    else:
        s_dir = r_dir 
    x_dir = s_dir + "/xlsx"
    y_dir = s_dir + "/yaml"
    p_dir = s_dir + "/pick"

    fn = s_files[std]["file_name"]
    sn = s_files[std]["rule_sheet"]

    df = read_xlsx(f_name=fn,s_name=sn, r_dir=x_dir)

    opf = y_dir + "/" + std.lower() + ".yaml"
    output2yaml(df, opf)

    opf = p_dir + "/" + std.lower() + ".pick"
    output2pick(df, opf)

    read_pick(opf)


# python -c "import pandas as pd; print(pd.__version__)"
# python -c "import openpyxl as xl; print(xl.__version__)"
# python -c "import yaml as y; print(y.__version__)"



if __name__ == '__main__':
    df = cvt_xlsx("SDTM_V2_0", s_files=source_data_files)
