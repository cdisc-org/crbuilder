# Purpose: Get a list of rules using sql query
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   04/24/2023 (htu) - initial coding based transformer_cosmosdb
#   04/25/2023 (htu) - 
#     1. remove udf.TO_STRING and added loop to convert list to str
#     2. added job_id and write2file

import os
import csv
import datetime as dt
from dotenv import load_dotenv
from rulebuilder.echo_msg import echo_msg
from rulebuilder.create_log_dir import create_log_dir
from rulebuilder.define_db_container import define_db_container 

def get_rule_list(url: str, key: str, db_name: str, ct_name: str, 
                  job_id:str=None,write2file:int=0):
    v_prg = __name__
    v_stp = 1.0
    v_msg = f"Getting rule records from {db_name}.{ct_name}..."
    echo_msg(v_prg, v_stp, v_msg, 2)
    if write2file > 0:
        v_stp = 2.1
        tm = dt.datetime.now()
        ymd = tm.strftime("%Y%m%d")
        if job_id is None:
            job_id = tm.strftime("R%H%M%S")
        log_cfg = create_log_dir(job_id=job_id)
        log_fn = f"{log_cfg['log_fdir']}/{job_id}-detailed.txt"
        rst_fn = f"{log_cfg['dat_fdir']}/{job_id}-{ymd}.csv"
        os.environ["log_fn"] = log_fn
        v_msg = "Output data to {log_cfg['dat_log']}..."
        echo_msg(v_prg,v_stp,v_msg,2)

    rules = []
    query = """
            SELECT DISTINCT
            Rule['Core']['Id'] ?? null as 'CORE-ID',
            (ARRAY(SELECT DISTINCT VALUE Reference['Rule_Identifier']['Id'] FROM Authority IN Rule['Authorities'] JOIN Standard IN Authority['Standards'] JOIN Reference IN Standard['References'])) as 'CDISC Rule ID',
            (ARRAY(SELECT DISTINCT VALUE Authority['Organization'] FROM Authority IN Rule['Authorities'])) as 'Organization',
            (ARRAY(SELECT DISTINCT VALUE Standard['Name'] FROM Authority IN Rule['Authorities'] JOIN Standard IN Authority['Standards'])) as 'Standard Name',
            (ARRAY(SELECT DISTINCT VALUE Standard['Version'] FROM Authority IN Rule['Authorities'] JOIN Standard IN Authority['Standards'])) as 'Standard Version',
            Rule['Core']['Status'] ?? null as 'Status',
            Rule['Sensitivity'] ?? null as 'Sensitivity',
            Rule['Rule_Type'] ?? null as 'Rule Type',
            Rule['Executability'] ?? null as 'Executability',
            Rule['Scope'] ?? null as 'Scope',
            Rule['Outcome']['Message'] ?? null as 'Error Message',
            Rule['Outcome']['Output_Variables'] ?? null as 'Output Variables',
            Rule['Description'] ?? null as 'Description',
            Rule['Check'] ?? null as 'Check',
            Rule['Operations'] ?? null as 'Operations'
            FROM Rules['json'] as Rule
            ORDER BY Rule['Core']['Id'] ASC
        """
    dc = define_db_container(url, key, db_name=db_name, ct_name=ct_name)
    for rule in dc.query_items(
            query=query, enable_cross_partition_query=True
        ):
        rules.append(rule)
    v_stp = 2.2
    tot_records = len(rules)
    v_msg = f" . Got total of {tot_records}."
    echo_msg(v_msg,v_stp,v_msg,3)

    for i in range(tot_records):
        for k in rules[i].keys():
            if isinstance(rules[i][k], list):
                # Convert integer items to strings
                rules[i][k] = [str(x) for x in rules[i][k]]
                # Join the items with commas
                rules[i][k] = ",".join(rules[i][k])
    
    if write2file > 0:
        v_stp = 3.1
        v_msg = f"Writing records to {rst_fn}..."
        echo_msg(v_prg, v_stp, v_msg, 3)
        fd_names = rules[0].keys()
        with open(rst_fn, mode='w', newline='') as file:
            # Create a writer object
            writer = csv.DictWriter(file, fieldnames=fd_names)
            # Write the header row
            writer.writeheader()
            # Write the data rows
            for row in rules:
                writer.writerow(row)
    return rules


# Test cases
if __name__ == "__main__":
    load_dotenv()
    url = os.getenv("DEV_COSMOS_URL")
    key = os.getenv("DEV_COSMOS_KEY")
    db = os.getenv("DEV_COSMOS_DATABASE")
    ct = os.getenv("DEV_COSMOS_CONTAINER")
    os.environ["g_lvl"] = "5"

    rs = get_rule_list(url, key, db, ct, write2file=1)
