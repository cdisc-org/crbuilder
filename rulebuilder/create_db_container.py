# Purpose: Create a Container in a Cosmos DB 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/31/2023 (htu) - initial coding based on 
#     https://learn.microsoft.com/en-us/python/api/overview/azure/cosmos-readme?view=azure-python#create-a-container
# 

# import sys 
# import json 
from azure.cosmos import PartitionKey, exceptions
from rulebuilder.get_db_cfg import get_db_cfg


db = 'library'
ct = 'core_rules_dev'
cfg = get_db_cfg(db_name=db, container_name=ct)
# json.dump(cfg,sys.stdout, indent=4)
dbc = cfg["db_conn"]

try:
    container = dbc.get_container_client(ct)
    print(f"Found container - {ct} in Cosmos DB - {db}.")
    print(f"Here is a list of containers in DB {db}: ")
    containers = dbc.list_containers()
    for container in containers:
        print(f" . {container['id']}")
except exceptions.CosmosResourceNotFoundError:
    container = dbc.create_container(
        id=db, partition_key=PartitionKey(path="/id"))
    print(f"Created container - {ct} in Cosmos DB - {db}.")
except exceptions.CosmosHttpResponseError:
    raise
