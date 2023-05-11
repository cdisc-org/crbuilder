# Purpose: Define a database container 
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   04/24/2023 (htu) - initial coding
#

import json
import requests
import os
from dotenv import load_dotenv
from azure.cosmos import ContainerProxy
from azure.cosmos import CosmosClient

def define_db_container(url: str, key: str, db_name: str, ct_name: str):
    cln = CosmosClient(url, key)
    db = cln.get_database_client(database=db_name)
    ct = db.get_container_client(container=ct_name)
    # db_link = f"/dbs/{db_name}"
    # ct_link = f"{db_name}/colls/{ct_name}"
    # cp = ContainerProxy(cln, db, ct)
    return ct 


# Test cases
if __name__ == "__main__":
    load_dotenv()
    # url = os.getenv("DEV_COSMOS_URL")
    # key = os.getenv("DEV_COSMOS_KEY")
    # db = os.getenv("DEV_COSMOS_DATABASE")
    # ct = os.getenv("DEV_COSMOS_CONTAINER")
    # headers = {
    #     'Content-Type': 'application/json',
    #     'x-ms-documentdb-isquery': 'True',
    #     'x-ms-date': '',
    #     'x-ms-version': '2018-12-31',
    #     'Authorization': '',
    # }
    # udf_query = {
    #     'query': 'SELECT udf.id FROM udf'
    # }
    # token = 'type=master&ver=1.0&sig=<signature>'
    # headers['Authorization'] = token
    # udf_url = f'{url}dbs/{db}/colls/{ct}/udfs'
    # response = requests.post(udf_url, headers=headers, data=json.dumps(udf_query))
    # response.raise_for_status()
    # response_json = response.json()
    # udf_ids = [udf['id'] for udf in response_json]
    # print(f'User-defined functions in container {ct}:')
    # for udf_id in udf_ids:
    #     print(udf_id)

    load_dotenv()
    url = os.getenv("DEV_COSMOS_URL")
    key = os.getenv("DEV_COSMOS_KEY")
    db = os.getenv("DEV_COSMOS_DATABASE")
    ct = os.getenv("DEV_COSMOS_CONTAINER")

    client = CosmosClient(url, credential=key)
    database = client.get_database_client(db)
    container = database.get_container_client(ct)

    containers = database.list_containers()

    for ctr in containers:
        print(ctr['id'])

    query = "SELECT *  FROM udf "
    results = container.query_items(
        query=query,
        enable_cross_partition_query=True
    )

    for item in results:
        print(item['id'])
