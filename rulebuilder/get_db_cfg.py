# Purpose: Get Comsmos DB connection configuration
# -----------------------------------------------------------------------------
# History: MM/DD/YYYY (developer) - description
#   03/31/2023 (htu) - initial coding based on
#     https://learn.microsoft.com/en-us/python/api/overview/azure/cosmos-readme?view=azure-python#create-a-container
#   04/03/2023 (htu) - renamed container_name to ct_name 
#

import os
from dotenv import load_dotenv
from rulebuilder.echo_msg import echo_msg
from azure.cosmos import CosmosClient, exceptions

def get_db_cfg (db_name:str='library',ct_name:str=None):
    """
    Retrieves the configuration for a database and an optional container in an Azure Cosmos DB account.

    Parameters:
    db_name (str): The name of the database.
    ct_name (str): The name of the container (optional).

    Returns:
    dict: A dictionary containing the database and container configuration details, including the URL, key, database name,
    database client, and container client (if ct_name is provided and the container exists).

    Raises:
    Nothing is raised. If the database or container does not exist, a message is printed to the console.

    """
    v_prg = __name__
    v_stp = 1.0
    v_msg = "Configuring database connections..."
    echo_msg(v_prg, v_stp, v_msg,1)

    if db_name is None:
        v_stp = 1.1
        v_msg = "Database name is required."
        echo_msg(v_prg, v_stp, v_msg,1)
        return {}

    # 1.2 check if the database exists
    load_dotenv()
    url = os.getenv("DEV_COSMOS_URL")
    key = os.getenv("DEV_COSMOS_KEY")
    db_cfg = {"url": url, "key": key, "db_name": db_name, 
              "ct_name": ct_name}
    try:
        v_stp = 1.21
        cln = CosmosClient(url, credential=key)
        v_msg = f"Create database client for {db_name}"
        db_cfg["db_client"] = cln
        echo_msg(v_prg, v_stp, v_msg,2)
    except exceptions.CosmosResourceNotFoundError:
        v_stp = 1.22
        v_msg = f"Could not establish client connection for {db_name}"
        echo_msg(v_prg, v_stp, v_msg,0)
        return db_cfg 
    
    try:
        v_stp = 1.31 
        db_conn = cln.get_database_client(db_name)
        db_cfg["db_conn"] = db_conn 
        # If no exception is raised, the database exists
        v_msg = f"Database - {db_name} exists."
        echo_msg(v_prg, v_stp, v_msg,3)
    except exceptions.CosmosResourceNotFoundError:
        v_stp = 1.32 
        v_msg = "Database - " + db_name + " DOES NOT exists."
        echo_msg(v_prg, v_stp, v_msg,1)
        return db_cfg 
    
    # 1.3 check if container exists 
    if ct_name is not None:
        try:
            v_stp = 1.41
            cont = db_conn.get_container_client(ct_name)
            v_msg = "Found container - " + ct_name
            v_msg += " in Cosmos DB - " + db_name
            echo_msg(v_prg, v_stp, v_msg,4)
            db_cfg["ct_conn"] = cont 
        except exceptions.CosmosResourceNotFoundError:
            v_stp = 1.42
            v_msg = "Could not find " + ct_name + " in DB " + db_name 
            echo_msg(v_prg, v_stp, v_msg,0)
        except exceptions.CosmosHttpResponseError:
            raise
    else:
        v_stp = 1.43
        v_msg = "No container name is specified."
        echo_msg(v_prg, v_stp, v_msg,0)

    return db_cfg 


if __name__ == "__main__":
    # Test case 1: Retrieve the database configuration with no container specified.
    db_cfg = get_db_cfg(db_name='library')
    assert 'url' in db_cfg.keys()
    assert 'key' in db_cfg.keys()

    assert 'db_name' in db_cfg.keys()
    assert 'db_client' in db_cfg.keys()
    assert 'db_conn' in db_cfg.keys()
    assert 'ct_conn' not in db_cfg.keys()

    # Test case 2: Retrieve the database and container configuration.
    db_cfg = get_db_cfg(db_name='library', ct_name='core_rules_dev')
    assert 'url' in db_cfg.keys()
    assert 'key' in db_cfg.keys()
    assert 'db_name' in db_cfg.keys()
    assert 'db_client' in db_cfg.keys()
    assert 'db_conn' in db_cfg.keys()
    assert 'ct_conn' in db_cfg.keys()

    print("All tests are successful!")