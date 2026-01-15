import os
import sys
import subprocess
import time
import re
import json
import snowflake.connector
from snowflake.connector.errors import ProgrammingError
from snowflake.connector.util_text import split_statements
from io import StringIO
from common_logger import logger

# Constant list of SQL files to execute
SQL_FILES = [
    "snowflake_sql/01_SETUP_INFRASTRUCTURE.sql",
    "snowflake_sql/02_REGISTER_PROCEDURES.sql"
]

# Predefined Role ID for 'Storage Blob Data Contributor'
ROLE_ID_STORAGE_BLOB_DATA_CONTRIBUTOR = os.getenv("ROLE_ID_STORAGE_BLOB_DATA_CONTRIBUTOR")

def get_credentials():
    """
    Fetch Snowflake credentials from environment variables.
    Returns a dictionary with connection parameters.
    """
    return {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "password": os.getenv("SNOWFLAKE_PASSWORD"),
        "warehouse": "COMPUTE_WH",
        "role": "ACCOUNTADMIN"
    }

def get_replacements():
    """
    Fetch replacement variables from environment variables.
    Returns a dictionary with placeholders and their corresponding values.
    It is used to replace placeholders in SQL files before execution.
    """    
    replacements = {
        "__AZURE_TENANT_ID__": os.getenv("REPLACE_AZURE_TENANT_ID"),
        "__STORAGE_URL__": os.getenv("REPLACE_STORAGE_URL"),
        "__ETL_PASSWORD__": os.getenv("SNOWFLAKE_ETL_PASSWORD"),
        "__BI_PASSWORD__": os.getenv("SNOWFLAKE_BI_PASSWORD")
    }
    for k, v in replacements.items():
        if not v or v.strip() == "":
            logger.critical(f"Variable {k} is empty! Cannot proceed.")
            sys.exit(1)
    return replacements

def whitelist_snowflake_vnet(conn):
    """
    Whitelists Snowflake VNet subnets in the specified Azure Storage Account firewall.
    """
    logger.info("FIREWALL: Whitelisting Snowflake VNet in Azure Storage...")
    rg_name = os.getenv("AZURE_RESOURCE_GROUP")
    storage_name = os.getenv("AZURE_STORAGE_ACCOUNT")
    
    if not rg_name or not storage_name:
        logger.error("Missing Azure Env Vars. Skipping firewall config.")
        return

    cur = conn.cursor()
    try:
        cur.execute("SELECT SYSTEM$GET_SNOWFLAKE_PLATFORM_INFO()")
        result = cur.fetchone()
        if not result: return

        result_json = result[0]
        data = json.loads(result_json)
        subnet_ids = data.get('snowflake-vnet-subnet-id', [])
        if not subnet_ids: subnet_ids = data.get('snowflake_vnet_subnet_ids', [])

        if not subnet_ids:
            logger.info("No subnets found to whitelist.")
            return

        logger.info(f"Found {len(subnet_ids)} subnet(s). Processing...")
        
        # Determine if running on Windows
        # Use shell=True on Windows to avoid issues with command execution
        is_windows = (os.name == 'nt')
        
        for subnet_id in subnet_ids:
            readable_id = subnet_id.split('/')[-1]
            logger.info(f"Adding rule for: {readable_id}")
            
            cmd = [
                "az", "storage", "account", "network-rule", "add",
                "--resource-group", rg_name,
                "--account-name", storage_name,
                "--subnet", subnet_id,
                "--output", "none"
            ]
            
            try:
                subprocess.run(cmd, check=True, stderr=subprocess.PIPE, shell=is_windows)
            except subprocess.CalledProcessError:
                pass 
                
    except Exception as e:
        logger.error(f"Error configuring Firewall: {e}")
    finally:
        cur.close()


def configure_azure_storage_access():
    """
    Grants the 'Storage Blob Data Contributor' role to the Snowflake Service Principal.
    Retries up to 3 times in case of transient errors.
    """
    logger.info("INTEGRATION SETUP: Configuring Azure Access (RBAC)...")

    # Get Snowflake Service Principal Object ID from env variable
    sp_object_id = os.getenv("SNOWFLAKE_PRINCIPAL_ID")
    if not sp_object_id:
        logger.error("CRITICAL: SNOWFLAKE_PRINCIPAL_ID variable is missing!")
        return False

    logger.info(f"Using Snowflake Object ID: {sp_object_id}")
    
    rg_name = os.getenv("AZURE_RESOURCE_GROUP")
    storage_name = os.getenv("AZURE_STORAGE_ACCOUNT")
    sub_id = os.getenv("AZURE_SUBSCRIPTION_ID")

    logger.info("Granting Role: Storage Blob Data Contributor...")

    if not all([rg_name, storage_name, sub_id]):
        logger.error("Missing Azure Env Vars.")
        return False

    scope_id = f"/subscriptions/{sub_id}/resourceGroups/{rg_name}/providers/Microsoft.Storage/storageAccounts/{storage_name}"
    
    # Construct the Azure CLI command to assign the role
    role_cmd = [
        "az", "role", "assignment", "create", 
        "--role", ROLE_ID_STORAGE_BLOB_DATA_CONTRIBUTOR, 
        "--assignee-object-id", sp_object_id, 
        "--assignee-principal-type", "ServicePrincipal", 
        "--scope", scope_id, 
        "--output", "none"
    ]

    logger.info(f"Executing Azure CLI command to assign role...")

    # Determine if running on Windows
    # Use shell=True on Windows to avoid issues with command execution
    is_windows = (os.name == 'nt')

    for attempt in range(1, 4):
        try:
            subprocess.run(role_cmd, check=True, stderr=subprocess.PIPE, text=True, shell=is_windows)
            logger.info("SUCCESS: Role granted successfully!")
            return True
        except subprocess.CalledProcessError as e:
            error_msg = e.stderr.strip() if e.stderr else "Unknown CLI Error"
            if "already exists" in error_msg:
                logger.info("Role already exists.")
                return True
            logger.warning(f"Attempt {attempt}/3 failed. Azure said: {error_msg}")
            time.sleep(5)
    
    logger.error("FAILED to grant role via Azure CLI.")
    return False

def execute_sql_file(conn, file_path, replacements):
    """
    Executes a SQL file against the Snowflake connection.
    Replaces placeholders in the SQL file with provided replacements.
    """    
    logger.info(f"Processing: {file_path}")
    cursor = conn.cursor()
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()

        # Apply replacements
        for key, value in replacements.items():
            sql_content = sql_content.replace(key, value)

        # Split and execute statements
        for item in split_statements(StringIO(sql_content), remove_comments=True):
            if isinstance(item, tuple): stmt = item[0]
            else: stmt = item
            if stmt and stmt.strip():
                try: cursor.execute(stmt)
                except ProgrammingError as e:
                    logger.error(f"SQL ERROR: {e}")
                    return False
        
        logger.info(f"Success: {file_path}")
        
        # Infrastructure configuration logic
        if "01_SETUP_INFRASTRUCTURE" in file_path:
            is_gha = os.getenv("GITHUB_ACTIONS") == "true"

            if is_gha:
                logger.info("ENV: GitHub Actions detected. Running infrastructure setup (RBAC + Firewall).")
                
                # Configure Azure Storage Access (RBAC)
                if not configure_azure_storage_access():
                    logger.error("Failed to configure Azure Storage Access.")
                    return False
                
                # Configure Firewall
                whitelist_snowflake_vnet(conn)
            else:
                logger.info("ENV: Local Development detected. SKIPPING infrastructure setup (RBAC + Firewall).")
                logger.info("Assuming Infrastructure is already correctly provisioned by CI/CD pipeline.")
            
        return True
    except Exception as e:
        logger.error(f"File Error {file_path}: {e}")
        return False
    finally:
        cursor.close()

def main():
    replacements = get_replacements()
    creds = get_credentials()
    
    if not all([creds["account"], creds["user"], creds["password"]]):
        logger.critical("Missing credentials.")
        sys.exit(1)

    logger.info("Connecting to Snowflake...")
    try:
        conn = snowflake.connector.connect(**creds)
        logger.info("Connected!")
        for sql_file in SQL_FILES:
            if not execute_sql_file(conn, sql_file, replacements):
                logger.error("Deployment failed.")
                sys.exit(1)
        logger.info("DEPLOYMENT FINISHED SUCCESSFULLY.")
    except Exception as e:
        logger.critical(f"Critical Error: {e}")
        sys.exit(1)
    finally:
        if 'conn' in locals() and conn: conn.close()

if __name__ == "__main__":
    main()