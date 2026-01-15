# =============================================================================
# LOCAL DEV SCRIPT: CODE DEPLOYMENT
# =============================================================================
# This script assumes that Azure infrastructure already exists (created by GHA).
# Used for quick testing of changes in Python/SQL. Does not generate data.
# It also deploys Snowflake infrastructure and sets up environment variables.

# Import utility functions
. "$PSScriptRoot\utils.ps1"

$ErrorActionPreference = "Stop"

# Login and configuration
Write-Log "Azure Login & Context" -Level "STEP"
$account = az account show 2>$null
if (-not $account) { az login --output none }
$account = az account show --output json | ConvertFrom-Json
$SUB_ID = $account.id
$TENANT_ID = $account.tenantId

Write-Log "Connected as: $($account.user.name)" -Level "INFO"
Write-Log "Subscription: $SUB_ID" -Level "INFO"

# Finding resources
Write-Log "Detecting Existing Infrastructure..." -Level "STEP"

$STORAGE_NAME = az resource list --tag Project=Ecommerce-ETL --query "[?type=='Microsoft.Storage/storageAccounts'].name | [0]" -o tsv
$RG_NAME = az resource list --tag Project=Ecommerce-ETL --query "[?type=='Microsoft.Storage/storageAccounts'].resourceGroup | [0]" -o tsv

if (-not $STORAGE_NAME) {
    Write-Log "Infrastructure not found! Make sure GitHub Actions deployed resources with tag 'Project=Ecommerce-ETL'." -Level "ERROR"
    exit 1
}

Write-Log "Target RG: $RG_NAME | Storage: $STORAGE_NAME" -Level "INFO"


# Configuring developer access (network & RBAC)
Write-Log "Configuring Developer Access (Network & RBAC)..." -Level "STEP"

# Firewall
$myIp = (Invoke-WebRequest -Uri "https://api.ipify.org" -UseBasicParsing).Content.Trim()
az storage account network-rule add --resource-group $RG_NAME --account-name $STORAGE_NAME --ip-address $myIp --output none 2>$null
Write-Log "Local IP ($myIp) whitelisted for debugging." -Level "SUCCESS"

# RBAC (Assigning Storage Blob Data Contributor role to current user)
Write-Log "Checking User RBAC roles..." -Level "STEP"
# Get ID of logged in user
$currentUserId = az ad signed-in-user show --query id -o tsv
$blobContributorRoleId = "ba92f5b4-2d11-453d-a403-e96b0029c9fe" # Storage Blob Data Contributor
$scope = "/subscriptions/$SUB_ID/resourceGroups/$RG_NAME/providers/Microsoft.Storage/storageAccounts/$STORAGE_NAME"

# Check if role already exists
$roleExists = az role assignment list --assignee $currentUserId --scope $scope --role $blobContributorRoleId --query "[].id" -o tsv

if (-not $roleExists) {
    Write-Log "Assigning 'Storage Blob Data Contributor' to your user..." -Level "INFO"
    az role assignment create --assignee $currentUserId --role $blobContributorRoleId --scope $scope --output none
    
    Write-Log "Waiting 20s for RBAC propagation..." -Level "INFO"
    Start-Sleep -Seconds 20
} else {
    Write-Log "User already has 'Storage Blob Data Contributor' role. Skipping assignment." -Level "INFO"
}


# Setting up environment variables
Write-Log "Setting up Environment Variables..." -Level "STEP"

# Getting passwords
if (-not $env:SNOWFLAKE_ACCOUNT) { $env:SNOWFLAKE_ACCOUNT = Read-Host "Enter Snowflake Account Identifier" }
if (-not $env:SNOWFLAKE_USER)    { $env:SNOWFLAKE_USER    = Read-Host "Enter Snowflake Login Name" }
if (-not $env:SNOWFLAKE_PASSWORD){ $env:SNOWFLAKE_PASSWORD = Read-Host "Enter Snowflake Password" -AsSecureString | ConvertFrom-SecureString -AsPlainText }
# Placeholders for SQL replacement
if (-not $env:SNOWFLAKE_ETL_PASSWORD) { $env:SNOWFLAKE_ETL_PASSWORD = "DummyPassword123!" } 
if (-not $env:SNOWFLAKE_BI_PASSWORD)  { $env:SNOWFLAKE_BI_PASSWORD  = "DummyPassword123!" }

# Getting Snowflake Service Principal ID
if (-not $env:SNOWFLAKE_PRINCIPAL_ID) {
    $sfAppId = "711e28f3-43c3-48d5-bad8-dfbcb14e6c64" # Snowflake app ID
    $spId = az ad sp show --id $sfAppId --query id -o tsv 2>$null
    
    if ($spId) {
        $env:SNOWFLAKE_PRINCIPAL_ID = $spId
        Write-Log "Snowflake SP ID: $spId" -Level "INFO"
    } else {
        $env:SNOWFLAKE_PRINCIPAL_ID = Read-Host "Enter Snowflake Service Principal Object ID"
    }
}

# Setting variables for Python scripts
$RootPath = Resolve-Path "$PSScriptRoot\.."
$env:PYTHONPATH = "$RootPath\scripts"
$env:AZURE_RESOURCE_GROUP = $RG_NAME
$env:AZURE_STORAGE_ACCOUNT = $STORAGE_NAME
$env:AZURE_SUBSCRIPTION_ID = $SUB_ID
$env:REPLACE_AZURE_TENANT_ID = $TENANT_ID
$env:REPLACE_STORAGE_URL = "azure://$STORAGE_NAME.blob.core.windows.net/raw/"
$env:ROLE_ID_STORAGE_BLOB_DATA_CONTRIBUTOR = "ba92f5b4-2d11-453d-a403-e96b0029c9fe"


# Deploying Snowflake Objects
Write-Log "Deploying Snowflake Objects..." -Level "STEP"
$DeployScript = Join-Path $env:PYTHONPATH "deploy_snowflake.py"
python $DeployScript

if ($LASTEXITCODE -eq 0) {
    Write-Log "Code deployed to Snowflake successfully." -Level "SUCCESS"
} else {
    Write-Log "Deployment failed." -Level "ERROR"
}

Write-Log "DONE." -Level "STEP"