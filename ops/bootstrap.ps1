
# =============================================================================
# Bootstrap Script for Setting Up Azure Infrastructure for CI/CD with Terraform
# =============================================================================

# Import utility functions
. "$PSScriptRoot\utils.ps1"

$ErrorActionPreference = "Stop"

# Variables
$RG_NAME = "rg-ecommerce-tfstate"
$LOCATION = "switzerlandnorth"

$STORAGE_NAME = "sttfstate" + (Get-Random -Minimum 10000 -Maximum 99999) 
$CONTAINER_NAME = "tfstate"
$SP_NAME = "sp-ecommerce-github-actions"



# =============================================================================
# 1. CREATE AZURE RESOURCES
# =============================================================================

Write-Log "Creating Azure Infrastructure for CI/CD..." -Level "STEP"

# 1. Resource Group
Write-Log "Creating Resource Group: $RG_NAME..." -Level "INFO"
az group create --name $RG_NAME --location $LOCATION --output none

# 2. Storage Account for Terraform State
Write-Log "Creating Storage Account: $STORAGE_NAME..." -Level "INFO"
az storage account create --resource-group $RG_NAME --name $STORAGE_NAME --sku Standard_LRS --encryption-services blob --output none

# 3. Blob Container for Terraform State
Write-Log "Creating Blob Container: $CONTAINER_NAME..." -Level "INFO"
$KEY = (az storage account keys list --resource-group $RG_NAME --account-name $STORAGE_NAME --query '[0].value' -o tsv)
az storage container create --name $CONTAINER_NAME --account-name $STORAGE_NAME --account-key $KEY --output none


# 4. Backend Configuration File for Terraform (Local copy)
Write-Log "Creating Terraform Backend Configuration (for local reference)..." -Level "INFO"
$BackendConfig = @"
resource_group_name  = "$RG_NAME"
storage_account_name = "$STORAGE_NAME"
container_name       = "$CONTAINER_NAME"
key                  = "ecommerce.terraform.tfstate"
"@

$RootPath = Resolve-Path "$PSScriptRoot\.."
$BackendConfig | Set-Content "$RootPath\terraform\backend.conf" -Encoding ASCII


# =============================================================================
# 2. PRE-REGISTER SNOWFLAKE SERVICE PRINCIPAL (GLOBAL ID)
# =============================================================================
# Snowflake's official Service Principal App ID in Azure AD
$SNOWFLAKE_APP_ID = "711e28f3-43c3-48d5-bad8-dfbcb14e6c64"

Write-Log "Checking for Snowflake Service Principal ($SNOWFLAKE_APP_ID)..." -Level "INFO"
$snowflakeSpExists = az ad sp list --filter "appId eq '$SNOWFLAKE_APP_ID'" --query "[].appId" -o tsv

if (-not $snowflakeSpExists) {
    Write-Log "Snowflake SP not found. Creating local instance..." -Level "INFO"
    try {
        az ad sp create --id $SNOWFLAKE_APP_ID --output none
    } catch {
        Write-Log "WARNING: Could not create Snowflake SP. Ignore if already exists." -Level "WARNING"
    }
}


# =============================================================
# 3. SNOWFLAKE SERVICE PRINCIPAL WITH CONSTRAINED DELEGATION
# =============================================================

Write-Log "Creating Service Principal with Constrained Delegation..." -Level "INFO"
$SUB_ID = (az account show --query id -o tsv)

# Define custom role JSON
$roleName = "Snowflake Access Granter (Restricted)"
$roleFileName = Join-Path $env:TEMP "restricted_role_def.json"

# Built-in Role IDs
$Storage_blob_Data_Contributor_RoleId = "ba92f5b4-2d11-453d-a403-e96b0029c9fe"


$jsonContent = @"
{
  "Name": "$roleName",
  "IsCustom": true,
  "Description": "Can assign roles (restricted via assignment condition)",
  "AssignableScopes": ["/subscriptions/$SUB_ID"],
  "Permissions": [{
    "actions": [
      "Microsoft.Authorization/roleAssignments/read",
      "Microsoft.Authorization/roleAssignments/write",
      "Microsoft.Authorization/roleAssignments/delete"
    ],
    "notActions": [],
    "dataActions": [],
    "notDataActions": []
  }]
}
"@

$jsonContent | Set-Content $roleFileName -Encoding ASCII

Write-Log "Creating custom role definition..." -Level "INFO"
az role definition create --role-definition $roleFileName --output none

Write-Log "Custom role created successfully." -Level "SUCCESS"


# Create Service Principal for GitHub Actions
Write-Log "Creating Service Principal for GitHub Actions..." -Level "INFO"
$spJsonString = (az ad sp create-for-rbac --name $SP_NAME --skip-assignment --output json)
$spObject = $spJsonString | ConvertFrom-Json
$robotId = $spObject.appId

Write-Log "Waiting 20s for AD propagation..." -Level "INFO"
Start-Sleep -Seconds 20


# 1. Contributor Role - for general infra management
Write-Log "Assigning 'Contributor' role..." -Level "INFO"
az role assignment create `
  --assignee $robotId `
  --role "Contributor" `
  --scope "/subscriptions/$SUB_ID" `
  --output none


# 2. Custom Role - only for assigning Storage Blob Data Contributor role
Write-Log "Assigning custom role with ABAC condition..." -Level "INFO"
$condition = "@Request[Microsoft.Authorization/roleAssignments:RoleDefinitionId] ForAnyOfAnyValues:GuidEquals {$Storage_blob_Data_Contributor_RoleId}"

az role assignment create `
  --assignee $robotId `
  --role "$roleName" `
  --scope "/subscriptions/$SUB_ID" `
  --condition $condition `
  --condition-version "2.0" `
  --output none

Write-Log "Both roles assigned successfully." -Level "SUCCESS"


# =============================================================================
# FINAL OUTPUT - GITHUB SECRETS CHEATSHEET
# =============================================================================

Write-Log "Retrieving Snowflake Object ID for automation..." -Level "INFO"
$snowflakeObjectId = az ad sp show --id $SNOWFLAKE_APP_ID --query id -o tsv

Write-Host ""
Write-Log "======================================================" -Level "SUCCESS"
Write-Log "  INFRASTRUCTURE READY! COPY THESE TO GITHUB SECRETS  " -Level "SUCCESS"
Write-Log "======================================================" -Level "SUCCESS"

Write-Host "`n--- 1. COMBINED AZURE CONFIG JSON (Secret) ---" -ForegroundColor Cyan

$safeJson = [PSCustomObject]@{
    clientId             = $spObject.appId
    clientSecret         = $spObject.password
    subscriptionId       = $SUB_ID
    tenantId             = $spObject.tenant
    
    resourceGroupName     = $RG_NAME
    tfStateStorageAccount = $STORAGE_NAME

    snowflakePrincipalId  = $snowflakeObjectId
}

Write-Host "`n--- COPY THIS WHOLE JSON BLOCK ---" -ForegroundColor Cyan
Write-Host "Secret Name:  AZURE_CONFIG" -ForegroundColor Green
Write-Host "Value:" -ForegroundColor Gray
$safeJson | ConvertTo-Json | Write-Host -ForegroundColor Yellow



Write-Host "`n--- 2. MANUAL SNOWFLAKE INPUT (You need to provide these yourself) ---" -ForegroundColor Magenta
Write-Host "Create these secrets with your own Snowflake data:" -ForegroundColor Gray
Write-Host " - SNOWFLAKE_ACCOUNT"
Write-Host " - SNOWFLAKE_USER"
Write-Host " - SNOWFLAKE_PASSWORD"
Write-Host " - SNOWFLAKE_ETL_PASSWORD (Make something up)"
Write-Host " - SNOWFLAKE_BI_PASSWORD  (Make something up)"

Write-Host ""
Write-Log "DONE." -Level "STEP"