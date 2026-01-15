# =============================================================================
# Cleanup Script - Removes All Resources Created by Bootstrap
# =============================================================================

# Import utility functions
. "$PSScriptRoot\utils.ps1"

$ErrorActionPreference = "Continue"  # Continue on errors to clean up as much as possible

# Variables (same as bootstrap)
$RG_NAME_TFSTATE = "rg-ecommerce-tfstate"
$RG_NAME_DEV = "rg-ecommerce-dev"
$SP_NAME = "sp-ecommerce-github-actions"
$ROLE_NAME = "Snowflake Access Granter (Restricted)"
$SNOWFLAKE_APP_ID = "711e28f3-43c3-48d5-bad8-dfbcb14e6c64"


$SUB_ID = (az account show --query id -o tsv)

# =============================================================================
# 1. REMOVE SERVICE PRINCIPAL
# =============================================================================

Write-Log "Removing Service Principal: $SP_NAME..." -Level "STEP"

# Get SP App ID
$spAppId = az ad sp list --display-name $SP_NAME --query "[0].appId" -o tsv

if ($spAppId) {
    Write-Log "Found Service Principal with App ID: $spAppId" -Level "INFO"
    
    # Remove all role assignments first
    Write-Log "Removing role assignments..." -Level "INFO"
    az role assignment delete --assignee $spAppId --scope "/subscriptions/$SUB_ID" 2>$null
    
    # Delete the Service Principal
    Write-Log "Deleting Service Principal..." -Level "INFO"
    az ad sp delete --id $spAppId 2>$null
    
    # Delete the App Registration
    Write-Log "Deleting App Registration..." -Level "INFO"
    az ad app delete --id $spAppId 2>$null
    
    Write-Log "Service Principal removed." -Level "SUCCESS"
} else {
    Write-Log "Service Principal not found (already deleted or never created)." -Level "INFO"
}

# =============================================================================
# 2. REMOVE CUSTOM ROLE
# =============================================================================

Write-Log "Removing Custom Role: $ROLE_NAME..." -Level "STEP"

$customRoleExists = az role definition list --name $ROLE_NAME --query "[0].name" -o tsv

if ($customRoleExists) {
    Write-Log "Found Custom Role, deleting..." -Level "INFO"
    az role definition delete --name $ROLE_NAME --custom-role-only 2>$null
    Write-Log "Custom Role removed." -Level "SUCCESS"
} else {
    Write-Log "Custom Role not found (already deleted or never created)." -Level "INFO"
}

# =============================================================================
# 3. REMOVE RESOURCE GROUP (includes Storage Account and Container)
# =============================================================================

# Remove TFSTATE Resource Group
Write-Log "Removing Resource Group: $RG_NAME_TFSTATE..." -Level "STEP"

$rgExists = az group exists --name $RG_NAME_TFSTATE

if ($rgExists -eq "true") {
    Write-Log "Found Resource Group, deleting (this may take a few minutes)..." -Level "INFO"
    az group delete --name $RG_NAME_TFSTATE --yes --no-wait
    Write-Log "Resource Group deletion initiated." -Level "SUCCESS"
} else {
    Write-Log "Resource Group not found (already deleted or never created)." -Level "INFO"
}


# Remove DEV Resource Group
Write-Log "Removing Resource Group: $RG_NAME_DEV..." -Level "STEP"

$rgExists = az group exists --name $RG_NAME_DEV

if ($rgExists -eq "true") {
    Write-Log "Found Resource Group, deleting (this may take a few minutes)..." -Level "INFO"
    az group delete --name $RG_NAME_DEV --yes --no-wait
    Write-Log "Resource Group deletion initiated." -Level "SUCCESS"
} else {
    Write-Log "Resource Group not found (already deleted or never created)." -Level "INFO"
}



# =============================================================================
# 4. REMOVE LOCAL FILES
# =============================================================================

Write-Log "Removing local configuration files..." -Level "STEP"

$filesToRemove = @(
    "terraform/backend.conf",
    "restricted_role_def.json",
    "role_create_output.json",
    "TEST_CREDENTIALS_DELETE_ME.json"
)

foreach ($file in $filesToRemove) {
    if (Test-Path $file) {
        Remove-Item $file -Force
        Write-Log "Removed: $file" -Level "INFO"
    }
}

Write-Log "Local files cleaned up." -Level "SUCCESS"

# =============================================================================
# 5. OPTIONAL: REMOVE SNOWFLAKE SERVICE PRINCIPAL (USUALLY DON'T DO THIS)
# =============================================================================

Write-Host ""
Write-Log "NOTE: Snowflake Service Principal ($SNOWFLAKE_APP_ID) was NOT removed." -Level "INFO"
Write-Log "This is a global Microsoft Entra ID object and should remain." -Level "INFO"
Write-Host ""
Write-Host "If you really want to remove it, run:" -ForegroundColor Yellow
Write-Host "az ad sp delete --id $SNOWFLAKE_APP_ID" -ForegroundColor Yellow
Write-Host ""

# =============================================================================
# FINAL SUMMARY
# =============================================================================

Write-Host ""
Write-Log "======================================================" -Level "SUCCESS"
Write-Log "                 CLEANUP COMPLETED                    " -Level "SUCCESS"
Write-Log "======================================================" -Level "SUCCESS"
Write-Host ""

Write-Host "Removed:" -ForegroundColor Cyan
Write-Host "Service Principal: $SP_NAME" -ForegroundColor Green
Write-Host "Custom Role: $ROLE_NAME" -ForegroundColor Green
Write-Host "Resource Group: $RG_NAME_TFSTATE (deletion in progress)" -ForegroundColor Green
Write-Host "Resource Group: $RG_NAME_DEV (deletion in progress)" -ForegroundColor Green
Write-Host "Local configuration files" -ForegroundColor Green
Write-Host ""

Write-Host "You can now run bootstrap.ps1 again to start fresh." -ForegroundColor Cyan
Write-Host ""
Write-Log "DONE." -Level "STEP"