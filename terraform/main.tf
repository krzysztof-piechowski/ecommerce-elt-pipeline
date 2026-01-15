# Azure Resource Configuration
data "azurerm_client_config" "current" {}

resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

locals {
  prefix       = "ecommerce"
  env          = "dev"
  location     = "Switzerland North"
  storage_name = "st${local.prefix}${local.env}${random_string.suffix.result}"
  kv_name      = "kv-${local.prefix}-${local.env}-${random_string.suffix.result}"
  adf_name     = "adf-${local.prefix}-${local.env}-${random_string.suffix.result}"

  # Common tags for all resources
  tags = {
    Project     = "Ecommerce-ETL"
    Environment = "Dev"
    Owner       = "Krzysztof"
    ManagedBy   = "Terraform"
  }
}

# 1. Resource Group
resource "azurerm_resource_group" "rg" {
  name     = "rg-${local.prefix}-${local.env}"
  location = local.location
  tags     = local.tags
}

# 2. Storage Account
resource "azurerm_storage_account" "sa" {
  name                     = local.storage_name
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true

  network_rules {
    default_action = "Deny"
    bypass         = ["AzureServices"]
    ip_rules       = var.runner_ip != null ? [var.runner_ip] : []
  }
  
  lifecycle {
    ignore_changes = [
      network_rules[0].virtual_network_subnet_ids
    ]
  }
  tags     = local.tags
}

resource "azurerm_role_assignment" "current_user_blob_owner" {
  scope                = azurerm_storage_account.sa.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}

resource "azurerm_storage_container" "raw" {
  name                  = "raw"
  storage_account_name  = azurerm_storage_account.sa.name
  container_access_type = "private"
  depends_on            = [azurerm_role_assignment.current_user_blob_owner]
}

# 3. Data Factory 
resource "azurerm_data_factory" "adf" {
  name                = local.adf_name
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name

  identity {
    type = "SystemAssigned"
  }

  tags     = local.tags
}

# 4. Key Vault 
resource "azurerm_key_vault" "kv" {
  name                        = local.kv_name
  location                    = azurerm_resource_group.rg.location
  resource_group_name         = azurerm_resource_group.rg.name
  tenant_id                   = data.azurerm_client_config.current.tenant_id
  sku_name                    = "standard"
  soft_delete_retention_days  = 7
  purge_protection_enabled    = false
  enabled_for_disk_encryption = true
  enabled_for_deployment      = true

  # Access for current user (full access)
  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = data.azurerm_client_config.current.object_id
    secret_permissions = ["Get", "List", "Set", "Delete", "Purge"]
  }

  # Access for ADF (so it can read secrets)
  access_policy {
    tenant_id = data.azurerm_client_config.current.tenant_id
    object_id = azurerm_data_factory.adf.identity[0].principal_id
    secret_permissions = ["Get", "List"]
  }

  tags     = local.tags
}