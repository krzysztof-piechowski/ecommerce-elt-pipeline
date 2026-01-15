#--- OUTPUTS FOR THE RESOURCES CREATED ---
output "resource_group_name" { value = azurerm_resource_group.rg.name }
output "storage_account_name" { value = azurerm_storage_account.sa.name }
output "container_name" { value = azurerm_storage_container.raw.name }
output "data_factory_name" { value = azurerm_data_factory.adf.name }
output "key_vault_name" { value = azurerm_key_vault.kv.name }