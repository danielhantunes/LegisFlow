output "resource_group_name" {
  description = "Main resource group name."
  value       = azurerm_resource_group.main.name
}

output "lakehouse_storage_account_name" {
  description = "ADLS Gen2 storage account name."
  value       = azurerm_storage_account.lakehouse.name
}

output "lakehouse_storage_account_id" {
  description = "ADLS Gen2 storage account resource ID."
  value       = azurerm_storage_account.lakehouse.id
}

output "lakehouse_filesystem_name" {
  description = "Primary ADLS Gen2 filesystem name."
  value       = azurerm_storage_data_lake_gen2_filesystem.lakehouse.name
}

output "managed_identity_id" {
  description = "User-assigned managed identity resource ID."
  value       = azurerm_user_assigned_identity.workload.id
}

output "managed_identity_principal_id" {
  description = "User-assigned managed identity principal ID."
  value       = azurerm_user_assigned_identity.workload.principal_id
}

output "application_insights_connection_string" {
  description = "Application Insights connection string."
  value       = azurerm_application_insights.main.connection_string
  sensitive   = true
}
