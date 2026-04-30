output "function_app_name" {
  description = "CEAP ingestion Function App name."
  value       = azurerm_linux_function_app.ceap.name
}

output "function_app_principal_id" {
  description = "System-assigned managed identity principal ID for Function App."
  value       = azurerm_linux_function_app.ceap.identity[0].principal_id
}

output "function_storage_account_name" {
  description = "Function App storage account name."
  value       = azurerm_storage_account.function.name
}

output "ingestion_state_table_name" {
  description = "Table Storage name used for ingestion state."
  value       = azurerm_storage_table.state.name
}
