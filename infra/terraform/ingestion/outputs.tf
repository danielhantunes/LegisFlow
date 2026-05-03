output "function_app_name" {
  description = "Shared ingestion Function App name."
  value       = azurerm_function_app_flex_consumption.ingestion.name
}

output "function_app_principal_id" {
  description = "System-assigned managed identity principal ID for Function App."
  value       = azurerm_function_app_flex_consumption.ingestion.identity[0].principal_id
}

output "function_storage_account_name" {
  description = "Function App storage account name."
  value       = azurerm_storage_account.function.name
}

output "ingestion_state_table_name" {
  description = "Table Storage name used for ingestion state."
  value       = azurerm_storage_table.state.name
}

output "ingestion_control_api_table_name" {
  description = "Table Storage for CEAP API 2026 unit-level control."
  value       = azurerm_storage_table.control_api_2026.name
}

output "ceap_api_queue_name" {
  description = "Main queue for CEAP API 2026 work messages."
  value       = azurerm_storage_queue.ceap_api_work.name
}

output "ceap_api_poison_queue_name" {
  description = "Poison queue for CEAP API 2026 after max dequeue retries."
  value       = azurerm_storage_queue.ceap_api_poison.name
}
