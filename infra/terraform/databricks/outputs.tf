output "databricks_workspace_id" {
  description = "Azure Databricks workspace resource ID."
  value       = azurerm_databricks_workspace.main.id
}

output "databricks_workspace_url" {
  description = "Azure Databricks workspace URL."
  value       = azurerm_databricks_workspace.main.workspace_url
}

output "databricks_workspace_name" {
  description = "Azure Databricks workspace name."
  value       = azurerm_databricks_workspace.main.name
}

output "databricks_access_connector_id" {
  description = "Databricks Access Connector resource ID."
  value       = azurerm_databricks_access_connector.main.id
}

output "databricks_access_connector_principal_id" {
  description = "Databricks Access Connector managed identity principal ID."
  value       = azurerm_databricks_access_connector.main.identity[0].principal_id
}
