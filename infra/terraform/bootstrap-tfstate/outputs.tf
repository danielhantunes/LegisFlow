output "tfstate_resource_group_name" {
  description = "Terraform state resource group name."
  value       = azurerm_resource_group.tfstate.name
}

output "tfstate_storage_account_name" {
  description = "Terraform state storage account name."
  value       = azurerm_storage_account.tfstate.name
}

output "tfstate_container_name" {
  description = "Terraform state blob container name."
  value       = azurerm_storage_container.tfstate.name
}
