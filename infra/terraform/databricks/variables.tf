variable "subscription_id" {
  type        = string
  description = "Azure subscription ID where Databricks resources are created."
}

variable "location" {
  type        = string
  description = "Azure region for Databricks resources."
  default     = "eastus2"
}

variable "resource_group_name" {
  type        = string
  description = "Main resource group name."
  default     = "rg-legisflow-dev"
}

variable "databricks_workspace_name" {
  type        = string
  description = "Databricks workspace name."
  default     = "dbw-legisflow-dev"
}

variable "databricks_managed_resource_group_name" {
  type        = string
  description = "Managed resource group name used by Azure Databricks."
  default     = "rg-legisflow-dev-databricks-managed"
}

variable "databricks_access_connector_name" {
  type        = string
  description = "Databricks Access Connector name for Unity Catalog external data access."
  default     = "dbc-legisflow-dev-access"
}

variable "lakehouse_storage_account_id" {
  type        = string
  description = "ADLS Gen2 storage account resource ID for Unity Catalog external location access."
}

variable "tags" {
  type        = map(string)
  description = "Common tags applied to all resources."
  default = {
    project     = "legisflow"
    environment = "dev"
    owner       = "daniel"
    managed_by  = "terraform"
    cost_center = "portfolio"
  }
}
