variable "subscription_id" {
  type        = string
  description = "Azure subscription ID where base resources are created."
}

variable "location" {
  type        = string
  description = "Azure region for base resources."
  default     = "eastus2"
}

variable "resource_group_name" {
  type        = string
  description = "Main resource group name."
  default     = "rg-legisflow-dev"
}

variable "storage_account_name_prefix" {
  type        = string
  description = "Prefix for ADLS Gen2 storage account name."
  default     = "stlegisflowdev"
}

variable "lakehouse_filesystem_name" {
  type        = string
  description = "ADLS Gen2 filesystem used as the main lakehouse container."
  default     = "lakehouse"
}

variable "managed_identity_name" {
  type        = string
  description = "User-assigned managed identity name for workloads."
  default     = "id-legisflow-dev-workload"
}

variable "log_analytics_workspace_name" {
  type        = string
  description = "Log Analytics workspace name."
  default     = "log-legisflow-dev"
}

variable "application_insights_name" {
  type        = string
  description = "Application Insights resource name."
  default     = "appi-legisflow-dev"
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
