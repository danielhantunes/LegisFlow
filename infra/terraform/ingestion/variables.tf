variable "subscription_id" {
  type        = string
  description = "Azure subscription ID where ingestion resources are created."
}

variable "location" {
  type        = string
  description = "Azure region for ingestion resources."
  default     = "eastus2"
}

variable "resource_group_name" {
  type        = string
  description = "Main resource group name for ingestion resources."
  default     = "rg-legisflow-dev"
}

variable "function_app_name" {
  type        = string
  description = "Shared Function App name for ingestion workloads."
  default     = "func-legisflow-ingestion-dev"
}

variable "function_storage_account_name_prefix" {
  type        = string
  description = "Prefix for Function App storage account."
  default     = "stlegisflowfuncdev"
}

variable "state_table_name" {
  type        = string
  description = "Table Storage table name for ingestion state."
  default     = "IngestionState"
}

variable "app_service_plan_name" {
  type        = string
  description = "App Service Plan name for Azure Functions (Flex Consumption SKU FC1)."
  default     = "asp-legisflow-dev-functions"
}

variable "application_insights_name" {
  type        = string
  description = "Existing Application Insights name."
  default     = "appi-legisflow-dev"
}

variable "lakehouse_storage_account_name" {
  type        = string
  description = "Existing ADLS Gen2 storage account name for raw/bronze/silver/gold."
}

variable "lakehouse_storage_account_id" {
  type        = string
  description = "Existing ADLS Gen2 storage account resource ID for RBAC assignment."
}

variable "ceap_timer_schedule" {
  type        = string
  description = "CRON schedule for CEAP ingestion timer trigger."
  default     = "0 */20 * * * *"
}

variable "max_retry_attempts" {
  type        = number
  description = "Maximum retry attempts for ingestion partitions."
  default     = 3
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
