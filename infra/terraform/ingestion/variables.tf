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

variable "control_api_table_name" {
  type        = string
  description = "Table Storage for CEAP API 2026 per-unit control (logical model: ingestion_control_api_2026). Azure allows alphanumeric only."
  default     = "IngestionControlApi2026"
}

variable "ceap_api_queue_name" {
  type        = string
  description = "Queue name for CEAP API 2026 work messages (main queue)."
  default     = "ceap-api-2026-work"
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

# ---------------------------------------------------------------------------
# Reference snapshot domain (/partidos, /legislaturas, /deputados, /frentes,
# /orgaos)
# ---------------------------------------------------------------------------

variable "reference_snapshot_queue_name" {
  type        = string
  description = "Queue name for reference snapshot work messages."
  default     = "reference-snapshot-work"
}

variable "reference_snapshot_dispatch_schedule" {
  type        = string
  description = "CRON for the reference snapshot dispatcher (every 20 minutes during validation)."
  default     = "0 */20 * * * *"
}

variable "reference_timezone" {
  type        = string
  description = "Timezone used to derive reference_snapshot_YYYYMMDD."
  default     = "America/Sao_Paulo"
}

variable "reference_lock_ttl_minutes" {
  type        = number
  description = "Dispatcher lock TTL in minutes for reference domain."
  default     = 15
}

variable "enable_reference_reset_function" {
  type        = bool
  description = "Domain-specific feature flag for the reference reset HTTP function."
  default     = false
}

# ---------------------------------------------------------------------------
# Votações domain (/votacoes + /votacoes/{id}/votos, microbatch + fanout)
# ---------------------------------------------------------------------------

variable "votacoes_queue_name" {
  type        = string
  description = "Queue name for votacoes work messages."
  default     = "votacoes-api-work"
}

variable "votacoes_dispatch_schedule" {
  type        = string
  description = "CRON for the votacoes dispatcher (every 10 minutes during validation)."
  default     = "0 */10 * * * *"
}

variable "votacoes_dispatch_granularity_min" {
  type        = number
  description = "Minute granularity used to derive the microbatch pipeline_run_id (e.g. 10 → ...22:30)."
  default     = 10
}

variable "votacoes_lookback_minutes" {
  type        = number
  description = "How far back the votacoes dispatcher scans /votacoes on every tick."
  default     = 60
}

variable "votacoes_lock_ttl_minutes" {
  type        = number
  description = "Dispatcher lock TTL in minutes for votacoes domain."
  default     = 15
}

variable "votacoes_max_messages_per_tick" {
  type        = number
  description = "Cap on fanout messages enqueued by a single votacoes dispatcher tick."
  default     = 500
}

variable "votacoes_max_list_pages" {
  type        = number
  description = "Maximum pages the votacoes dispatcher will fetch from /votacoes per tick."
  default     = 200
}

variable "enable_votacoes_reset_function" {
  type        = bool
  description = "Domain-specific feature flag for the votacoes reset HTTP function."
  default     = false
}

# ---------------------------------------------------------------------------
# Global admin
# ---------------------------------------------------------------------------

variable "enable_reset_functions" {
  type        = bool
  description = "Global kill-switch enabling all *_reset HTTP functions across domains."
  default     = false
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
