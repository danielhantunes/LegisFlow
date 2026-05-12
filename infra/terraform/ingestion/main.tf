data "azurerm_application_insights" "main" {
  name                = var.application_insights_name
  resource_group_name = var.resource_group_name
}

resource "random_string" "func_storage_suffix" {
  length  = 5
  upper   = false
  special = false
  numeric = true
}

locals {
  function_storage_account_name = substr(
    lower("${var.function_storage_account_name_prefix}${random_string.func_storage_suffix.result}"),
    0,
    24
  )
  ceap_api_poison_queue_name             = "${var.ceap_api_queue_name}-poison"
  reference_snapshot_poison_queue_name   = "${var.reference_snapshot_queue_name}-poison"
  votacoes_poison_queue_name             = "${var.votacoes_queue_name}-poison"
}

resource "azurerm_storage_account" "function" {
  name                     = local.function_storage_account_name
  resource_group_name      = var.resource_group_name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  access_tier              = "Hot"
  min_tls_version          = "TLS1_2"

  allow_nested_items_to_be_public = false
  shared_access_key_enabled       = true

  tags = var.tags
}

resource "azurerm_storage_container" "function_package" {
  name                  = "function-releases"
  storage_account_id    = azurerm_storage_account.function.id
  container_access_type = "private"
}

resource "azurerm_storage_table" "state" {
  name                 = var.state_table_name
  storage_account_name = azurerm_storage_account.function.name
}

resource "azurerm_storage_table" "control_api_2026" {
  name                 = var.control_api_table_name
  storage_account_name = azurerm_storage_account.function.name
}

resource "azurerm_storage_queue" "ceap_api_work" {
  name                 = var.ceap_api_queue_name
  storage_account_name = azurerm_storage_account.function.name
}

resource "azurerm_storage_queue" "ceap_api_poison" {
  name                 = local.ceap_api_poison_queue_name
  storage_account_name = azurerm_storage_account.function.name
}

# Reference snapshot domain queues (work + poison).
resource "azurerm_storage_queue" "reference_snapshot_work" {
  name                 = var.reference_snapshot_queue_name
  storage_account_name = azurerm_storage_account.function.name
}

resource "azurerm_storage_queue" "reference_snapshot_poison" {
  name                 = local.reference_snapshot_poison_queue_name
  storage_account_name = azurerm_storage_account.function.name
}

# Votacoes domain queues (work + poison).
resource "azurerm_storage_queue" "votacoes_work" {
  name                 = var.votacoes_queue_name
  storage_account_name = azurerm_storage_account.function.name
}

resource "azurerm_storage_queue" "votacoes_poison" {
  name                 = local.votacoes_poison_queue_name
  storage_account_name = azurerm_storage_account.function.name
}

resource "azurerm_service_plan" "functions" {
  name                = var.app_service_plan_name
  resource_group_name = var.resource_group_name
  location            = var.location
  os_type             = "Linux"
  # Flex Consumption avoids classic Linux Consumption (Y1) "Dynamic VMs" quota requirements
  # and is the recommended path for Linux Functions going forward.
  sku_name = "FC1"
  tags     = var.tags
}

resource "azurerm_function_app_flex_consumption" "ingestion" {
  name                = var.function_app_name
  resource_group_name = var.resource_group_name
  location            = var.location

  service_plan_id = azurerm_service_plan.functions.id

  storage_container_type      = "blobContainer"
  storage_container_endpoint  = "${azurerm_storage_account.function.primary_blob_endpoint}${azurerm_storage_container.function_package.name}"
  storage_authentication_type = "StorageAccountConnectionString"
  storage_access_key          = azurerm_storage_account.function.primary_access_key

  runtime_name    = "python"
  runtime_version = "3.11"

  https_only = true

  maximum_instance_count = 50
  instance_memory_in_mb  = 2048

  site_config {
    application_insights_key               = data.azurerm_application_insights.main.instrumentation_key
    application_insights_connection_string = data.azurerm_application_insights.main.connection_string
  }

  identity {
    type = "SystemAssigned"
  }

  app_settings = {
    "FUNCTIONS_EXTENSION_VERSION" = "~4"
    # Explicit storage connection for Queue triggers (worker/poison). Flex host may not
    # wire queue listeners to the same credential shape as AzureWebJobsStorage alone.
    "CEAP_QUEUE_STORAGE"       = azurerm_storage_account.function.primary_connection_string
    "CEAP_TIMER_SCHEDULE"      = var.ceap_timer_schedule
    "INGESTION_STATE_TABLE"    = azurerm_storage_table.state.name
    "INGESTION_CONTROL_TABLE"  = azurerm_storage_table.control_api_2026.name
    "CEAP_API_QUEUE_NAME"      = azurerm_storage_queue.ceap_api_work.name
    "CEAP_API_POISON_QUEUE_NAME" = azurerm_storage_queue.ceap_api_poison.name
    "CEAP_API_2026_DISPATCH_SCHEDULE" = var.ceap_timer_schedule
    "CEAP_API_YEAR"            = "2026"
    "CEAP_TARGET_YEAR"         = "2026"
    "CEAP_RECONCILIATION_DAY"  = "25"
    "CEAP_DAILY_LOOKBACK_MONTHS" = "1"
    "CEAP_STALE_AFTER_MINUTES" = "60"
    "CEAP_REFERENCE_TIMEZONE"  = "America/Sao_Paulo"
    "CEAP_RECONCILIATION_START_MONTH" = "1"
    "CEAP_MAX_TASKS_PER_DISPATCH" = "1000"
    "CEAP_DISPATCH_MAX_MESSAGES" = "1000"
    "CEAP_LEGACY_MONOLITH_ENABLED" = "false"
    "AzureWebJobs.ceap_expenses_ingestion_timer.Disabled" = "true"
    "RAW_STORAGE_ACCOUNT_NAME" = var.lakehouse_storage_account_name
    "LAKEHOUSE_FILESYSTEM_NAME" = "lakehouse"
    "MAX_RETRY_ATTEMPTS"       = tostring(var.max_retry_attempts)

    # ----- Reference snapshot domain ----------------------------------------
    "REFERENCE_SNAPSHOT_DISPATCH_SCHEDULE" = var.reference_snapshot_dispatch_schedule
    "REFERENCE_SNAPSHOT_QUEUE_NAME"        = azurerm_storage_queue.reference_snapshot_work.name
    "REFERENCE_SNAPSHOT_POISON_QUEUE_NAME" = azurerm_storage_queue.reference_snapshot_poison.name
    "REFERENCE_TIMEZONE"                   = var.reference_timezone
    "REFERENCE_LOCK_TTL_MINUTES"           = tostring(var.reference_lock_ttl_minutes)
    "ENABLE_REFERENCE_RESET_FUNCTION"      = var.enable_reference_reset_function ? "true" : "false"

    # ----- Votacoes domain --------------------------------------------------
    "VOTACOES_DISPATCH_SCHEDULE"        = var.votacoes_dispatch_schedule
    "VOTACOES_DISPATCH_GRANULARITY_MIN" = tostring(var.votacoes_dispatch_granularity_min)
    "VOTACOES_LOOKBACK_MINUTES"         = tostring(var.votacoes_lookback_minutes)
    "VOTACOES_QUEUE_NAME"               = azurerm_storage_queue.votacoes_work.name
    "VOTACOES_POISON_QUEUE_NAME"        = azurerm_storage_queue.votacoes_poison.name
    "VOTACOES_LOCK_TTL_MINUTES"         = tostring(var.votacoes_lock_ttl_minutes)
    "VOTACOES_MAX_MESSAGES_PER_TICK"    = tostring(var.votacoes_max_messages_per_tick)
    "VOTACOES_MAX_LIST_PAGES"           = tostring(var.votacoes_max_list_pages)
    "ENABLE_VOTACOES_RESET_FUNCTION"    = var.enable_votacoes_reset_function ? "true" : "false"

    # ----- Global admin -----------------------------------------------------
    "ENABLE_RESET_FUNCTIONS" = var.enable_reset_functions ? "true" : "false"
  }

  tags = var.tags
}

resource "azurerm_role_assignment" "function_blob_contributor" {
  scope                = var.lakehouse_storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_function_app_flex_consumption.ingestion.identity[0].principal_id
}
