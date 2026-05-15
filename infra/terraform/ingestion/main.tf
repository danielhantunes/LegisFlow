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
  proposicoes_poison_queue_name          = "${var.proposicoes_queue_name}-poison"
  eventos_poison_queue_name              = "${var.eventos_queue_name}-poison"
  institucional_poison_queue_name        = "${var.institucional_queue_name}-poison"
  discursos_poison_queue_name            = "${var.discursos_queue_name}-poison"
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

# Proposicoes domain queues (work + poison).
resource "azurerm_storage_queue" "proposicoes_work" {
  name                 = var.proposicoes_queue_name
  storage_account_name = azurerm_storage_account.function.name
}

resource "azurerm_storage_queue" "proposicoes_poison" {
  name                 = local.proposicoes_poison_queue_name
  storage_account_name = azurerm_storage_account.function.name
}

# Eventos domain queues (work + poison).
resource "azurerm_storage_queue" "eventos_work" {
  name                 = var.eventos_queue_name
  storage_account_name = azurerm_storage_account.function.name
}

resource "azurerm_storage_queue" "eventos_poison" {
  name                 = local.eventos_poison_queue_name
  storage_account_name = azurerm_storage_account.function.name
}

# Institucional domain queues (work + poison).
resource "azurerm_storage_queue" "institucional_work" {
  name                 = var.institucional_queue_name
  storage_account_name = azurerm_storage_account.function.name
}

resource "azurerm_storage_queue" "institucional_poison" {
  name                 = local.institucional_poison_queue_name
  storage_account_name = azurerm_storage_account.function.name
}

# Discursos domain queues (work + poison).
resource "azurerm_storage_queue" "discursos_work" {
  name                 = var.discursos_queue_name
  storage_account_name = azurerm_storage_account.function.name
}

resource "azurerm_storage_queue" "discursos_poison" {
  name                 = local.discursos_poison_queue_name
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
    "CEAP_DAILY_LOOKBACK_MONTHS" = "0"
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
    "LOG_LEVEL"                = var.log_level

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
    "VOTACOES_RECONCILIATION_DAY"     = tostring(var.votacoes_reconciliation_day)
    "VOTACOES_MICROBATCH_SAFETY_WINDOW_HOURS" = tostring(var.votacoes_microbatch_safety_window_hours)
    "VOTACOES_RECON_MAX_PAGES_PER_TICK" = tostring(var.votacoes_recon_max_pages_per_tick)
    "TARGET_YEAR"                       = tostring(var.votacoes_target_year)
    "ENABLE_VOTACOES_RESET_FUNCTION"    = var.enable_votacoes_reset_function ? "true" : "false"
    "ENABLE_MANUAL_RECONCILIATION_FUNCTIONS" = var.enable_manual_votacoes_reconciliation_function ? "true" : "false"

    # ----- Proposicoes domain ----------------------------------------------
    "PROPOSICOES_DAILY_DISPATCH_SCHEDULE"             = var.proposicoes_daily_dispatch_schedule
    "PROPOSICOES_RECONCILIATION_DISPATCH_SCHEDULE"   = var.proposicoes_reconciliation_dispatch_schedule
    "PROPOSICOES_LOOKBACK_MINUTES"         = tostring(var.proposicoes_lookback_minutes)
    "PROPOSICOES_MICROBATCH_LOOKBACK_DAYS" = tostring(var.proposicoes_microbatch_lookback_days)
    "PROPOSICOES_QUEUE_NAME"               = azurerm_storage_queue.proposicoes_work.name
    "PROPOSICOES_POISON_QUEUE_NAME"        = azurerm_storage_queue.proposicoes_poison.name
    "PROPOSICOES_LOCK_TTL_MINUTES"         = tostring(var.proposicoes_lock_ttl_minutes)
    "PROPOSICOES_MAX_MESSAGES_PER_TICK"    = tostring(var.proposicoes_max_messages_per_tick)
    "PROPOSICOES_MAX_LIST_PAGES"           = tostring(var.proposicoes_max_list_pages)
    "PROPOSICOES_RECONCILIATION_DAY"       = tostring(var.proposicoes_reconciliation_day)
    "PROPOSICOES_RECON_MAX_PAGES_PER_TICK" = tostring(var.proposicoes_recon_max_pages_per_tick)
    "ENABLE_PROPOSICOES_RESET_FUNCTION"    = var.enable_proposicoes_reset_function ? "true" : "false"

    # ----- Eventos domain ---------------------------------------------------
    "EVENTOS_DAILY_DISPATCH_SCHEDULE"             = var.eventos_daily_dispatch_schedule
    "EVENTOS_RECONCILIATION_DISPATCH_SCHEDULE"    = var.eventos_reconciliation_dispatch_schedule
    "EVENTOS_DAILY_FUTURE_DAYS"                   = tostring(var.eventos_daily_future_days)
    "EVENTOS_RECONCILIATION_PAST_DAYS"            = tostring(var.eventos_reconciliation_past_days)
    "EVENTOS_RECONCILIATION_FUTURE_DAYS"          = tostring(var.eventos_reconciliation_future_days)
    "EVENTOS_QUEUE_NAME"               = azurerm_storage_queue.eventos_work.name
    "EVENTOS_POISON_QUEUE_NAME"        = azurerm_storage_queue.eventos_poison.name
    "EVENTOS_LOCK_TTL_MINUTES"         = tostring(var.eventos_lock_ttl_minutes)
    "EVENTOS_MAX_MESSAGES_PER_TICK"    = tostring(var.eventos_max_messages_per_tick)
    "EVENTOS_MAX_LIST_PAGES"           = tostring(var.eventos_max_list_pages)
    "EVENTOS_RECON_MAX_PAGES_PER_TICK" = tostring(var.eventos_recon_max_pages_per_tick)
    "ENABLE_EVENTOS_RESET_FUNCTION"    = var.enable_eventos_reset_function ? "true" : "false"

    # ----- Institucional domain --------------------------------------------
    "INSTITUCIONAL_DISPATCH_SCHEDULE"     = var.institucional_dispatch_schedule
    "INSTITUCIONAL_QUEUE_NAME"            = azurerm_storage_queue.institucional_work.name
    "INSTITUCIONAL_POISON_QUEUE_NAME"     = azurerm_storage_queue.institucional_poison.name
    "INSTITUCIONAL_LOCK_TTL_MINUTES"      = tostring(var.institucional_lock_ttl_minutes)
    "INSTITUCIONAL_MAX_MESSAGES_PER_TICK" = tostring(var.institucional_max_messages_per_tick)
    "INSTITUCIONAL_MAX_LIST_PAGES"        = tostring(var.institucional_max_list_pages)
    "ENABLE_INSTITUCIONAL_RESET_FUNCTION" = var.enable_institucional_reset_function ? "true" : "false"

    # ----- Discursos domain -------------------------------------------------
    "DISCURSOS_DAILY_DISPATCH_SCHEDULE"             = var.discursos_daily_dispatch_schedule
    "DISCURSOS_RECONCILIATION_DISPATCH_SCHEDULE"    = var.discursos_reconciliation_dispatch_schedule
    "DISCURSOS_DAILY_LOOKBACK_DAYS"                 = tostring(var.discursos_daily_lookback_days)
    "DISCURSOS_QUEUE_NAME"               = azurerm_storage_queue.discursos_work.name
    "DISCURSOS_POISON_QUEUE_NAME"        = azurerm_storage_queue.discursos_poison.name
    "DISCURSOS_LOCK_TTL_MINUTES"         = tostring(var.discursos_lock_ttl_minutes)
    "DISCURSOS_MAX_MESSAGES_PER_TICK"    = tostring(var.discursos_max_messages_per_tick)
    "DISCURSOS_MAX_LIST_PAGES"           = tostring(var.discursos_max_list_pages)
    "DISCURSOS_RECON_MAX_LIST_PAGES"     = tostring(var.discursos_recon_max_list_pages)
    "DISCURSOS_RECON_MAX_PAGES_PER_TICK" = tostring(var.discursos_recon_max_pages_per_tick)
    "ENABLE_DISCURSOS_RESET_FUNCTION"    = var.enable_discursos_reset_function ? "true" : "false"

    # ----- Daily consolidated summary -------------------------------------
    "DAILY_SUMMARY_ENABLED"                 = var.daily_summary_enabled ? "true" : "false"
    "DAILY_SUMMARY_EXPECTED_DOMAINS"        = var.daily_summary_expected_domains
    "DAILY_SUMMARY_REFERENCE_TIMEZONE"      = var.daily_summary_reference_timezone
    "DAILY_SUMMARY_CREATE_SUCCESS_MARKER"   = var.daily_summary_create_success_marker ? "true" : "false"
    "DAILY_SUMMARY_CRON"                    = var.daily_summary_cron

    # ----- Global admin -----------------------------------------------------
    "ENABLE_CURRENT_YEAR_BACKFILL_FUNCTION" = var.enable_current_year_backfill_function ? "true" : "false"
    "RECONCILIATION_SCHEDULER_SCHEDULE"      = var.reconciliation_scheduler_schedule
    "ENABLE_RECONCILIATION_SCHEDULER"        = var.enable_reconciliation_scheduler ? "true" : "false"
    "ENABLE_RECONCILIATION_CONTROL_HTTP"    = var.enable_reconciliation_control_http ? "true" : "false"
    "PROPOSICOES_USE_CONTROLLED_RECONCILIATION" = var.proposicoes_use_controlled_reconciliation ? "true" : "false"
    "ENABLE_RESET_FUNCTIONS"                = var.enable_reset_functions ? "true" : "false"
  }

  tags = var.tags
}

resource "azurerm_role_assignment" "function_blob_contributor" {
  scope                = var.lakehouse_storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_function_app_flex_consumption.ingestion.identity[0].principal_id
}
