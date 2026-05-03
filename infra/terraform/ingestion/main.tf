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
    "FUNCTIONS_WORKER_RUNTIME" = "python"
    "WEBSITE_RUN_FROM_PACKAGE" = "1"
    "CEAP_TIMER_SCHEDULE"      = var.ceap_timer_schedule
    "INGESTION_STATE_TABLE"    = azurerm_storage_table.state.name
    "RAW_STORAGE_ACCOUNT_NAME" = var.lakehouse_storage_account_name
    "MAX_RETRY_ATTEMPTS"       = tostring(var.max_retry_attempts)
  }

  tags = var.tags
}

resource "azurerm_role_assignment" "function_blob_contributor" {
  scope                = var.lakehouse_storage_account_id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_function_app_flex_consumption.ingestion.identity[0].principal_id
}
