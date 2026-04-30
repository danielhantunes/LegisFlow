# databricks

This module provisions the Azure Databricks foundation for LegisFlow MVP.

## Resources

- Azure Databricks workspace with `premium` SKU (required for Unity Catalog governance features)
- Databricks Access Connector with system-assigned managed identity
- RBAC assignment on ADLS Gen2 (`Storage Blob Data Contributor`) for the Access Connector

## Why Premium SKU

The MVP requires Unity Catalog-oriented capabilities (storage credentials, external locations, governance, centralized metadata management). For that reason, the workspace is provisioned with `premium` tier.

## Backend

- Resource Group: `rg-legisflow-dev-tfstate`
- Storage account: `stlegisflowdevtfstate`
- Container: `tfstate`
- Key: `databricks-dev.tfstate`

## Usage

```bash
terraform init
terraform plan \
  -var "subscription_id=<your-subscription-id>" \
  -var "lakehouse_storage_account_id=<adls-resource-id>"
terraform apply \
  -var "subscription_id=<your-subscription-id>" \
  -var "lakehouse_storage_account_id=<adls-resource-id>"
```

## MVP note

Databricks notebooks, jobs, and DLT pipelines are intentionally managed manually in the workspace during MVP stabilization. Automated asset deployment is deferred to a later phase.
