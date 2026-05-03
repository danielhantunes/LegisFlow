# ingestion

This module provisions the shared ingestion runtime (single Function App) for LegisFlow MVP.

## Resources

- Linux Azure Function App on **Flex Consumption** (`FC1`) (`~4`, Python 3.11), default name `func-legisflow-ingestion-dev`
- Dedicated Function storage account
- Blob container used as the Flex Consumption backend package store (`function-releases`)
- Azure Table Storage table for ingestion state control
- System-assigned managed identity for the shared Function App
- RBAC assignment on Lakehouse ADLS: `Storage Blob Data Contributor`

## Inputs expected from base module

- `lakehouse_storage_account_name`
- `lakehouse_storage_account_id`
- Existing App Insights name in the same resource group (`appi-legisflow-dev`)

## Backend

- Resource Group: `rg-legisflow-dev-tfstate`
- Storage account: `stlegisflowdevtfstate`
- Container: `tfstate`
- Key: `ingestion-dev.tfstate`

## Usage

```bash
terraform init
terraform plan \
  -var "subscription_id=<your-subscription-id>" \
  -var "lakehouse_storage_account_name=<adls-name>" \
  -var "lakehouse_storage_account_id=<adls-resource-id>"
terraform apply \
  -var "subscription_id=<your-subscription-id>" \
  -var "lakehouse_storage_account_name=<adls-name>" \
  -var "lakehouse_storage_account_id=<adls-resource-id>"
```
