# base

This module provisions the foundational Azure resources for LegisFlow dev environment.

## Resources

- Resource Group: `rg-legisflow-dev`
- Dedicated ADLS Gen2 storage account (name generated from `stlegisflowdev` + random suffix)
- Lakehouse filesystem/container: `lakehouse`
- User-assigned managed identity for workload access
- RBAC assignment: `Storage Blob Data Contributor` on ADLS for the workload identity
- Log Analytics Workspace
- Application Insights (workspace-based)

## Backend

This module expects the backend created by `bootstrap-tfstate`:

- Resource Group: `rg-legisflow-dev-tfstate`
- Storage account: `stlegisflowdevtfstate`
- Container: `tfstate`
- Key: `base-dev.tfstate`

## Usage

```bash
terraform init
terraform plan -var "subscription_id=<your-subscription-id>"
terraform apply -var "subscription_id=<your-subscription-id>"
```
