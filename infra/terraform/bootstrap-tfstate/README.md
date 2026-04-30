# bootstrap-tfstate

This module bootstraps the Azure remote backend resources used by other Terraform modules.

## Resources created

- Resource Group: `rg-legisflow-dev-tfstate`
- Storage Account: `stlegisflowdevtfstate`
- Blob Container: `tfstate`

## Usage

Run locally:

```bash
terraform init
terraform plan -var "subscription_id=<your-subscription-id>"
terraform apply -var "subscription_id=<your-subscription-id>"
```

After apply, configure other modules with `backend "azurerm"`:

```hcl
terraform {
  backend "azurerm" {
    resource_group_name  = "rg-legisflow-dev-tfstate"
    storage_account_name = "stlegisflowdevtfstate"
    container_name       = "tfstate"
    key                  = "base-dev.tfstate"
  }
}
```
