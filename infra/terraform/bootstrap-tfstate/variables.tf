variable "subscription_id" {
  type        = string
  description = "Azure subscription ID where tfstate backend resources are created."
}

variable "location" {
  type        = string
  description = "Azure region for tfstate resources."
  default     = "eastus2"
}

variable "resource_group_name" {
  type        = string
  description = "Resource group name for Terraform remote state resources."
  default     = "rg-legisflow-dev-tfstate"
}

variable "storage_account_name" {
  type        = string
  description = "Storage account name for Terraform remote state."
  default     = "stlegisflowdevtfstate"
}

variable "container_name" {
  type        = string
  description = "Blob container that stores Terraform state files."
  default     = "tfstate"
}

variable "tags" {
  type        = map(string)
  description = "Common tags applied to all resources."
  default = {
    project      = "legisflow"
    environment  = "dev"
    owner        = "daniel"
    managed_by   = "terraform"
    cost_center  = "portfolio"
  }
}
