# Terraform Configuration

This folder contains files related to Terraform, used for managing cloud resources.

## Current Files
- `main.tf`: The main configuration file that defines the infrastructure resources.
- `variables.tf`: Contains input variables used in the Terraform configuration.
- `.terraform.lock.hcl`: Ensures consistent provider versions across different environments.

## Usage Guidelines
- Run `terraform init` to initialize the working directory.
- Use `terraform plan` to preview changes before applying them.
- Apply changes with `terraform apply` to provision the defined resources.

## Notes
- Do not modify the `.terraform.lock.hcl` file manually; it is automatically managed by Terraform.
- Store sensitive information (e.g., credentials) securely and avoid hardcoding them in the configuration files.
- Ensure that the `variables.tf` file is properly documented to describe the purpose of each variable.