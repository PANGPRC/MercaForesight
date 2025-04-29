# MercaForesight - Terraform Configuration

## **Directory Overview**
This folder contains files related to Terraform, a tool used for managing cloud resources in an automated and consistent manner. Terraform enables the provisioning, updating, and versioning of infrastructure resources for the MercaForesight platform.

---

## **Current Files**
- **`main.tf`**: The main configuration file that defines the infrastructure resources.
- **`variables.tf`**: Contains input variables used in the Terraform configuration.
- **`.terraform.lock.hcl`**: Ensures consistent provider versions across different environments.

---

## **Usage Guidelines**
1. Initialize the working directory:
   ```bash
   terraform init
   ```
2. Preview changes before applying them:
   ```bash
   terraform plan
   ```
3. Apply changes to provision the defined resources:
   ```bash
   terraform apply
   ```
4. Destroy resources when they are no longer needed:
   ```bash
   terraform destroy
   ```
   
---

## **Notes**
- Lock File: Do not modify the `.terraform.lock.hcl` file manually; it is automatically managed by Terraform.
- Sensitive Information: Store sensitive information (e.g., credentials) securely using tools like AWS Secrets Manager or Azure Key Vault. Avoid hardcoding them in the configuration files.
- Variable Documentation: Ensure that the `variables.tf` file is properly documented to describe the purpose of each variable.
- State Management: Use a remote backend (e.g., S3, Azure Blob Storage) to store the Terraform state file securely and enable team collaboration.

---