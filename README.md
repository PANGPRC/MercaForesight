# MercaForesight

*Be patient.*

MercaForesight is an intelligent analytics platform designed for e-commerce platforms and operations teams, aiming to optimize business decisions through data-driven insights.

## Project Overview
The project focuses on:
- SKU performance analysis and optimization.
- Dynamic inventory management.
- Real-time financial and order insights.

## Project Structure

| Path                       | Description                                      |
|----------------------------|--------------------------------------------------|
| `.github/workflows/`       | CI/CD configuration files                        |
| `data/`                    | Data storage directory                           |
| `data/raw/`                | Raw, unprocessed data files                      |
| `data/processed/`          | Processed, cleaned, and aggregated data files    |
| `dbt/`                     | Files related to the DBT data modeling tool      |
| `dbt/models/`              | DBT models for data transformation               |
| `dbt/macros/`              | Reusable SQL macros for transformations          |
| `docs/`                    | Project documentation                            |
| `infra/`                   | Infrastructure configuration files               |
| `infra/docker/`            | Docker configuration files                       |
| `infra/terraform/`         | Terraform files for managing cloud resources     |
| `scripts/`                 | Automation scripts                               |
| `src/`                     | Source code                                      |
| `src/utils/`               | Utility functions and common modules             |
| `src/streaming/`           | Streaming data processing tasks                  |
| `src/batch/`               | Batch processing tasks                           |
| `tests/`                   | Test code                                        |

For more details, refer to the `README` files in each subdirectory.

## Visualization

Run superset

```bash
git clone https://github.com/apache/superset
cd superset
git checkout tags/4.1.2
docker compose -f docker-compose-image-tag.yml up
```

Login

```
username: admin
password: admin
```
