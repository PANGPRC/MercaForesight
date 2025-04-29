# MercaForesight - DBT Models

## **Directory Overview**
This folder contains files related to DBT (Data Build Tool) for data modeling, transformation, and analytics. DBT is used to build modular, maintainable, and reusable data pipelines.

---

## **Purpose**
- Transform raw data into clean, analysis-ready datasets.
- Define and manage data models for business metrics and insights.
- Ensure data quality through testing and validation.
- Enable collaboration and version control for data transformations.

---

## **Project Structure**

### 1. **`models/`**
- **Purpose**: Contains SQL models organized into subdirectories:
  - **`staging/`**: Intermediate models for cleaning and standardizing raw data.
  - **`core/`**: Core business models for key metrics and analyses.

### 2. **`macros/`**
- **Purpose**: Contains reusable SQL macros to simplify and standardize transformations across models.

### 3. **`config/`**
- **Purpose**: Stores configuration files, such as `dbt_project.yml` and other settings required for DBT to run properly.
- **Usage**: Define project-level configurations, such as model materialization settings, target profiles, and folder paths.

---

## **Key Features**
- **Data Transformation**: Transform raw data into clean, analysis-ready datasets.
- **Modular Design**: Models are organized into logical layers (`staging`, `core`) for better maintainability.

---

## **How to Use**

### 1. **Set Up the Environment**
- Install DBT: Follow the [DBT installation guide](https://docs.getdbt.com/docs/installation).
- Configure the `profiles.yml` file to connect to your database.

### 2. **Run DBT Commands**
- `dbt run`: Executes all models and materializes them in the database.
- `dbt docs generate`: Generates documentation for the project.
- `dbt docs serve`: Serves the documentation locally for exploration.

### 3. **Best Practices**
- Use version control (e.g., Git) to manage changes to DBT models.
- Regularly test models to ensure data quality.
- Document all transformations and business logic in the corresponding `README` files.

---

## **Notes**
- Ensure that raw data is complete and accurate before running DBT models.
- Avoid hardcoding sensitive information in SQL files; use environment variables instead.
- Regularly update and maintain the `README` files for each subdirectory to reflect changes in the project structure or logic.

---