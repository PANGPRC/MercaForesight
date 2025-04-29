# DBT Models

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

---

## **Key Features**
- **Data Transformation**: Transform raw data into clean, analysis-ready datasets.
- **Modular Design**: Models are organized into logical layers (`staging`, `core`) for better maintainability.
- **Testing**: Built-in tests (`not_null`, `unique`, etc.) and custom tests ensure data quality.
- **Reusability**: Macros and seeds enable consistent and reusable transformations.

---

## **How to Use**

### 1. **Set Up the Environment**
- Install DBT: Follow the [DBT installation guide](https://docs.getdbt.com/docs/installation).
- Configure the `profiles.yml` file to connect to your database.

### 2. **Run DBT Commands**
- `dbt run`: Executes all models and materializes them in the database.
- `dbt test`: Runs tests to validate data quality.
- `dbt seed`: Loads static data from the `seeds/` directory into the database.
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