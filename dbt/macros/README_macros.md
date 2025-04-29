# MercaForesight - DBT Macros

## **Directory Overview**
This folder contains reusable SQL macros for the DBT (Data Build Tool) project. Macros are used to simplify and standardize SQL transformations across models, ensuring consistency and reducing redundancy.

---

## **Purpose**
- Provide reusable logic for common SQL operations.
- Simplify complex transformations by encapsulating logic in macros.
- Enhance maintainability and readability of DBT models.

---

## **Key Macros**

### 1. **`safe_cast_macro.sql`**
- **Purpose**: Safely casts a column to a specified data type, ensuring compatibility and avoiding errors.
- **Usage**:
  ```sql
  {{ safe_cast("column_name", "data_type") }}
  ```
- **Example**:
  ```sql
  {{ safe_cast("order_date", "string") }}
  ```

---

## **Usage Guidelines**
- Place all reusable macros in this directory.
- Use descriptive names for macros to indicate their purpose.
- Document each macro with comments explaining its functionality and usage.

---

## **Notes**
- Test macros thoroughly to ensure they work as expected in different scenarios.
- Avoid hardcoding values within macros; use parameters to make them flexible.
- Regularly review and refactor macros to improve performance and maintainability.

---