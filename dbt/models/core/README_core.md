# **Core Models**

## **Directory Overview**
The `core` directory contains essential business models that support key business analyses and metric calculations. Each model file serves a specific purpose, generating clear analytical results based on raw data.

---

## **Model List**

### 1. **`mf_inventory_levels.sql`**
- **Purpose**: Calculates inventory-related metrics, including total inventory value, inventory turnover ratio, and inventory aging analysis.
- **Output Fields**:
  - `year`: The year of the data.
  - `month_name`: The name of the month.
  - `total_inventory_value`: The total value of inventory for the corresponding month.
  - `inventory_turnover_ratio`: The inventory turnover ratio.
  - `age_range`: The age range of inventory items.
  - `inventory_value`: The value of inventory items within the specified age range.

---

### 2. **`mf_customer_retention_rate.sql`**
- **Purpose**: Calculates customer retention rate, tracking customer retention by year and month.
- **Output Fields**:
  - `year`: The year of the data.
  - `month_name`: The name of the month.
  - `distinct_customers`: The number of distinct customers in the given month.
  - `rolling_customers`: The cumulative number of distinct customers up to the given month.
  - `customer_retention_rate`: The retention rate of customers for the given month.

---

### 3. **`mf_financial_commitments.sql`**
- **Purpose**: Analyzes the fulfillment rate of financial commitments.
- **Output Fields**:
  - `department_name`: The name of the department.
  - `market_category`: The market category.
  - `category_name`: The name of the product category.
  - `total_committed_funds`: The total committed funds for the department, market, and category.
  - `commitment_fulfillment_rate`: The rate of commitment fulfillment.

---

### 4. **`mf_fraud_detection.sql`**
- **Purpose**: Detects potential fraudulent activities.
- **Output Fields**:
  - `order_customer_id`: The ID of the customer associated with the order.
  - `avg_order_total`: The average order total for the customer.
  - `num_orders`: The number of orders placed by the customer.

---

### 5. **`mf_overall_performance.sql`**
- **Purpose**: Provides overall sales performance and operational efficiency metrics.
- **Output Fields**:
  - `year`: The year of the data.
  - `month_name`: The name of the month.
  - `total_sales`: The total sales for the month.
  - `avg_profit_margin`: The average profit margin for the month.
  - `avg_actual_shipment_days`: The average actual shipment days for the month.
  - `avg_scheduled_shipment_days`: The average scheduled shipment days for the month.

---

### 6. **`mf_payment_delays.sql`**
- **Purpose**: Analyzes payment delays.
- **Output Fields**:
  - `delivery_status`: The status of delivery.
  - `avg_payment_delay`: The average payment delay.
  - `total_orders`: The count of orders.

---

## **Notes**
1. **Data Dependencies**:
   - All models depend on tables from the `staging` layer (e.g., `dim_order`, `dim_customer`, etc.).
   - Ensure that the `staging` layer data is complete and accurate.

2. **Naming Conventions**:
   - All file names use lowercase letters, with words separated by underscores for easy identification and management.

3. **Performance Optimization**:
   - For complex calculations, consider preprocessing data during the ETL stage to improve query performance.

4. **Testing**:
   - Define appropriate tests (e.g., `not_null`, `unique`) for each model to ensure data quality.

---