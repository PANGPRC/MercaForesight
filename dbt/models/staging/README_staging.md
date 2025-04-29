# **Staging Models**

## **Directory Overview**
The `staging` directory contains intermediate models that serve as a bridge between raw data and core business models. These models are used to clean, standardize, and prepare data for further analysis in the `core` directory.

---

## **Purpose**
- Transform raw data into a standardized format.
- Apply basic cleaning and validation rules.
- Create reusable datasets for core business models.

---

## **Model List**

### 1. **`dim_customer.sql`**
- **Purpose**: Prepares customer-related data for analysis.
- **Key Fields**:
  - `customer_id`: Unique identifier for the customer.
  - `email`: Email address of the customer.
  - `first_name`: First name of the customer.
  - `last_name`: Last name of the customer.
  - `segment`: Customer segment or category.
  - `city`, `country`, `state`, `street`, `zipcode`: Address details of the customer.

---

### 2. **`dim_department.sql`**
- **Purpose**: Prepares department-related data for analysis.
- **Key Fields**:
  - `department_id`: Unique identifier for the department.
  - `department_name`: Name of the department.
  - `market_category`: Market category associated with the department.

---

### 3. **`dim_order.sql`**
- **Purpose**: Prepares order-related data for analysis.
- **Key Fields**:
  - `order_id`: Unique identifier for the order.
  - `order_customer_id`: Customer ID associated with the order.
  - `order_item_id`: Unique identifier for the order item.
  - `product_card_id`: Product ID in the order.
  - `department_id`: Department ID associated with the order.
  - `order_date`: Date when the order was placed.
  - `discount_amount`, `discount_rate`: Discount details for the order.
  - `order_price`, `profit_ratio`, `quantity`: Financial and quantity details.
  - `order_status`: Current status of the order.

---

### 4. **`dim_product.sql`**
- **Purpose**: Prepares product-related data for analysis.
- **Key Fields**:
  - `product_id`: Unique identifier for the product.
  - `category_id`: Unique identifier for the product category.
  - `category_name`: Name of the product category.
  - `description`: Detailed description of the product.
  - `image_url`: URL of the product image.
  - `product_name`: Name of the product.
  - `product_price`: Price of the product.
  - `product_status`: Current status of the product.

---

### 5. **`dim_shipping.sql`**
- **Purpose**: Prepares shipping-related data for analysis.
- **Key Fields**:
  - `shipping_date`: Date when the product was shipped.
  - `actual_shipping_days`: Actual number of days taken for shipping.
  - `scheduled_shipping_days`: Scheduled number of days for shipping.
  - `mode`: Mode of shipping (e.g., 'air', 'ground').
  - `delivery_status`: Current status of the delivery.

---

## **Notes**
1. **Data Dependencies**:
   - All models depend on raw data sources (e.g., `raw_customer`, `raw_order`, etc.).
   - Ensure that raw data is complete and accurate before running these models.

2. **Testing**:
   - Define appropriate tests (e.g., `not_null`, `unique`) for each model to ensure data quality.

3. **Performance Optimization**:
   - Use efficient SQL transformations to minimize processing time.

---