from typing import List

# Schema definitions for dimension extraction

CUSTOMER_DIM_SCHEMA: List[str] = [
    "Customer Id", "Customer Email", "Customer Fname", "Customer Lname",
    "Customer Segment", "Customer City", "Customer Country",
    "Customer State", "Customer Street", "Customer Zipcode"
]

PRODUCT_DIM_SCHEMA: List[str] = [
    "Product Card Id", "Product Category Id", "Category Name", "Product Description",
    "Product Image", "Product Name", "Product Price", "Product Status"
]

LOCATION_DIM_SCHEMA: List[str] = [
    "Order Zipcode", "Order City", "Order State", "Order Region", "Order Country",
    "Latitude", "Longitude"
]

ORDER_DIM_SCHEMA: List[str] = [
    "Order Id", "Order date (DateOrders)", "Order Customer Id", "Order Item Id", "Product Card Id",
    "Order Item Discount", "Order Item Discount Rate", "Order Item Product Price",
    "Order Item Profit Ratio", "Order Item Quantity", "Sales per customer", "Sales",
    "Order Item Total", "Order Profit Per Order", "Order Status", "Department Id"
]

SHIPPING_DIM_SCHEMA: List[str] = [
    "Shipping date (DateOrders)", "Days for shipping (real)", "Days for shipment (scheduled)",
    "Shipping Mode", "Delivery Status"
]

DEPARTMENT_DIM_SCHEMA: List[str] = [
    "Department Id", "Department Name", "Market"
]

METADATA_DIM_SCHEMA: List[str] = [
    "key", "offset", "partition", "time", "topic"
]


