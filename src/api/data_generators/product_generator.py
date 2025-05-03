import random
import time
from typing import Dict, Any, Generator
from datetime import datetime

class ProductGenerator:
    """Generates simulated product data for testing and development"""
    
    def __init__(self):
        self.categories = [
            "Electronics", "Clothing", "Home & Kitchen",
            "Sports & Outdoors", "Books", "Toys & Games"
        ]
        self.statuses = ["active", "inactive", "discontinued"]
        self.products = self._initialize_products()
        
    def _initialize_products(self) -> Dict[str, Dict[str, Any]]:
        """Initialize a set of base products"""
        return {
            f"PROD-{i:04d}": {
                "product_id": f"PROD-{i:04d}",
                "category_id": random.randint(1, 100),
                "category_name": random.choice(self.categories),
                "description": f"Product {i} description",
                "image_url": f"https://example.com/images/product_{i}.jpg",
                "product_name": f"Product {i}",
                "product_price": round(random.uniform(10.0, 1000.0), 2),
                "product_status": random.choice(self.statuses),
                "last_updated": datetime.now().isoformat()
            }
            for i in range(1, 101)
        }
    
    def generate_product_updates(self) -> Generator[Dict[str, Any], None, None]:
        """Generate continuous product updates"""
        while True:
            # Randomly select a product to update
            product_id = random.choice(list(self.products.keys()))
            product = self.products[product_id]
            
            # Randomly update some fields
            updates = {
                "product_price": round(product["product_price"] * random.uniform(0.9, 1.1), 2),
                "product_status": random.choice(self.statuses),
                "last_updated": datetime.now().isoformat()
            }
            
            # Apply updates
            product.update(updates)
            
            yield product
            
            # Random delay between updates (0.1 to 1 second)
            time.sleep(random.uniform(0.1, 1.0)) 