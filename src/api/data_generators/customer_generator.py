import random
import time
from typing import Dict, Any, Generator
from datetime import datetime

class CustomerGenerator:
    """Generates simulated customer data for testing and development"""
    
    def __init__(self):
        self.segments = ["Premium", "Standard", "Basic", "New"]
        self.cities = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"]
        self.countries = ["USA", "Canada", "UK", "Australia", "Germany"]
        self.customers = self._initialize_customers()
        
    def _initialize_customers(self) -> Dict[str, Dict[str, Any]]:
        """Initialize a set of base customers"""
        return {
            f"CUST-{i:04d}": {
                "customer_id": f"CUST-{i:04d}",
                "email": f"customer{i}@example.com",
                "first_name": f"First{i}",
                "last_name": f"Last{i}",
                "segment": random.choice(self.segments),
                "city": random.choice(self.cities),
                "country": random.choice(self.countries),
                "state": "State",
                "street": f"{random.randint(1, 999)} Main St",
                "zipcode": f"{random.randint(10000, 99999)}",
                "last_updated": datetime.now().isoformat()
            }
            for i in range(1, 101)
        }
    
    def generate_customer_updates(self) -> Generator[Dict[str, Any], None, None]:
        """Generate continuous customer updates"""
        while True:
            # Randomly select a customer to update
            customer_id = random.choice(list(self.customers.keys()))
            customer = self.customers[customer_id]
            
            # Randomly update some fields
            updates = {
                "segment": random.choice(self.segments),
                "city": random.choice(self.cities),
                "country": random.choice(self.countries),
                "last_updated": datetime.now().isoformat()
            }
            
            # Apply updates
            customer.update(updates)
            
            yield customer
            
            # Random delay between updates (0.5 to 2 seconds)
            time.sleep(random.uniform(0.5, 2.0)) 