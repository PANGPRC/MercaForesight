import random
import time
from typing import Dict, Any, Generator
from datetime import datetime

class AccessRecordGenerator:
    """Generates simulated access record data for testing and development"""
    
    def __init__(self):
        self.access_types = ["view", "search", "add_to_cart", "purchase"]
        self.device_types = ["desktop", "mobile", "tablet"]
        self.session_durations = range(30, 1800)  # 30 seconds to 30 minutes
        
    def generate_access_records(self) -> Generator[Dict[str, Any], None, None]:
        """Generate continuous access records"""
        while True:
            # Generate a new access record
            record = {
                "record_id": f"ACC-{int(time.time())}",
                "customer_id": f"CUST-{random.randint(1, 100):04d}",
                "product_id": f"PROD-{random.randint(1, 100):04d}",
                "access_time": datetime.now().isoformat(),
                "access_type": random.choice(self.access_types),
                "session_duration": random.choice(self.session_durations),
                "device_type": random.choice(self.device_types)
            }
            
            yield record
            
            # Random delay between records (0.1 to 0.5 seconds)
            time.sleep(random.uniform(0.1, 0.5)) 