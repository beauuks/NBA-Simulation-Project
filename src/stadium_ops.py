import threading
import random
import time
import queue
import logging

from src.database import save_stadium_ops_to_db

class StadiumOperation():
    def __init__(self, game_id, arena_name, operation_type, capacity=18000):
        self.game_id = game_id
        self.arena_name = arena_name
        self.operation_type = operation_type
        self.capacity = capacity
        self.stop_event = threading.Event()
        self.processed_count = 0
        self.queue = queue.Queue()
        self.details = {}
        self.name = f"{arena_name}-{operation_type}"
    
    def run(self):
        logging.info(f"Starting {self.operation_type} at {self.arena_name}")
        
        if self.operation_type == "security":
            self.run_security()
        elif self.operation_type == "concessions":
            self.run_concessions()
        elif self.operation_type == "merchandise":
            self.run_merchandise()
        
        # Save operations data to database
        details_str = str(self.details) if self.details else None
        save_stadium_ops_to_db(
            self.game_id, self.arena_name, self.operation_type,
            self.processed_count, details_str
        )
    
    def run_security(self):
        # Simulate fans entering arena through security
        total_fans = random.randint(int(self.capacity * 0.7), self.capacity)
        entry_rates = {'VIP': 0.1, 'Season': 0.3, 'Regular': 0.6}
        
        self.details['total_fans'] = total_fans
        self.details['entry_types'] = {entry_type: 0 for entry_type in entry_rates}
        
        # Set a time limit rather than processing all fans
        end_time = time.time() + 5  # 5 seconds in real time like other operations
        
        # Process a limited number of fans within the time frame
        while not self.stop_event.is_set() and time.time() < end_time and self.processed_count < total_fans:
            # Determine entry type for current fan
            entry_type = random.choices(
                list(entry_rates.keys()),
                weights=list(entry_rates.values())
            )[0]
            
            # Different processing times based on entry type
            if entry_type == 'VIP':
                time.sleep(random.uniform(0.005, 0.01))  # Fast VIP lane
            elif entry_type == 'Season':
                time.sleep(random.uniform(0.01, 0.03))   # Season ticket holders
            else:
                time.sleep(random.uniform(0.02, 0.05))   # Regular tickets
            
            self.processed_count += 1
            
            # Initialize if the key doesn't exist
            if 'entry_types' not in self.details:
                self.details['entry_types'] = {key: 0 for key in entry_rates.keys()}
            
            # Update entry type count
            self.details['entry_types'][entry_type] = self.details['entry_types'].get(entry_type, 0) + 1
            
            if self.processed_count % 100 == 0:
                logging.info(f"Security: {self.processed_count} fans have entered {self.arena_name}")
        
        # Calculate percentage processed
        self.details['processed_percentage'] = (self.processed_count / total_fans) * 100
        
        # Convert entry_types dictionary to string to ensure it saves properly
        self.details['entry_types_str'] = str(self.details['entry_types'])
        
        logging.info(f"Security completed: {self.processed_count} fans processed at {self.arena_name}")
    
    def run_concessions(self):
        # Simulate concession stands serving food/drinks
        stands = ['Hot Dogs', 'Beverages', 'Popcorn', 'Nachos', 'Pizza']
        stand_sales = {stand: 0 for stand in stands}
        stand_revenue = {stand: 0 for stand in stands}
        
        # Price list
        prices = {
            'Hot Dogs': 8.50,
            'Beverages': 6.00,
            'Popcorn': 7.50,
            'Nachos': 9.00,
            'Pizza': 10.50
        }
        
        # Generate sales for a period of time (simulated)
        end_time = time.time() + 5  # 5 seconds in real time
        
        while not self.stop_event.is_set() and time.time() < end_time:
            # Process a sale
            stand = random.choice(stands)
            quantity = random.randint(1, 3)
            stand_sales[stand] += quantity
            revenue = prices[stand] * quantity
            stand_revenue[stand] += revenue
            
            self.processed_count += quantity
            
            # Simulate transaction time
            time.sleep(random.uniform(0.01, 0.08))
            
            if self.processed_count % 50 == 0:
                logging.info(f"Concessions: {self.processed_count} orders processed at {self.arena_name}")
            
            # Store details
            self.details['stand_sales'] = stand_sales
            self.details['stand_revenue'] = stand_revenue
            self.details['total_revenue'] = sum(stand_revenue.values())
            
        logging.info(f"Concessions completed: {self.processed_count} orders processed at {self.arena_name}")
        logging.info(f"Total concessions revenue: ${self.details['total_revenue']:.2f}")
    
    def run_merchandise(self):
        # Simulate merchandise sales
        products = ['Jersey', 'Cap', 'T-shirt', 'Basketball', 'Poster']
        sales = {product: 0 for product in products}
        revenue = {product: 0 for product in products}
        
        # Price list
        prices = {
            'Jersey': 120.00,
            'Cap': 35.00,
            'T-shirt': 45.00,
            'Basketball': 60.00,
            'Poster': 25.00
        }
        
        # Generate sales for 3 hours (simulated time)
        end_time = time.time() + 5  # 5 seconds in real time
        
        while not self.stop_event.is_set() and time.time() < end_time:
            # Process a sale
            product = random.choice(products)
            quantity = random.randint(1, 2)
            sales[product] += quantity
            revenue[product] += prices[product] * quantity
            
            self.processed_count += quantity
            
            # Simulate transaction time
            time.sleep(random.uniform(0.01, 0.1))
            
            if self.processed_count % 20 == 0:
                logging.info(f"Merchandise: {self.processed_count} items sold at {self.arena_name}")
        
        # Store details
        self.details['sales'] = sales
        self.details['revenue'] = revenue
        self.details['total_revenue'] = sum(revenue.values())
        
        # Report final sales
        logging.info(f"Merchandise sales at {self.arena_name}:")
        for product, count in sales.items():
            logging.info(f"  - {product}: {count} units, ${revenue[product]:.2f}")
        logging.info(f"Total: {self.processed_count} items sold, ${self.details['total_revenue']:.2f} revenue")
