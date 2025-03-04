import threading
import random
import time
import queue
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(message)s'
)

# Thread-safe data structures
game_results = {}
game_lock = threading.Lock()
stats_queue = queue.Queue()

class NBA_Game(threading.Thread):
    def __init__(self, team1, team2, game_id):
        super().__init__(name=f"Game-{team1}-vs-{team2}")
        self.team1 = team1
        self.team2 = team2
        self.game_id = game_id
        self.score = {team1: 0, team2: 0}
        self.player_stats = {}
        self.quarters_completed = 0
        self.events = []
        self.event_lock = threading.Lock()
        self.game_ended = threading.Event()
    
    def add_event(self, event):
        with self.event_lock:
            self.events.append((time.time(), event))
            logging.info(f"[{self.team1} vs {self.team2}] {event}")
    
    def simulate_quarter(self, quarter):
        """Simulate a quarter of basketball"""
        self.add_event(f"Quarter {quarter} started")
        
        # Simulate possessions for this quarter
        possessions = random.randint(20, 30)
        for _ in range(possessions):
            offense_team = random.choice([self.team1, self.team2])
            defense_team = self.team2 if offense_team == self.team1 else self.team1
            
            # Simulate a possession
            play_type = random.choices(
                ['2PT', '3PT', 'FT', 'TO'], 
                weights=[0.55, 0.25, 0.10, 0.10]
            )[0]
            
            if play_type == '2PT':
                success = random.random() < 0.45  # 45% success rate
                if success:
                    self.score[offense_team] += 2
                    self.add_event(f"{offense_team} scores 2 points")
                else:
                    self.add_event(f"{offense_team} misses a shot, {defense_team} rebounds")
            
            elif play_type == '3PT':
                success = random.random() < 0.35  # 35% success rate
                if success:
                    self.score[offense_team] += 3
                    self.add_event(f"{offense_team} scores a three-pointer!")
                else:
                    self.add_event(f"{offense_team} misses a three-point attempt")
            
            elif play_type == 'FT':
                shots = random.randint(1, 3)
                made = 0
                for _ in range(shots):
                    if random.random() < 0.75:  # 75% free throw success
                        made += 1
                
                self.score[offense_team] += made
                self.add_event(f"{offense_team} makes {made} of {shots} free throws")
            
            elif play_type == 'TO':
                self.add_event(f"{offense_team} turns the ball over to {defense_team}")
            
            # Short sleep to simulate real-time
            time.sleep(0.1)
        
        self.add_event(f"Quarter {quarter} ended. Score: {self.team1} {self.score[self.team1]} - {self.team2} {self.score[self.team2]}")
        self.quarters_completed += 1
    
    def run(self):
        self.add_event(f"🏀 Game started!")
        
        # Simulate 4 quarters
        for quarter in range(1, 5):
            self.simulate_quarter(quarter)
            
            # Short break between quarters
            if quarter < 4:
                self.add_event("Quarter break")
                time.sleep(1)
        
        # Determine winner
        if self.score[self.team1] == self.score[self.team2]:
            # Simulate overtime
            self.add_event("Game tied! Going to overtime")
            self.simulate_quarter(5)
        
        winner = max(self.score, key=self.score.get)
        self.add_event(f"🏆 Final Score: {self.team1} {self.score[self.team1]} - {self.team2} {self.score[self.team2]}")
        self.add_event(f"🎉 Winner: {winner}")
        
        # Store game results safely
        with game_lock:
            game_results[self.game_id] = {
                'team1': self.team1,
                'team2': self.team2,
                'score1': self.score[self.team1],
                'score2': self.score[self.team2],
                'winner': winner,
                'events': self.events
            }
        
        # Signal that the game has ended
        self.game_ended.set()

class StadiumOperation(threading.Thread):
    def __init__(self, arena_name, operation_type, capacity=18000):
        super().__init__(name=f"{arena_name}-{operation_type}")
        self.arena_name = arena_name
        self.operation_type = operation_type
        self.capacity = capacity
        self.stop_event = threading.Event()
        self.processed_count = 0
        self.queue = queue.Queue()
    
    def run(self):
        logging.info(f"Starting {self.operation_type} at {self.arena_name}")
        
        if self.operation_type == "security":
            self.run_security()
        elif self.operation_type == "concessions":
            self.run_concessions()
        elif self.operation_type == "merchandise":
            self.run_merchandise()
    
    def run_security(self):
        # Simulate fans entering arena through security
        total_fans = random.randint(int(self.capacity * 0.7), self.capacity)
        
        # Fill the queue with fans to process
        for i in range(total_fans):
            self.queue.put(f"Fan-{i+1}")
        
        # Process fans through security
        while not self.stop_event.is_set() and not self.queue.empty():
            fan = self.queue.get()
            # Simulate security check (takes between 0.01-0.05 seconds)
            time.sleep(random.uniform(0.01, 0.05))
            self.processed_count += 1
            
            if self.processed_count % 100 == 0:
                logging.info(f"Security: {self.processed_count} fans have entered {self.arena_name}")
            
            self.queue.task_done()
        
        logging.info(f"Security completed: {self.processed_count} fans processed at {self.arena_name}")
    
    def run_concessions(self):
        # Simulate concession stands serving food/drinks
        # Create different concession stands
        stands = ['Hot Dogs', 'Beverages', 'Popcorn', 'Nachos', 'Pizza']
        stand_queues = {stand: queue.Queue() for stand in stands}
        
        # Generate random orders
        order_count = random.randint(1000, 3000)
        for i in range(order_count):
            stand = random.choice(stands)
            stand_queues[stand].put(f"Order-{i+1}")
        
        # Process orders
        while not self.stop_event.is_set() and any(not q.empty() for q in stand_queues.values()):
            for stand, q in stand_queues.items():
                if not q.empty():
                    order = q.get()
                    # Processing time depends on stand type
                    time.sleep(random.uniform(0.05, 0.2))
                    self.processed_count += 1
                    
                    if self.processed_count % 50 == 0:
                        logging.info(f"Concessions: {self.processed_count} orders processed at {self.arena_name}")
                    
                    q.task_done()
        
        logging.info(f"Concessions completed: {self.processed_count} orders processed at {self.arena_name}")
    
    def run_merchandise(self):
        # Simulate merchandise sales
        products = ['Jersey', 'Cap', 'T-shirt', 'Basketball', 'Poster']
        sales = {product: 0 for product in products}
        
        # Generate sales for 3 hours (simulated time)
        end_time = time.time() + 10  # 10 seconds in real time
        
        while not self.stop_event.is_set() and time.time() < end_time:
            # Process a sale
            product = random.choice(products)
            sales[product] += 1
            self.processed_count += 1
            
            # Simulate transaction time
            time.sleep(random.uniform(0.01, 0.1))
            
            if self.processed_count % 20 == 0:
                logging.info(f"Merchandise: {self.processed_count} items sold at {self.arena_name}")
        
        # Report final sales
        logging.info(f"Merchandise sales at {self.arena_name}:")
        for product, count in sales.items():
            logging.info(f"  - {product}: {count} units")
        logging.info(f"Total: {self.processed_count} items sold")

def simulate_parallel_games(game_schedule):
    """Simulate multiple NBA games in parallel using thread pool"""
    with ThreadPoolExecutor(max_workers=len(game_schedule)) as executor:
        # Submit all games to the thread pool
        futures = []
        for game_id, (team1, team2, arena) in enumerate(game_schedule):
            # Start stadium operations first
            security = StadiumOperation(arena, "security")
            concessions = StadiumOperation(arena, "concessions")
            merchandise = StadiumOperation(arena, "merchandise")
            
            security.start()
            concessions.start()
            merchandise.start()
            
            # Create and start the game
            game = NBA_Game(team1, team2, game_id)
            futures.append(executor.submit(game.run))
            
            # Wait for all games to complete
            for future in futures:
                future.result()
            
            # Stop stadium operations
            security.stop_event.set()
            concessions.stop_event.set()
            merchandise.stop_event.set()
            
            security.join()
            concessions.join()
            merchandise.join()

def simulate_conferences(east_schedule, west_schedule):
    """Simulate eastern and western conference games using multiprocessing"""
    with ProcessPoolExecutor(max_workers=2) as executor:
        # Submit each conference's games to separate processes
        east_future = executor.submit(simulate_parallel_games, east_schedule)
        west_future = executor.submit(simulate_parallel_games, west_schedule)
        
        # Wait for both conferences to complete their games
        east_future.result()
        west_future.result()

# Example usage
if __name__ == "__main__":
    # Example game schedules
    eastern_games = [
        ("Boston Celtics", "Miami Heat", "TD Garden"),
        ("Milwaukee Bucks", "Philadelphia 76ers", "Fiserv Forum"),
        ("New York Knicks", "Toronto Raptors", "Madison Square Garden")
    ]
    
    western_games = [
        ("Los Angeles Lakers", "Golden State Warriors", "Staples Center"),
        ("Phoenix Suns", "Dallas Mavericks", "Footprint Center"),
        ("Denver Nuggets", "Portland Trail Blazers", "Ball Arena")
    ]
    
    # Simulate games
    logging.info("Starting NBA simulation")
    
    # Option 1: Simulate specific conference games in parallel
    simulate_conferences(eastern_games, western_games)
    
    # Option 2: Simulate all games in parallel threads
    # all_games = eastern_games + western_games
    # simulate_parallel_games(all_games)
    
    # Print results
    logging.info("Simulation completed!")
    logging.info("Game Results:")
    for game_id, result in game_results.items():
        logging.info(f"Game {game_id}: {result['team1']} {result['score1']} - {result['team2']} {result['score2']}")
        logging.info(f"Winner: {result['winner']}")