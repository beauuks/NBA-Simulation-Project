import threading
import random
import time
import queue
import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import pandas as pd
from datetime import datetime

# Try importing NBA API (install with: pip install nba_api)
try:
    from nba_api.stats.endpoints import LeagueGameFinder, CommonTeamRoster, BoxScoreTraditionalV2
    NBA_API_AVAILABLE = True
except ImportError:
    logging.warning("NBA API not available. Install with: pip install nba_api")
    NBA_API_AVAILABLE = False

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(message)s',
    handlers=[
        logging.FileHandler("nba_simulation.log"),
        logging.StreamHandler()
    ]
)

# Thread-safe data structures
game_results = {}
game_lock = threading.Lock()
stats_queue = queue.Queue()

# Database connection setup
def init_database():
    """Initialize the database with required tables"""
    conn = sqlite3.connect('nba_simulation.db')
    cursor = conn.cursor()
    
    # Create games table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS games (
        id INTEGER PRIMARY KEY,
        team1 TEXT,
        team2 TEXT,
        score1 INTEGER,
        score2 INTEGER,
        winner TEXT,
        arena TEXT,
        game_date TEXT
    )
    ''')
    
    # Create player stats table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS player_stats (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game_id INTEGER,
        player_name TEXT,
        team TEXT,
        points INTEGER,
        rebounds INTEGER,
        assists INTEGER,
        steals INTEGER,
        blocks INTEGER,
        FOREIGN KEY (game_id) REFERENCES games (id)
    )
    ''')
    
    # Create stadium operations table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stadium_ops (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game_id INTEGER,
        arena TEXT,
        operation_type TEXT,
        processed_count INTEGER,
        details TEXT,
        FOREIGN KEY (game_id) REFERENCES games (id)
    )
    ''')
    
    conn.commit()
    conn.close()
    logging.info("Database initialized successfully")

def save_game_to_db(game_id, result):
    """Save game results to database"""
    conn = sqlite3.connect('nba_simulation.db')
    cursor = conn.cursor()
    
    # Insert game result
    cursor.execute(
        "INSERT OR REPLACE INTO games VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (game_id, result['team1'], result['team2'], 
         result['score1'], result['score2'], result['winner'],
         result.get('arena', 'Unknown Arena'), 
         result.get('date', datetime.now().strftime('%Y-%m-%d')))
    )
    
    # Insert player stats if available
    if 'player_stats' in result:
        for player, stats in result['player_stats'].items():
            cursor.execute(
                "INSERT INTO player_stats (game_id, player_name, team, points, rebounds, assists, steals, blocks) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (game_id, player, stats['team'], stats['points'], stats['rebounds'], 
                 stats['assists'], stats['steals'], stats['blocks'])
            )
    
    conn.commit()
    conn.close()

def save_stadium_ops_to_db(game_id, arena, operation_type, processed_count, details=None):
    """Save stadium operations data to database"""
    conn = sqlite3.connect('nba_simulation.db')
    cursor = conn.cursor()
    
    cursor.execute(
        "INSERT INTO stadium_ops (game_id, arena, operation_type, processed_count, details) VALUES (?, ?, ?, ?, ?)",
        (game_id, arena, operation_type, processed_count, details or "")
    )
    
    conn.commit()
    conn.close()

def get_team_roster(team_id):
    """Get player roster for a team using NBA API"""
    if not NBA_API_AVAILABLE:
        # Return dummy roster if API not available
        return [f"Player{i}" for i in range(1, 16)]
    
    try:
        roster = CommonTeamRoster(team_id=team_id).get_data_frames()[0]
        return roster['PLAYER_NAME'].tolist()
    except Exception as e:
        logging.error(f"Error fetching roster for team {team_id}: {e}")
        return [f"Player{i}" for i in range(1, 16)]

class Player:
    def __init__(self, name, team):
        self.name = name
        self.team = team
        self.stats = {
            'points': 0,
            'rebounds': 0,
            'assists': 0,
            'steals': 0,
            'blocks': 0,
            'fouls': 0,
            'minutes': 0
        }
    
    def update_stat(self, stat_name, value=1):
        """Update a player statistic"""
        self.stats[stat_name] += value
    
    def get_stats_dict(self):
        """Return stats as a dictionary with team included"""
        result = self.stats.copy()
        result['team'] = self.team
        return result

class NBA_Game(threading.Thread):
    def __init__(self, team1, team2, game_id, arena=None, date=None, team1_id=None, team2_id=None):
        super().__init__(name=f"Game-{team1}-vs-{team2}")
        self.team1 = team1
        self.team2 = team2
        self.game_id = game_id
        self.arena = arena or f"{team1} Arena"
        self.date = date or datetime.now().strftime('%Y-%m-%d')
        self.team1_id = team1_id
        self.team2_id = team2_id
        
        self.score = {team1: 0, team2: 0}
        self.quarters_completed = 0
        self.events = []
        self.event_lock = threading.Lock()
        self.game_ended = threading.Event()
        
        # Initialize players
        self.players = {}
        self.initialize_players()
    
    def initialize_players(self):
        """Initialize player rosters for both teams"""
        # Team 1 players
        roster1 = get_team_roster(self.team1_id) if self.team1_id else [f"{self.team1}Player{i}" for i in range(1, 16)]
        for player_name in roster1:
            self.players[player_name] = Player(player_name, self.team1)
        
        # Team 2 players
        roster2 = get_team_roster(self.team2_id) if self.team2_id else [f"{self.team2}Player{i}" for i in range(1, 16)]
        for player_name in roster2:
            self.players[player_name] = Player(player_name, self.team2)
    
    def add_event(self, event):
        with self.event_lock:
            self.events.append((time.time(), event))
            logging.info(f"[{self.team1} vs {self.team2}] {event}")
    
    def get_random_player(self, team):
        """Get a random player from a team"""
        team_players = [p for p_name, p in self.players.items() if p.team == team]
        return random.choice(team_players) if team_players else None
    
    def simulate_quarter(self, quarter):
        """Simulate a quarter of basketball"""
        self.add_event(f"Quarter {quarter} started")
        
        # Simulate possessions for this quarter
        possessions = random.randint(20, 30)
        for _ in range(possessions):
            offense_team = random.choice([self.team1, self.team2])
            defense_team = self.team2 if offense_team == self.team1 else self.team1
            
            # Get random players for this play
            offense_player = self.get_random_player(offense_team)
            defense_player = self.get_random_player(defense_team)
            
            if not offense_player or not defense_player:
                continue
            
            # Simulate a possession
            play_type = random.choices(
                ['2PT', '3PT', 'FT', 'TO', 'STEAL', 'BLOCK'], 
                weights=[0.45, 0.25, 0.10, 0.10, 0.05, 0.05]
            )[0]
            
            if play_type == '2PT':
                success = random.random() < 0.45  # 45% success rate
                if success:
                    self.score[offense_team] += 2
                    offense_player.update_stat('points', 2)
                    
                    # Possible assist
                    if random.random() < 0.6:  # 60% of made shots are assisted
                        assisting_player = self.get_random_player(offense_team)
                        if assisting_player and assisting_player != offense_player:
                            assisting_player.update_stat('assists')
                            self.add_event(f"{offense_player.name} scores 2 points, assisted by {assisting_player.name}")
                    else:
                        self.add_event(f"{offense_player.name} scores 2 points")
                else:
                    # Rebound opportunity
                    if random.random() < 0.7:  # 70% defensive rebounds
                        rebounder = self.get_random_player(defense_team)
                        if rebounder:
                            rebounder.update_stat('rebounds')
                            self.add_event(f"{offense_player.name} misses a shot, {rebounder.name} rebounds")
                    else:
                        rebounder = self.get_random_player(offense_team)
                        if rebounder:
                            rebounder.update_stat('rebounds')
                            self.add_event(f"{offense_player.name} misses a shot, offensive rebound by {rebounder.name}")
            
            elif play_type == '3PT':
                success = random.random() < 0.35  # 35% success rate
                if success:
                    self.score[offense_team] += 3
                    offense_player.update_stat('points', 3)
                    
                    # Possible assist
                    if random.random() < 0.8:  # 80% of 3PT are assisted
                        assisting_player = self.get_random_player(offense_team)
                        if assisting_player and assisting_player != offense_player:
                            assisting_player.update_stat('assists')
                            self.add_event(f"{offense_player.name} scores a three-pointer, assisted by {assisting_player.name}")
                    else:
                        self.add_event(f"{offense_player.name} scores a three-pointer!")
                else:
                    # Rebound opportunity
                    if random.random() < 0.75:  # 75% defensive rebounds on 3PT misses
                        rebounder = self.get_random_player(defense_team)
                        if rebounder:
                            rebounder.update_stat('rebounds')
                            self.add_event(f"{offense_player.name} misses a three-point attempt, {rebounder.name} rebounds")
                    else:
                        rebounder = self.get_random_player(offense_team)
                        if rebounder:
                            rebounder.update_stat('rebounds')
                            self.add_event(f"{offense_player.name} misses a three-point attempt, offensive rebound by {rebounder.name}")
            
            elif play_type == 'FT':
                shots = random.randint(1, 3)
                made = 0
                for _ in range(shots):
                    if random.random() < 0.75:  # 75% free throw success
                        made += 1
                
                if made > 0:
                    self.score[offense_team] += made
                    offense_player.update_stat('points', made)
                
                self.add_event(f"{offense_player.name} makes {made} of {shots} free throws")
            
            elif play_type == 'TO':
                self.add_event(f"{offense_player.name} turns the ball over to {defense_team}")
            
            elif play_type == 'STEAL':
                defense_player.update_stat('steals')
                self.add_event(f"{defense_player.name} steals the ball from {offense_player.name}")
            
            elif play_type == 'BLOCK':
                defense_player.update_stat('blocks')
                self.add_event(f"{defense_player.name} blocks {offense_player.name}'s shot")
            
            # Short sleep to simulate real-time
            time.sleep(0.05)
        
        self.add_event(f"Quarter {quarter} ended. Score: {self.team1} {self.score[self.team1]} - {self.team2} {self.score[self.team2]}")
        self.quarters_completed += 1
    
    def run(self):
        self.add_event(f"🏀 Game started at {self.arena}!")
        
        # Simulate 4 quarters
        for quarter in range(1, 5):
            self.simulate_quarter(quarter)
            
            # Short break between quarters
            if quarter < 4:
                self.add_event("Quarter break")
                time.sleep(0.5)
        
        # Determine winner
        if self.score[self.team1] == self.score[self.team2]:
            # Simulate overtime
            self.add_event("Game tied! Going to overtime")
            self.simulate_quarter(5)
        
        winner = max(self.score, key=self.score.get)
        self.add_event(f"🏆 Final Score: {self.team1} {self.score[self.team1]} - {self.team2} {self.score[self.team2]}")
        self.add_event(f"🎉 Winner: {winner}")
        
        # Prepare player stats
        player_stats = {player.name: player.get_stats_dict() for player in self.players.values()}
        
        # Store game results safely
        with game_lock:
            game_results[self.game_id] = {
                'team1': self.team1,
                'team2': self.team2,
                'score1': self.score[self.team1],
                'score2': self.score[self.team2],
                'winner': winner,
                'events': self.events,
                'arena': self.arena,
                'date': self.date,
                'player_stats': player_stats
            }
        
        # Save to database
        save_game_to_db(self.game_id, game_results[self.game_id])
        
        # Signal that the game has ended
        self.game_ended.set()

class StadiumOperation(threading.Thread):
    def __init__(self, game_id, arena_name, operation_type, capacity=18000):
        super().__init__(name=f"{arena_name}-{operation_type}")
        self.game_id = game_id
        self.arena_name = arena_name
        self.operation_type = operation_type
        self.capacity = capacity
        self.stop_event = threading.Event()
        self.processed_count = 0
        self.queue = queue.Queue()
        self.details = {}
    
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
        
        # Fill the queue with fans to process
        for i in range(total_fans):
            # Determine entry type
            entry_type = random.choices(
                list(entry_rates.keys()),
                weights=list(entry_rates.values())
            )[0]
            
            self.queue.put((f"Fan-{i+1}", entry_type))
        
        # Process fans through security
        while not self.stop_event.is_set() and not self.queue.empty():
            fan, entry_type = self.queue.get()
            
            # Different processing times based on entry type
            if entry_type == 'VIP':
                time.sleep(random.uniform(0.005, 0.01))  # Fast VIP lane
            elif entry_type == 'Season':
                time.sleep(random.uniform(0.01, 0.03))   # Season ticket holders
            else:
                time.sleep(random.uniform(0.02, 0.05))   # Regular tickets
            
            self.processed_count += 1
            self.details['entry_types'][entry_type] += 1
            
            if self.processed_count % 100 == 0:
                logging.info(f"Security: {self.processed_count} fans have entered {self.arena_name}")
            
            self.queue.task_done()
        
        self.details['processed_percentage'] = (self.processed_count / total_fans) * 100
        logging.info(f"Security completed: {self.processed_count} fans processed at {self.arena_name}")
    
    def run_concessions(self):
        # Simulate concession stands serving food/drinks
        stands = ['Hot Dogs', 'Beverages', 'Popcorn', 'Nachos', 'Pizza']
        stand_queues = {stand: queue.Queue() for stand in stands}
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
        
        # Generate random orders
        order_count = random.randint(1000, 3000)
        for i in range(order_count):
            stand = random.choice(stands)
            quantity = random.randint(1, 3)
            stand_queues[stand].put((f"Order-{i+1}", quantity))
        
        # Process orders
        while not self.stop_event.is_set() and any(not q.empty() for q in stand_queues.values()):
            for stand, q in stand_queues.items():
                if not q.empty():
                    order, quantity = q.get()
                    # Processing time depends on stand type and quantity
                    time.sleep(random.uniform(0.05, 0.1) * quantity)
                    
                    stand_sales[stand] += quantity
                    revenue = prices[stand] * quantity
                    stand_revenue[stand] += revenue
                    
                    self.processed_count += 1
                    
                    if self.processed_count % 50 == 0:
                        logging.info(f"Concessions: {self.processed_count} orders processed at {self.arena_name}")
                    
                    q.task_done()
        
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

def fetch_nba_games(season='2023-24', num_games=10):
    """Fetch real NBA games from the NBA API"""
    if not NBA_API_AVAILABLE:
        logging.warning("NBA API not available. Using dummy data.")
        return [
            ("Boston Celtics", "Miami Heat", "TD Garden", "BOS", "MIA"),
            ("Milwaukee Bucks", "Philadelphia 76ers", "Fiserv Forum", "MIL", "PHI"),
            ("Los Angeles Lakers", "Golden State Warriors", "Crypto.com Arena", "LAL", "GSW")
        ]
    
    try:
        # Get games from the NBA API
        games = LeagueGameFinder(season_nullable=season).get_data_frames()[0]
        
        # Filter completed games
        # games = games[games['WL'].notnull()]
        
        # Get a subset of most recent games
        games = games.sort_values(by='GAME_DATE', ascending=False).head(num_games)
        
        # Extract useful information
        game_data = []
        for _, game in games.iterrows():
            # Each game appears twice in the API (once for each team)
            # We only want to process each game once
            if game['MATCHUP'].startswith('@'):  # Away game
                continue
                
            home_team = game['TEAM_NAME']
            away_team = game['MATCHUP'].split(' ')[1]  # Extract opponent
            arena = "NBA Arena"  # Arena info not directly available
            game_date = game['GAME_DATE']
            home_team_id = game['TEAM_ID']
            away_team_id = None  # Would need another API call to get this
            
            game_data.append((home_team, away_team, arena, game_date, home_team_id, away_team_id))
        
        logging.info(f"Fetched {len(game_data)} NBA games from the API")
        return game_data
        
    except Exception as e:
        logging.error(f"Error fetching NBA games: {e}")
        return [
            ("Boston Celtics", "Miami Heat", "TD Garden", datetime.now().strftime('%Y-%m-%d'), None, None),
            ("Milwaukee Bucks", "Philadelphia 76ers", "Fiserv Forum", datetime.now().strftime('%Y-%m-%d'), None, None),
            ("Los Angeles Lakers", "Golden State Warriors", "Crypto.com Arena", datetime.now().strftime('%Y-%m-%d'), None, None)
        ]

def simulate_parallel_games(game_schedule):
    """Simulate multiple NBA games in parallel using thread pool"""
    with ThreadPoolExecutor(max_workers=len(game_schedule)) as executor:
        # Submit all games to the thread pool
        futures = []
        stadium_threads = []
        
        for game_id, game_info in enumerate(game_schedule):
            # Extract game information
            if len(game_info) >= 6:  # Full info including IDs
                team1, team2, arena, game_date, team1_id, team2_id = game_info
            elif len(game_info) >= 4:  # With date
                team1, team2, arena, game_date = game_info
                team1_id = team2_id = None
            else:  # Basic info
                team1, team2, arena = game_info
                game_date = datetime.now().strftime('%Y-%m-%d')
                team1_id = team2_id = None
            
            # Start stadium operations first
            security = StadiumOperation(game_id, arena, "security")
            concessions = StadiumOperation(game_id, arena, "concessions")
            merchandise = StadiumOperation(game_id, arena, "merchandise")
            
            security.start()
            concessions.start()
            merchandise.start()
            
            stadium_threads.extend([security, concessions, merchandise])
            
            # Create and start the game
            game = NBA_Game(team1, team2, game_id, arena, game_date, team1_id, team2_id)
            futures.append(executor.submit(game.run))
        
        # Wait for all games to complete
        for future in futures:
            future.result()
        
        # Stop stadium operations
        for thread in stadium_threads:
            thread.stop_event.set()
            thread.join(timeout=1.0)  # Join with timeout to avoid hanging

def simulate_conferences(east_schedule, west_schedule):
    """Simulate eastern and western conference games using multiprocessing"""
    with ProcessPoolExecutor(max_workers=2) as executor:
        # Submit each conference's games to separate processes
        east_future = executor.submit(simulate_parallel_games, east_schedule)
        west_future = executor.submit(simulate_parallel_games, west_schedule)
        
        # Wait for both conferences to complete their games
        east_future.result()
        west_future.result()

def generate_stats_report():
    """Generate a report of game stats from the database"""
    conn = sqlite3.connect('nba_simulation.db')
    cursor = conn.cursor()
    
    # Get top scoring teams
    cursor.execute('''
    SELECT team1, SUM(score1) as points
    FROM games
    GROUP BY team1
    ORDER BY points DESC
    LIMIT 5
    ''')
    top_teams = cursor.fetchall()
    
    # Get top scoring players
    cursor.execute('''
    SELECT player_name, SUM(points) as total_points
    FROM player_stats
    GROUP BY player_name
    ORDER BY total_points DESC
    LIMIT 10
    ''')
    top_players = cursor.fetchall()
    
    # Get stadium operation stats
    cursor.execute('''
    SELECT operation_type, AVG(processed_count) as avg_count
    FROM stadium_ops
    GROUP BY operation_type
    ''')
    ops_stats = cursor.fetchall()
    
    conn.close()
    
    # Log the report
    logging.info("\n===== NBA SIMULATION STATS REPORT =====")
    
    logging.info("\nTOP SCORING TEAMS:")
    for i, (team, points) in enumerate(top_teams, 1):
        logging.info(f"{i}. {team}: {points} points")
    
    logging.info("\nTOP SCORING PLAYERS:")
    for i, (player, points) in enumerate(top_players, 1):
        logging.info(f"{i}. {player}: {points} points")
    
    logging.info("\nSTADIUM OPERATIONS AVERAGES:")
    for op_type, avg in ops_stats:
        logging.info(f"{op_type.capitalize()}: {avg:.1f} average processed")
    
    logging.info("\n======================================")

# Main entry point
if __name__ == "__main__":
    # Initialize database
    init_database()
    
    # Fetch real NBA games using the API
    nba_games = fetch_nba_games(num_games=6)
    
    # Split games into conferences (simplified for demo)
    mid_point = len(nba_games) // 2
    eastern_games = nba_games[:mid_point]
    western_games = nba_games[mid_point:]
    
    # Log the start of simulation
    logging.info("Starting NBA simulation")
    logging.info(f"Eastern Conference Games: {len(eastern_games)}")
    logging.info(f"Western Conference Games: {len(western_games)}")
    
    # Option 1: Simulate conferences in parallel using multiprocessing
    simulate_conferences(eastern_games, western_games)
    
    # Option 2: Simulate all games using a thread pool
    # all_games = eastern_games + western_games
    # simulate_parallel_games(all_games)
    
    # Generate a stats report
    generate_stats_report()
    
    # Print results
    logging.info("Simulation completed!")
    logging.info("Game Results:")
    for game_id, result in game_results.items():
        logging.info(f"Game {game_id}: {result['team1']} {result['score1']} - {result['team2']} {result['score2']}")
        logging.info(f"Winner: {result['winner']}")