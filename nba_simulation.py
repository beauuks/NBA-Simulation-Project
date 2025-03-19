import threading
import random
import time
import queue
import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from datetime import datetime
import json

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

# playoffs 
playoff_results = {}
playoff_bracket = {}

# Load the JSON file
with open('nba_data.json', 'r') as f:
    nba_data = json.load(f)

# Access the data
NBA_TEAMS = nba_data['NBA_TEAMS']
NBA_PLAYERS = nba_data['NBA_PLAYERS']

# set up a database for games, players, and stadium operations
def init_database():
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
    """Get player roster for a team from our defined dictionaries"""
    if team_id and team_id in NBA_PLAYERS:
        return NBA_PLAYERS[team_id]
    
    # Return a default roster if team not found
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

def fetch_nba_games(season='2023-24', num_games=82):
    """Create predefined NBA games for a full regular season"""
    all_teams = list(NBA_TEAMS.values())
    
    # Create a comprehensive schedule
    games = []
    used_games = set()
    
    while len(games) < num_games:
        # Ensure no team plays more than 82 games
        for home_team in all_teams:
            for away_team in all_teams:
                if home_team != away_team:
                    # Create a unique game combination
                    game_key = (home_team['name'], away_team['name'])
                    reverse_game_key = (away_team['name'], home_team['name'])
                    
                    if game_key not in used_games and reverse_game_key not in used_games:
                        games.append((
                            home_team['name'], 
                            away_team['name'], 
                            home_team['arena'], 
                            datetime.now().strftime('%Y-%m-%d')
                        ))
                        used_games.add(game_key)
                        
                        if len(games) >= num_games:
                            break
            
            if len(games) >= num_games:
                break
    
    return games[:num_games]

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
        logging.info("Submitting Eastern Conference games.")
        east_future = executor.submit(simulate_parallel_games, east_schedule)
        logging.info("Submitting Western Conference games.")
        west_future = executor.submit(simulate_parallel_games, west_schedule)
        
        # Wait for both conferences to complete their games
        logging.info("Waiting for Eastern Conference to complete.")
        east_future.result()
        logging.info("Waiting for Western Conference to complete.")
        west_future.result()
    logging.info("Conference simulations completed.")

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

def create_playoff_bracket(top_teams):
    """Create a playoff bracket from top teams in each conference"""
    playoff_bracket.clear()

    # Eastern Conference Playoff Bracket
    east_bracket = top_teams[:8]
    playoff_bracket['Eastern Conference'] = {
        'First Round': [
            (east_bracket[0], east_bracket[7]),
            (east_bracket[1], east_bracket[6]),
            (east_bracket[2], east_bracket[5]),
            (east_bracket[3], east_bracket[4])
        ]
    }

    # Western Conference Playoff Bracket
    west_bracket = top_teams[8:]
    playoff_bracket['Western Conference'] = {
        'First Round': [
            (west_bracket[0], west_bracket[7]),
            (west_bracket[1], west_bracket[6]),
            (west_bracket[2], west_bracket[5]),
            (west_bracket[3], west_bracket[4])
        ]
    }

    return playoff_bracket

def simulate_playoff_series(team1, team2, series_length=7):
    """Simulate a best-of-7 playoff series"""
    series_winner = None
    series_score = {team1: 0, team2: 0}
    series_games = []
    
    # Determine home team advantage
    home_team = random.choice([team1, team2])
    away_team = team2 if home_team == team1 else team1
    
    while max(series_score.values()) < 4 and sum(series_score.values()) < 7:
        # Swap home/away for each game
        current_home = home_team if len(series_games) % 2 == 0 else away_team
        current_away = away_team if current_home == home_team else home_team
        
        # Simulate game with home team advantage
        game_id = len(series_games)
        game = NBA_Game(current_home, current_away, game_id, 
                        arena=f"{current_home} Arena", 
                        team1_id=NBA_TEAMS[current_home]["id"] if current_home in NBA_TEAMS else None, 
                        team2_id=NBA_TEAMS[current_away]["id"] if current_away in NBA_TEAMS else None)
        game.run()
        
        # Get the game result
        result = game_results.get(game_id, {})
        winner = result.get('winner')
        
        # Update series score
        series_score[winner] += 1
        series_games.append(result)
        
        # Check if series is over
        if max(series_score.values()) == 4:
            series_winner = max(series_score, key=series_score.get)
    
    return {
        'winner': series_winner,
        'series_score': series_score,
        'games': series_games
    }

def simulate_full_playoffs(top_teams):
    """Simulate the entire NBA playoffs"""
    playoff_bracket = create_playoff_bracket(top_teams)
    playoff_results.clear()
    
    # Simulate Conference Semifinals
    for conference in ['Eastern Conference', 'Western Conference']:
        semifinal_winners = []
        
        for series in playoff_bracket[conference]['First Round']:
            # Simulate first-round series
            series_result = simulate_playoff_series(series[0], series[1])
            playoff_results[f"{conference} First Round: {series[0]} vs {series[1]}"] = series_result
            semifinal_winners.append(series_result['winner'])
        
        # Update playoff bracket with Semifinals
        playoff_bracket[conference]['Semifinals'] = [
            (semifinal_winners[0], semifinal_winners[1]),
            (semifinal_winners[2], semifinal_winners[3])
        ]
        
        # Simulate Conference Semifinals
        conference_finalists = []
        for series in playoff_bracket[conference]['Semifinals']:
            series_result = simulate_playoff_series(series[0], series[1])
            playoff_results[f"{conference} Semifinals: {series[0]} vs {series[1]}"] = series_result
            conference_finalists.append(series_result['winner'])
        
        # Update playoff bracket with Conference Finals
        playoff_bracket[conference]['Conference Finals'] = (conference_finalists[0], conference_finalists[1])
        
        # Simulate Conference Finals
        conference_final_result = simulate_playoff_series(conference_finalists[0], conference_finalists[1])
        playoff_results[f"{conference} Conference Finals"] = conference_final_result
        playoff_bracket[conference]['Conference Champion'] = conference_final_result['winner']
    
    # NBA Finals
    finals_teams = [
        playoff_bracket['Eastern Conference']['Conference Champion'],
        playoff_bracket['Western Conference']['Conference Champion']
    ]
    nba_finals_result = simulate_playoff_series(finals_teams[0], finals_teams[1])
    playoff_results['NBA Finals'] = nba_finals_result
    
    return nba_finals_result

def generate_playoff_summary():
    """Generate a comprehensive playoff summary"""
    logging.info("\n===== 🏆 NBA PLAYOFFS SUMMARY 🏆 =====")
    
    # NBA Finals
    finals = playoff_results.get('NBA Finals', {})
    champion = finals.get('winner')
    logging.info(f"\nNBA CHAMPION: {champion}")
    
    # Detailed playoff progression
    for round_name, result in playoff_results.items():
        if round_name == 'NBA Finals':
            logging.info(f"\n{round_name}:")
            logging.info(f"{result['series_score'][result['winner']]} - {result['series_score'][result['winner'] == result['games'][0]['team1'] and result['games'][0]['team2'] or result['games'][0]['team1']]} Series Win")
            logging.info("Championship Series Detailed Results:")
            for i, game in enumerate(result['games'], 1):
                logging.info(f"Game {i}: {game['team1']} {game['score1']} - {game['team2']} {game['score2']} (Winner: {game['winner']})")
    
    logging.info("\n===== END OF PLAYOFFS SUMMARY =====")

# Main entry point
if __name__ == "__main__":
    # Initialize database
    init_database()
    
    # Simulate regular season
    logging.info("Starting NBA Regular Season Simulation")
    nba_games = fetch_nba_games(num_games=82)  # Full 82-game season
    
    # Split games into conferences (simplified)
    mid_point = len(nba_games) // 2
    eastern_games = nba_games[:mid_point]
    western_games = nba_games[mid_point:]
    
    # Simulate regular season
    simulate_conferences(eastern_games, western_games)
    
    # Generate stats report for regular season
    generate_stats_report()
    
    # Determine top teams for playoffs (top 8 from each conference)
    # For simplicity, we'll use the first 16 teams from game_results
    top_teams = list(set([
        result['team1'] for result in list(game_results.values())[:16]
    ]))
    
    # Simulate Playoffs
    logging.info("\nStarting NBA Playoffs Simulation")
    champion = simulate_full_playoffs(top_teams)
    
    # Generate playoff summary
    generate_playoff_summary()
    
    logging.info("Complete NBA Season Simulation Completed!")