import threading
import random
import time
import queue
import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from datetime import datetime, timedelta
import json
import uuid

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

    # games table
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
    
    # player stats table
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
    
    # stadium operations table
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
    """Get player roster for a team"""
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

def generate_nba_schedule(season_start_date=datetime(2023, 10, 24), num_games=82):
    """Generates a simplified, but more realistic NBA schedule with game IDs."""

    teams = list(NBA_TEAMS.values())
    schedule = []
    game_date = season_start_date
    days_between_games = 2

    for _ in range(num_games * len(teams) // 2):
        home_team = random.choice(teams)
        away_team = random.choice(teams)

        if home_team != away_team:
            game_id = str(uuid.uuid4()) #Generate a unique ID
            schedule.append({
                "game_id": game_id, #add game id
                "home": home_team["name"],
                "away": away_team["name"],
                "arena": home_team["arena"],
                "date": game_date.strftime("%Y-%m-%d")
            })
            game_date += timedelta(days=days_between_games)

    return schedule

def simulate_parallel_games(game_schedule):
    """Simulate multiple NBA games in parallel using thread pool"""
    with ThreadPoolExecutor(max_workers=len(game_schedule)) as executor:
        futures = []
        stadium_threads = []

        for game in game_schedule: #loop through schedule dictionaries.
            game_id = game["game_id"] #access game_id from dictionary.
            team1 = game["home"]
            team2 = game["away"]
            arena = game["arena"]
            game_date = game["date"]
            team1_id = None #or get them from the dictionary if you added them
            team2_id = None

            # Start stadium operations first
            security = StadiumOperation(game_id, arena, "security")
            concessions = StadiumOperation(game_id, arena, "concessions")
            merchandise = StadiumOperation(game_id, arena, "merchandise")

            security.start()
            concessions.start()
            merchandise.start()

            stadium_threads.extend([security, concessions, merchandise])

            # Create and start the game
            game_instance = NBA_Game(team1, team2, game_id, arena, game_date, team1_id, team2_id)
            futures.append(executor.submit(game_instance.run))

        # Wait for all games to complete
        for future in futures:
            future.result()

        # Stop stadium operations
        for thread in stadium_threads:
            thread.stop_event.set()
            thread.join(timeout=1.0)

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
    try:
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

    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

def determine_top_conference_teams(teams, NBA_TEAMS):
    """Determine the top 8 teams from each conference."""

    east_teams = []
    west_teams = []

    for team in teams:
        team_name = team['name']
        if team_name in NBA_TEAMS:
            conference = NBA_TEAMS[team_name]['conference']
            if conference == 'East':
                east_teams.append(team)
            elif conference == 'West':
                west_teams.append(team)
        else:
            print(f"team name {team_name} not found in NBA_TEAMS dictionary")

    def get_win_percentage(team):
        return team['wins'] / (team['wins'] + team['losses'])

    top_east = sorted(east_teams, key=get_win_percentage, reverse=True)[:8]
    top_west = sorted(west_teams, key=get_win_percentage, reverse=True)[:8]

    return top_east, top_west

def create_playoff_bracket(east_teams, west_teams):
    """Create a playoff bracket from top teams in each conference"""
    playoff_bracket = {}

    playoff_bracket['Eastern Conference'] = {
        'First Round': [
            (east_teams[0], east_teams[7]),
            (east_teams[1], east_teams[6]),
            (east_teams[2], east_teams[5]),
            (east_teams[3], east_teams[4])
        ]
    }

    playoff_bracket['Western Conference'] = {
        'First Round': [
            (west_teams[0], west_teams[7]),
            (west_teams[1], west_teams[6]),
            (west_teams[2], west_teams[5]),
            (west_teams[3], west_teams[4])
        ]
    }

    return playoff_bracket

def simulate_playoff_series(team1, team2, game_results, series_length=7):
    """Simulate a best-of-7 playoff series"""
    series_winner = None
    series_score = {team1: 0, team2: 0}
    series_games = []

    home_team = random.choice([team1, team2])
    away_team = team2 if home_team == team1 else team1

    while max(series_score.values()) < 4 and sum(series_score.values()) < 7:
        current_home = home_team if len(series_games) % 2 == 0 or len(series_games) == 4 or len(series_games) == 6 else away_team
        current_away = away_team if current_home == home_team else home_team

        game_id = f"{team1}-{team2}-{len(series_games)}"
        game = NBA_Game(current_home, current_away, game_id,
                        arena=f"{current_home} Arena",
                        team1_id=NBA_TEAMS[current_home]["id"] if current_home in NBA_TEAMS else None,
                        team2_id=NBA_TEAMS[current_away]["id"] if current_away in NBA_TEAMS else None)
        game.run()

        result = game_results.get(game_id, {})
        winner = result.get('winner')

        series_score[winner] += 1
        series_games.append(result)

        if max(series_score.values()) == 4:
            series_winner = max(series_score, key=series_score.get)

    return {
        'winner': series_winner,
        'series_score': series_score,
        'games': series_games
    }

def simulate_full_playoffs(east_teams, west_teams, game_results):
    """Simulate the entire NBA playoffs"""
    playoff_bracket = create_playoff_bracket(east_teams, west_teams)
    playoff_results = {}

    for conference in ['Eastern Conference', 'Western Conference']:
        semifinal_winners = []

        for series in playoff_bracket[conference]['First Round']:
            series_result = simulate_playoff_series(series[0], series[1], game_results)
            playoff_results[f"{conference} First Round: {series[0]} vs {series[1]}"] = series_result
            semifinal_winners.append(series_result['winner'])

        playoff_bracket[conference]['Semifinals'] = [
            (semifinal_winners[0], semifinal_winners[1]),
            (semifinal_winners[2], semifinal_winners[3])
        ]

        conference_finalists = []
        for series in playoff_bracket[conference]['Semifinals']:
            series_result = simulate_playoff_series(series[0], series[1], game_results)
            playoff_results[f"{conference} Semifinals: {series[0]} vs {series[1]}"] = series_result
            conference_finalists.append(series_result['winner'])

        playoff_bracket[conference]['Conference Finals'] = (conference_finalists[0], conference_finalists[1])

        conference_final_result = simulate_playoff_series(conference_finalists[0], conference_finalists[1], game_results)
        playoff_results[f"{conference} Conference Finals"] = conference_final_result
        playoff_bracket[conference]['Conference Champion'] = conference_final_result['winner']

    finals_teams = [
        playoff_bracket['Eastern Conference']['Conference Champion'],
        playoff_bracket['Western Conference']['Conference Champion']
    ]
    nba_finals_result = simulate_playoff_series(finals_teams[0], finals_teams[1], game_results)
    playoff_results['NBA Finals'] = nba_finals_result

    return playoff_results

def generate_playoff_summary(playoff_results, game_results):
    """Generate a comprehensive playoff summary"""
    logging.info("\n===== 🏆 NBA PLAYOFFS SUMMARY 🏆 =====")

    finals = playoff_results.get('NBA Finals', {})
    champion = finals.get('winner')
    logging.info(f"\nNBA CHAMPION: {champion}")

    for round_name, result in playoff_results.items():
        logging.info(f"\n{round_name}:")
        logging.info(f"{result['series_score'][result['winner']]} - {result['series_score'][result['winner'] == result['games'][0]['team1'] and result['games'][0]['team2'] or result['games'][0]['team1']]} Series Win")
        logging.info("Series Detailed Results:")
        for i, game in enumerate(result['games'], 1):
            game_id = game['game_id']
            logging.info(f"  Game {i}: {game['team1']} {game_results[game_id]['score1']} - {game['team2']} {game_results[game_id]['score2']} (Winner: {game['winner']})")

    logging.info("\n===== END OF PLAYOFFS SUMMARY =====")

def create_realistic_playoff_schedule(playoff_bracket, start_date=datetime(2024, 4, 20)):
    """Create a more realistic game schedule for the playoffs."""

    schedule = {}
    current_date = start_date
    rest_days = 2  # Default rest days between games

    def schedule_series(series_pair, round_name, conference):
        nonlocal current_date
        team1, team2 = series_pair
        series_id = f"{conference} {round_name}: {team1} vs {team2}"
        schedule[series_id] = []
        home_team = random.choice([team1, team2])
        away_team = team2 if home_team == team1 else team1

        for game_num in range(1, 8):  # Max 7 games
            if game_num == 1 or game_num == 2 or game_num == 5 or game_num == 7:
              game_home = home_team
              game_away = away_team
            else:
              game_home = away_team
              game_away = home_team

            schedule[series_id].append({
                "game_num": game_num,
                "home": game_home,
                "away": game_away,
                "date": current_date.strftime("%Y-%m-%d")
            })
            current_date += timedelta(days=1)  # Games on consecutive days

            if game_num in [2, 4, 6]: #add rest days after games 2, 4, 6
              current_date += timedelta(days=rest_days)

            # Check for series completion
            if game_num == 4:
                # If a team has won 4 games, the series is over
                series_result = simulate_playoff_series(team1, team2) #simulate series to get winner.
                if series_result['series_score'][series_result['winner']] == 4:
                  break
            if game_num == 5:
                series_result = simulate_playoff_series(team1, team2)
                if series_result['series_score'][series_result['winner']] == 4:
                  break
            if game_num == 6:
                series_result = simulate_playoff_series(team1, team2)
                if series_result['series_score'][series_result['winner']] == 4:
                  break

        current_date += timedelta(days=rest_days)  # Rest after series

    # Schedule each round
    for conference, rounds in playoff_bracket.items():
        for round_name, series_pairs in rounds.items():
            if isinstance(series_pairs, tuple): #Handles Conference Finals.
              schedule_series(series_pairs, round_name, conference)
            else:
              for series_pair in series_pairs:
                  schedule_series(series_pair, round_name, conference)

    # Schedule NBA Finals
    finals_teams = [
        playoff_bracket['Eastern Conference']['Conference Champion'],
        playoff_bracket['Western Conference']['Conference Champion']
    ]
    schedule_series(finals_teams, "NBA Finals", "NBA")

    return schedule

if __name__ == "__main__":
    # Initialize database
    init_database()
    
    # Simulate regular season
    logging.info("Starting NBA Regular Season Simulation")
    nba_games = generate_nba_schedule(num_games=82)  # Full 82-game season
    
    # Split games into conferences (simplified)
    mid_point = len(nba_games) // 2
    eastern_games = nba_games[:mid_point]
    western_games = nba_games[mid_point:]
    
    # Simulate regular season
    simulate_conferences(eastern_games, western_games)
    
    # Generate stats report for regular season
    generate_stats_report()
    
    # Determine top teams for playoffs (top 8 from each conference)
    teams_with_records = []
    for game in game_results.values():
        if game.get('team1') and game.get('winner'):
            winner = game.get('winner')
            loser = game.get('team2') if winner == game.get('team1') else game.get('team1')

            team1_exists = False
            team2_exists = False

            for team_record in teams_with_records:
                if team_record['name'] == game.get('team1'):
                    team1_exists = True
                    if winner == game.get('team1'):
                        team_record['wins'] += 1
                    else:
                        team_record['losses'] += 1
                if team_record['name'] == game.get('team2'):
                    team2_exists = True
                    if winner == game.get('team2'):
                        team_record['wins'] += 1
                    else:
                        team_record['losses'] += 1

            if not team1_exists:
                if winner == game.get('team1'):
                    teams_with_records.append({'name': game.get('team1'), 'wins': 1, 'losses': 0})
                else:
                    teams_with_records.append({'name': game.get('team1'), 'wins': 0, 'losses': 1})

            if not team2_exists:
                if winner == game.get('team2'):
                    teams_with_records.append({'name': game.get('team2'), 'wins': 1, 'losses': 0})
                else:
                    teams_with_records.append({'name': game.get('team2'), 'wins': 0, 'losses': 1})

    east_teams, west_teams = determine_top_conference_teams(teams_with_records, NBA_TEAMS)

    # Simulate Playoffs
    logging.info("\nStarting NBA Playoffs Simulation")
    playoff_results = simulate_full_playoffs(east_teams, west_teams, game_results)

    # Generate playoff summary
    generate_playoff_summary(playoff_results, game_results)

    # Create Playoff Schedule
    playoff_bracket = create_playoff_bracket(east_teams, west_teams)
    playoff_schedule = create_realistic_playoff_schedule(playoff_bracket)

    # print playoff schedule.
    for series, games in playoff_schedule.items():
        print(f"\n{series}:")
        for game in games:
            print(f"  Game {game['game_num']}: {game['away']} @ {game['home']} on {game['date']}")

    logging.info("Complete NBA Season Simulation Completed!")