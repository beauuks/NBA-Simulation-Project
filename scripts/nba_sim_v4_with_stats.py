import threading
import random
import time
import queue
import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import pandas as pd
from datetime import datetime
import json
import csv
import os
import numpy as np

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

# Load player stats from CSV
def load_player_stats(csv_file_path='player_stats.csv'):
    """Load player stats from a CSV file"""
    try:
        # Check if file exists
        if not os.path.exists(csv_file_path):
            # Create sample file with the provided data if it doesn't exist
            sample_data = """Player,Tm,Opp,Res,MP,FG,FGA,FG%,3P,3PA,3P%,FT,FTA,FT%,ORB,DRB,TRB,AST,STL,BLK,TOV,PF,PTS,GmSc,Data
Jayson Tatum,BOS,NYK,W,30.3,14,18,0.778,8,11,0.727,1,2,0.5,0,4,4,10,1,1,1,1,37,38.1,2024-10-22
Anthony Davis,LAL,MIN,W,37.58,11,23,0.478,1,3,0.333,13,15,0.867,3,13,16,4,1,3,1,1,36,34.0,2024-10-22
Derrick White,BOS,NYK,W,26.63,8,13,0.615,6,10,0.6,2,2,1.0,0,3,3,4,1,0,0,1,24,22.4,2024-10-22"""
            
            with open(csv_file_path, 'w') as f:
                f.write(sample_data)
            
            logging.info(f"Created sample player stats file at {csv_file_path}")
        
        # Load the CSV data
        df = pd.read_csv(csv_file_path)
        
        # Group by player and team to get player averages
        player_stats = {}
        
        for _, row in df.iterrows():
            player_name = row['Player']
            team = row['Tm']
            
            # Create a unique key that combines player name and team
            player_key = f"{player_name}_{team}"
            
            # Extract relevant stats
            stats = {
                'name': player_name,
                'team': team,
                'mp': float(row['MP']),
                'fg_pct': float(row['FG%']),
                '3p_pct': float(row['3P%']),
                'ft_pct': float(row['FT%']),
                'ast_per_min': float(row['AST']) / float(row['MP']),
                'stl_per_min': float(row['STL']) / float(row['MP']),
                'blk_per_min': float(row['BLK']) / float(row['MP']),
                'reb_per_min': float(row['TRB']) / float(row['MP']),
                'pts_per_min': float(row['PTS']) / float(row['MP']),
                'tov_per_min': float(row['TOV']) / float(row['MP']),
                'shot_dist': {  # Shot distribution (2PT vs 3PT)
                    '2pt': (float(row['FGA']) - float(row['3PA'])) / float(row['FGA']) if float(row['FGA']) > 0 else 0.6,
                    '3pt': float(row['3PA']) / float(row['FGA']) if float(row['FGA']) > 0 else 0.4
                }
            }
            
            player_stats[player_key] = stats
        
        logging.info(f"Loaded stats for {len(player_stats)} players")
        return player_stats
    
    except Exception as e:
        logging.error(f"Error loading player stats: {e}")
        # Return empty dict if there's an error
        return {}

# Global player stats
PLAYER_STATS = load_player_stats()

# Load the JSON file
try:
    with open('nba_data.json', 'r') as f:
        nba_data = json.load(f)
        
    # Access the data
    NBA_TEAMS = nba_data['NBA_TEAMS']
    NBA_PLAYERS = nba_data['NBA_PLAYERS']
except FileNotFoundError:
    # Create basic data if file doesn't exist
    logging.warning("nba_data.json not found. Creating basic team and player data.")
    
    NBA_TEAMS = {
        "BOS": {"name": "Boston Celtics", "arena": "TD Garden"},
        "LAL": {"name": "Los Angeles Lakers", "arena": "Crypto.com Arena"},
        "MIA": {"name": "Miami Heat", "arena": "Kaseya Center"},
        "MIL": {"name": "Milwaukee Bucks", "arena": "Fiserv Forum"},
        "PHI": {"name": "Philadelphia 76ers", "arena": "Wells Fargo Center"},
        "GSW": {"name": "Golden State Warriors", "arena": "Chase Center"},
        "DEN": {"name": "Denver Nuggets", "arena": "Ball Arena"},
        "PHX": {"name": "Phoenix Suns", "arena": "Footprint Center"},
        "NYK": {"name": "New York Knicks", "arena": "Madison Square Garden"},
        "BKN": {"name": "Brooklyn Nets", "arena": "Barclays Center"},
        "DAL": {"name": "Dallas Mavericks", "arena": "American Airlines Center"},
        "OKC": {"name": "Oklahoma City Thunder", "arena": "Paycom Center"},
        "CLE": {"name": "Cleveland Cavaliers", "arena": "Rocket Mortgage FieldHouse"},
        "CHI": {"name": "Chicago Bulls", "arena": "United Center"},
        "MEM": {"name": "Memphis Grizzlies", "arena": "FedExForum"},
        "NOP": {"name": "New Orleans Pelicans", "arena": "Smoothie King Center"},
        "MIN": {"name": "Minnesota Timberwolves", "arena": "Target Center"},
    }
    
    # Create basic player data using the stats we have
    NBA_PLAYERS = {}
    for player_key, stats in PLAYER_STATS.items():
        team_code = stats['team']
        if team_code not in NBA_PLAYERS:
            NBA_PLAYERS[team_code] = []
        
        NBA_PLAYERS[team_code].append(stats['name'])
    
    # Add some default players for teams with no players in our stats
    for team_code in NBA_TEAMS:
        if team_code not in NBA_PLAYERS or not NBA_PLAYERS[team_code]:
            NBA_PLAYERS[team_code] = [f"{NBA_TEAMS[team_code]['name']}Player{i}" for i in range(1, 6)]

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
        minutes REAL,
        points INTEGER,
        rebounds INTEGER,
        assists INTEGER,
        steals INTEGER,
        blocks INTEGER,
        turnovers INTEGER,
        fg_made INTEGER,
        fg_attempts INTEGER,
        fg_pct REAL,
        three_pt_made INTEGER,
        three_pt_attempts INTEGER,
        three_pt_pct REAL,
        ft_made INTEGER,
        ft_attempts INTEGER,
        ft_pct REAL,
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
            # Convert team abbreviation to full name if needed
            team_abbr = stats.get('team_abbr', '')
            team_name = stats.get('team', '')
            
            # If we have both full name and abbreviation, insert as is
            # Otherwise, try to convert abbreviation to full name
            if team_name and team_abbr:
                team = team_name
            elif team_abbr and team_abbr in NBA_TEAMS:
                team = NBA_TEAMS[team_abbr]['name']
            else:
                team = team_name or "Unknown Team"
            
            cursor.execute(
                """
                INSERT INTO player_stats 
                (game_id, player_name, team, minutes, points, rebounds, assists, 
                steals, blocks, turnovers, fg_made, fg_attempts, fg_pct,
                three_pt_made, three_pt_attempts, three_pt_pct,
                ft_made, ft_attempts, ft_pct) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    game_id, player, team, 
                    stats.get('minutes', 0),
                    stats.get('points', 0), 
                    stats.get('rebounds', 0), 
                    stats.get('assists', 0),
                    stats.get('steals', 0), 
                    stats.get('blocks', 0), 
                    stats.get('turnovers', 0),
                    stats.get('fg_made', 0),
                    stats.get('fg_attempts', 0),
                    stats.get('fg_pct', 0.0),
                    stats.get('three_pt_made', 0),
                    stats.get('three_pt_attempts', 0),
                    stats.get('three_pt_pct', 0.0),
                    stats.get('ft_made', 0),
                    stats.get('ft_attempts', 0),
                    stats.get('ft_pct', 0.0)
                )
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

def get_team_code(team_name):
    """Find team code from team name"""
    for code, team_info in NBA_TEAMS.items():
        if team_info['name'] == team_name:
            return code
    return None

def get_team_roster(team_id):
    """Get player roster for a team from our defined dictionaries"""
    if team_id and team_id in NBA_PLAYERS:
        return NBA_PLAYERS[team_id]
    
    # Return a default roster if team not found
    return [f"Player{i}" for i in range(1, 6)]

def get_player_stats_key(player_name, team_id):
    """Get the key for player stats dictionary"""
    return f"{player_name}_{team_id}"

class Player:
    def __init__(self, name, team, team_abbr=None):
        self.name = name
        self.team = team
        self.team_abbr = team_abbr
        self.minutes_played = 0
        
        # Initialize stats from real data if available
        self.real_stats = None
        if team_abbr:
            stats_key = get_player_stats_key(name, team_abbr)
            if stats_key in PLAYER_STATS:
                self.real_stats = PLAYER_STATS[stats_key]
        
        # Initialize game stats
        self.stats = {
            'minutes': 0,
            'points': 0,
            'rebounds': 0,
            'assists': 0,
            'steals': 0,
            'blocks': 0,
            'turnovers': 0,
            'fg_made': 0,
            'fg_attempts': 0,
            'fg_pct': 0.0,
            'three_pt_made': 0,
            'three_pt_attempts': 0,
            'three_pt_pct': 0.0,
            'ft_made': 0,
            'ft_attempts': 0,
            'ft_pct': 0.0
        }
    
    def update_minutes(self, minutes):
        """Update minutes played"""
        self.minutes_played += minutes
        self.stats['minutes'] = self.minutes_played
    
    def update_stat(self, stat_name, value=1):
        """Update a player statistic"""
        if stat_name in self.stats:
            self.stats[stat_name] += value
    
    def simulate_shot(self, shot_type):
        """Simulate a shot attempt based on real stats or reasonable defaults"""
        # Default shooting percentages if real stats not available
        default_percentages = {
            '2PT': 0.45,  # 45% success for 2-pointers
            '3PT': 0.35,  # 35% success for 3-pointers
            'FT': 0.75    # 75% success for free throws
        }
        
        if self.real_stats:
            # Use real player stats for shot percentage
            if shot_type == '2PT':
                success_rate = self.real_stats['fg_pct']
                self.stats['fg_attempts'] += 1
                
                if random.random() < success_rate:
                    self.stats['fg_made'] += 1
                    self.stats['points'] += 2
                    return True
                return False
                
            elif shot_type == '3PT':
                success_rate = self.real_stats['3p_pct']
                self.stats['three_pt_attempts'] += 1
                self.stats['fg_attempts'] += 1
                
                if random.random() < success_rate:
                    self.stats['three_pt_made'] += 1
                    self.stats['fg_made'] += 1
                    self.stats['points'] += 3
                    return True
                return False
                
            elif shot_type == 'FT':
                success_rate = self.real_stats['ft_pct']
                self.stats['ft_attempts'] += 1
                
                if random.random() < success_rate:
                    self.stats['ft_made'] += 1
                    self.stats['points'] += 1
                    return True
                return False
        else:
            # Use default percentages
            success_rate = default_percentages.get(shot_type, 0.5)
            
            if shot_type == '2PT':
                self.stats['fg_attempts'] += 1
                if random.random() < success_rate:
                    self.stats['fg_made'] += 1
                    self.stats['points'] += 2
                    return True
                
            elif shot_type == '3PT':
                self.stats['three_pt_attempts'] += 1
                self.stats['fg_attempts'] += 1
                if random.random() < success_rate:
                    self.stats['three_pt_made'] += 1
                    self.stats['fg_made'] += 1
                    self.stats['points'] += 3
                    return True
                
            elif shot_type == 'FT':
                self.stats['ft_attempts'] += 1
                if random.random() < success_rate:
                    self.stats['ft_made'] += 1
                    self.stats['points'] += 1
                    return True
        
        return False
    
    def choose_shot_type(self):
        """Choose shot type based on player's real shot distribution"""
        if self.real_stats and 'shot_dist' in self.real_stats:
            # Use player's real shot distribution
            weights = [
                self.real_stats['shot_dist'].get('2pt', 0.6),
                self.real_stats['shot_dist'].get('3pt', 0.4)
            ]
            shot_type = random.choices(['2PT', '3PT'], weights=weights)[0]
        else:
            # Default distribution
            shot_type = random.choices(['2PT', '3PT'], weights=[0.6, 0.4])[0]
        
        return shot_type
    
    def simulate_player_actions(self, minutes):
        """Simulate player actions for a given number of minutes"""
        self.update_minutes(minutes)
        
        # Calculate expected stats based on per-minute rates or defaults
        if self.real_stats:
            expected_pts = self.real_stats['pts_per_min'] * minutes
            expected_ast = self.real_stats['ast_per_min'] * minutes
            expected_reb = self.real_stats['reb_per_min'] * minutes
            expected_stl = self.real_stats['stl_per_min'] * minutes
            expected_blk = self.real_stats['blk_per_min'] * minutes
            expected_tov = self.real_stats['tov_per_min'] * minutes
        else:
            # Default per-minute rates
            expected_pts = 0.5 * minutes  # 0.5 points per minute
            expected_ast = 0.1 * minutes  # 0.1 assists per minute
            expected_reb = 0.2 * minutes  # 0.2 rebounds per minute
            expected_stl = 0.05 * minutes  # 0.05 steals per minute
            expected_blk = 0.05 * minutes  # 0.05 blocks per minute
            expected_tov = 0.07 * minutes  # 0.07 turnovers per minute
        
        # Add random variation (±20%)
        variation = lambda x: max(0, x * random.uniform(0.8, 1.2))
        
        # Update non-scoring stats with variation
        assists = int(variation(expected_ast))
        rebounds = int(variation(expected_reb))
        steals = int(variation(expected_stl))
        blocks = int(variation(expected_blk))
        turnovers = int(variation(expected_tov))
        
        self.stats['assists'] += assists
        self.stats['rebounds'] += rebounds
        self.stats['steals'] += steals
        self.stats['blocks'] += blocks
        self.stats['turnovers'] += turnovers
        
        # Calculate ratios only if values are positive
        if self.stats['fg_attempts'] > 0:
            self.stats['fg_pct'] = self.stats['fg_made'] / self.stats['fg_attempts']
        
        if self.stats['three_pt_attempts'] > 0:
            self.stats['three_pt_pct'] = self.stats['three_pt_made'] / self.stats['three_pt_attempts']
        
        if self.stats['ft_attempts'] > 0:
            self.stats['ft_pct'] = self.stats['ft_made'] / self.stats['ft_attempts']
        
        return {
            'assists': assists,
            'rebounds': rebounds,
            'steals': steals,
            'blocks': blocks,
            'turnovers': turnovers
        }
    
    def get_stats_dict(self):
        """Return stats as a dictionary with team included"""
        result = self.stats.copy()
        result['team'] = self.team
        result['team_abbr'] = self.team_abbr
        return result

class NBA_Game(threading.Thread):
    def __init__(self, team1, team2, game_id, arena=None, date=None, team1_id=None, team2_id=None):
        super().__init__(name=f"Game-{team1}-vs-{team2}")
        self.team1 = team1
        self.team2 = team2
        self.game_id = game_id
        self.arena = arena or f"{team1} Arena"
        self.date = date or datetime.now().strftime('%Y-%m-%d')
        self.team1_id = team1_id or get_team_code(team1)
        self.team2_id = team2_id or get_team_code(team2)
        
        self.score = {team1: 0, team2: 0}
        self.quarters_completed = 0
        self.events = []
        self.event_lock = threading.Lock()
        self.game_ended = threading.Event()
        
        # Game state
        self.current_quarter_time = 0
        self.quarter_length = 12 * 60  # 12 minutes in seconds
        self.possession_team = None
        self.player_minutes = {}
        
        # Initialize players
        self.players = {}
        self.active_players = {self.team1: [], self.team2: []}
        self.initialize_players()
    
    def initialize_players(self):
        """Initialize player rosters for both teams"""
        # Team 1 players
        roster1 = get_team_roster(self.team1_id) if self.team1_id else [f"{self.team1}Player{i}" for i in range(1, 6)]
        for player_name in roster1:
            self.players[player_name] = Player(player_name, self.team1, self.team1_id)
            self.player_minutes[player_name] = 0
        
        # Team 2 players
        roster2 = get_team_roster(self.team2_id) if self.team2_id else [f"{self.team2}Player{i}" for i in range(1, 6)]
        for player_name in roster2:
            self.players[player_name] = Player(player_name, self.team2, self.team2_id)
            self.player_minutes[player_name] = 0
        
        # Select 5 starters for each team
        self.substitute_players(self.team1)
        self.substitute_players(self.team2)
    
    def substitute_players(self, team):
        """Substitute players based on minutes played"""
        roster = [p for p_name, p in self.players.items() if p.team == team]
        
        # Sort by minutes played (less minutes get priority)
        roster.sort(key=lambda p: self.player_minutes.get(p.name, 0))
        
        # Select the top 5 players with least minutes
        self.active_players[team] = roster[:5]
        
        self.add_event(f"Substitution for {team}: {', '.join([p.name for p in self.active_players[team]])}")
    
    def add_event(self, event):
        with self.event_lock:
            self.events.append((time.time(), event))
            logging.info(f"[{self.team1} vs {self.team2}] {event}")
    
    def get_random_active_player(self, team):
        """Get a random active player from a team"""
        team_players = self.active_players[team]
        return random.choice(team_players) if team_players else None
    
    def simulate_possession(self, offense_team):
        """Simulate a basketball possession"""
        defense_team = self.team2 if offense_team == self.team1 else self.team1
        
        # Get offensive and defensive players
        offense_player = self.get_random_active_player(offense_team)
        defense_player = self.get_random_active_player(defense_team)
        
        if not offense_player or not defense_player:
            return
        
        # Determine if there's a turnover before a shot attempt
        turnover_chance = 0.15  # 15% chance of turnover per possession
        
        if offense_player.real_stats:
            # Adjust turnover chance based on player stats
            turnover_chance = min(0.3, max(0.05, offense_player.real_stats['tov_per_min'] * 3))
        
        if random.random() < turnover_chance:
            # Turnover
            offense_player.update_stat('turnovers', 1)
            
            # Check if it's a steal
            steal_chance = 0.5  # 50% of turnovers are steals
            if defense_player.real_stats:
                steal_chance = min(0.8, max(0.2, defense_player.real_stats['stl_per_min'] * 10))
            
            if random.random() < steal_chance:
                defense_player.update_stat('steals', 1)
                self.add_event(f"{defense_player.name} steals the ball from {offense_player.name}")
            else:
                self.add_event(f"{offense_player.name} turns the ball over")
            
            return
        
        # Determine shot type based on player's style
        shot_type = offense_player.choose_shot_type()
        
        # Check for a block before the shot
        block_chance = 0.05  # 5% chance of a block
        if defense_player.real_stats:
            block_chance = min(0.15, max(0.01, defense_player.real_stats['blk_per_min'] * 5))
        
        if random.random() < block_chance:
            defense_player.update_stat('blocks', 1)
            self.add_event(f"{defense_player.name} blocks {offense_player.name}'s shot")
            
            # Determine who gets the rebound after block
            if random.random() < 0.6:  # Defense gets rebound 60% of the time after block
                rebounder = self.get_random_active_player(defense_team)
                if rebounder:
                    rebounder.update_stat('rebounds', 1)
                    self.add_event(f"{rebounder.name} rebounds after the block")
            else:
                rebounder = self.get_random_active_player(offense_team)
                if rebounder:
                    rebounder.update_stat('rebounds', 1)
                    self.add_event(f"{rebounder.name} recovers the ball after the block")
            
            return
        
        # Attempt the shot
        shot_made = offense_player.simulate_shot(shot_type)
        
        if shot_made:
            # Update score
            points = 2 if shot_type == '2PT' else 3 if shot_type == '3PT' else 1
            self.score[offense_team] += points
            
            # Check if there was an assist
            assist_chance = 0.6 if shot_type == '2PT' else 0.8 if shot_type == '3PT' else 0
            
            if random.random() < assist_chance:
                # Choose an assisting player that's not the shooter
                potential_assisters = [p for p in self.active_players[offense_team] if p != offense_player]
                if potential_assisters:
                    assisting_player = random.choice(potential_assisters)
                    assisting_player.update_stat('assists', 1)
                    self.add_event(f"{offense_player.name} scores {points} points, assisted by {assisting_player.name}")
                else:
                    self.add_event(f"{offense_player.name} scores {points} points")
            else:
                self.add_event(f"{offense_player.name} scores {points} points")
        else:
            # Shot missed, rebound opportunity
            self.add_event(f"{offense_player.name} misses a {shot_type} shot")
            
            # Determine rebound probabilities
            def_rebound_chance = 0.7 if shot_type in ['2PT', '3PT'] else 0.15  # Lower for free throws
            
            if random.random() < def_rebound_chance:
                # Defensive rebound
                rebounder = self.get_random_active_player(defense_team)
                if rebounder:
                    rebounder.update_stat('rebounds', 1)
                    self.add_event(f"{rebounder.name} grabs the defensive rebound")
            else:
                # Offensive rebound
                rebounder = self.get_random_active_player(offense_team)
                if rebounder:
                    rebounder.update_stat('rebounds', 1)
                    self.add_event(f"{rebounder.name} grabs the offensive rebound")
    
    def simulate_quarter(self, quarter):
        """Simulate a quarter of basketball"""
        self.add_event(f"Quarter {quarter} started")
        
        # Reset quarter time
        self.current_quarter_time = 0
        
        # Initial possession
        self.possession_team = random.choice([self.team1, self.team2])
        
        # Substitute players at the beginning of each quarter
        self.substitute_players(self.team1)
        self.substitute_players(self.team2)
        
        # Simulate the quarter in 2-minute segments
        while self.current_quarter_time < self.quarter_length:
            segment_duration = min(120, self.quarter_length - self.current_quarter_time)  # 2 minutes or remainder
            self.current_quarter_time += segment_duration
            
            # Update player minutes and simulate stats for the 2-minute segment
            for player in self.active_players[self.team1] + self.active_players[self.team2]:
                minutes_played = segment_duration / 60  # Convert seconds to minutes
                self.player_minutes[player.name] += minutes_played
                player.simulate_player_actions(minutes_played)
            
            ## Estimate number of possessions based on typical NBA pace
            possessions_per_minute = 2  # Average pace
            possessions_in_segment = int(segment_duration / 60 * possessions_per_minute * 2)  # For both teams
            
            for _ in range(possessions_in_segment):
                # Simulate a possession
                self.simulate_possession(self.possession_team)
                
                # Alternate possession
                self.possession_team = self.team2 if self.possession_team == self.team1 else self.team1
            
            # Consider substitutions every 2 minutes
            if random.random() < 0.4:  # 40% chance of substitution
                team_to_sub = random.choice([self.team1, self.team2])
                self.substitute_players(team_to_sub)
        
        # End of quarter stats
        self.quarters_completed += 1
        self.add_event(f"End of Quarter {quarter}: {self.team1} {self.score[self.team1]} - {self.team2} {self.score[self.team2]}")
    
    def run(self):
        """Main game simulation method"""
        self.add_event(f"Game started: {self.team1} vs {self.team2} at {self.arena}")
        game_start_time = time.time()
        
        # Simulate 4 quarters
        for quarter in range(1, 5):
            self.simulate_quarter(quarter)
            
            # Simulate quarter break
            time.sleep(0.1)  # Brief pause to simulate quarter break
        
        # Check if overtime is needed
        overtime_count = 0
        while self.score[self.team1] == self.score[self.team2]:
            overtime_count += 1
            self.add_event(f"Overtime {overtime_count} started")
            
            # Reset for overtime
            self.current_quarter_time = 0
            self.quarter_length = 5 * 60  # 5 minutes for overtime
            
            # Simulate overtime
            self.simulate_quarter(f"OT{overtime_count}")
        
        # Game ended
        winner = self.team1 if self.score[self.team1] > self.score[self.team2] else self.team2
        game_duration = time.time() - game_start_time
        
        self.add_event(f"Game ended: {self.team1} {self.score[self.team1]} - {self.team2} {self.score[self.team2]}")
        self.add_event(f"{winner} wins! Game took {game_duration:.2f} seconds to simulate")
        
        # Save results
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
                'player_stats': {player.name: player.get_stats_dict() for player in self.players.values()}
            }
        
        # Signal game ended
        self.game_ended.set()
        
        # Save to database
        try:
            save_game_to_db(self.game_id, game_results[self.game_id])
        except Exception as e:
            logging.error(f"Error saving game to database: {e}")

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

# Process incoming stats data in a dedicated thread
class StatsProcessor(threading.Thread):
    def __init__(self):
        super().__init__(name="StatsProcessor", daemon=True)
        self.running = True
    
    def run(self):
        logging.info("Stats processor started")
        
        while self.running:
            try:
                # Get stats from queue with timeout
                stats = stats_queue.get(timeout=1)
                
                # Process the stats (e.g., calculate advanced metrics)
                if 'game_id' in stats and 'player_stats' in stats:
                    logging.info(f"Processing stats for game {stats['game_id']}")
                    
                    # Perform some aggregation or analysis
                    for player, player_stats in stats['player_stats'].items():
                        # Example: Calculate player efficiency rating
                        points = player_stats.get('points', 0)
                        rebounds = player_stats.get('rebounds', 0)
                        assists = player_stats.get('assists', 0)
                        steals = player_stats.get('steals', 0)
                        blocks = player_stats.get('blocks', 0)
                        fg_attempts = player_stats.get('fg_attempts', 0)
                        ft_attempts = player_stats.get('ft_attempts', 0)
                        turnovers = player_stats.get('turnovers', 0)
                        
                        # Simple PER formula (not the actual one, just an example)
                        per = (points + rebounds + assists + steals + blocks) - (fg_attempts - player_stats.get('fg_made', 0) + 
                                                                               ft_attempts - player_stats.get('ft_made', 0) + 
                                                                               turnovers)
                        
                        player_stats['per'] = per
                
                stats_queue.task_done()
            
            except queue.Empty:
                # No stats to process, continue
                pass
            except Exception as e:
                logging.error(f"Error processing stats: {e}")
    
    def stop(self):
        self.running = False

def simulate_multiple_games(num_games=5):
    """Simulate multiple NBA games in parallel"""
    logging.info(f"Starting simulation of {num_games} NBA games")
    
    # Initialize database
    init_database()
    
    # Create a stats processor
    stats_processor = StatsProcessor()
    stats_processor.start()
    
    # Create and start game threads
    games = []
    stadium_ops = []
    
    for i in range(num_games):
        # Select random teams
        team_codes = list(NBA_TEAMS.keys())
        team1_id, team2_id = random.sample(team_codes, 2)
        
        team1 = NBA_TEAMS[team1_id]['name']
        team2 = NBA_TEAMS[team2_id]['name']
        arena = NBA_TEAMS[team1_id]['arena']
        game_id = i + 1
        
        # Create and start game thread
        game = NBA_Game(team1, team2, game_id, arena, team1_id=team1_id, team2_id=team2_id)
        games.append(game)
        game.start()
        
        # Create and start stadium operations for this game
        operations = ["ticket_processing", "concessions", "security", "facilities"]
        for op_type in operations:
            op = StadiumOperation(game_id, arena, op_type)
            stadium_ops.append(op)
            op.start()
    
    # Wait for all games to finish
    for game in games:
        game.join()
        
        # After game finishes, get the results and add to stats queue
        with game_lock:
            if game.game_id in game_results:
                stats_queue.put(game_results[game.game_id])
    
    # Stop stadium operations
    for op in stadium_ops:
        op.stop()
    
    # Wait for stats processor to finish processing
    stats_queue.join()
    stats_processor.stop()
    
    logging.info("All games completed, generating summary...")
    
    # Generate summary statistics
    total_points = 0
    games_completed = 0
    
    with game_lock:
        for game_id, result in game_results.items():
            total_points += result['score1'] + result['score2']
            games_completed += 1
    
    if games_completed > 0:
        avg_points_per_game = total_points / games_completed
        logging.info(f"Game Simulation Complete: {games_completed} games, Avg points: {avg_points_per_game:.1f}")
        
        # Write summary to a file
        with open('simulation_summary.txt', 'w') as f:
            f.write(f"NBA Simulation Summary\n")
            f.write(f"Games Completed: {games_completed}\n")
            f.write(f"Average Points Per Game: {avg_points_per_game:.1f}\n\n")
            
            # Write game results
            f.write("Game Results:\n")
            for game_id, result in game_results.items():
                f.write(f"Game {game_id}: {result['team1']} {result['score1']} - {result['team2']} {result['score2']}, Winner: {result['winner']}\n")
    
    return game_results

def run_simulation_with_pool(num_games=5, num_workers=None):
    """Run simulation using a process pool for potential performance boost"""
    logging.info(f"Starting simulation using process pool with {num_games} games")
    
    # Initialize database
    init_database()
    
    # Use ProcessPoolExecutor to distribute game simulations
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        # Prepare game configurations
        game_configs = []
        
        for i in range(num_games):
            # Select random teams
            team_codes = list(NBA_TEAMS.keys())
            team1_id, team2_id = random.sample(team_codes, 2)
            
            team1 = NBA_TEAMS[team1_id]['name']
            team2 = NBA_TEAMS[team2_id]['name']
            arena = NBA_TEAMS[team1_id]['arena']
            game_id = i + 1
            
            game_configs.append((team1, team2, game_id, arena, team1_id, team2_id))
        
        # Define worker function for process pool
        def simulate_game_in_process(config):
            team1, team2, game_id, arena, team1_id, team2_id = config
            game = NBA_Game(team1, team2, game_id, arena, team1_id=team1_id, team2_id=team2_id)
            game.run()  # Run directly rather than starting a thread
            return game_id, game_results.get(game_id)
        
        # Execute games in parallel
        results = list(executor.map(simulate_game_in_process, game_configs))
    
    logging.info("All games completed in process pool, generating summary...")
    
    # Collect and process results
    process_results = {}
    for game_id, result in results:
        if result:
            process_results[game_id] = result
    
    # Generate summary statistics
    total_points = 0
    games_completed = len(process_results)
    
    for game_id, result in process_results.items():
        total_points += result['score1'] + result['score2']
    
    if games_completed > 0:
        avg_points_per_game = total_points / games_completed
        logging.info(f"Game Simulation Complete (Process Pool): {games_completed} games, Avg points: {avg_points_per_game:.1f}")
        
        # Write summary to a file
        with open('simulation_summary_pool.txt', 'w') as f:
            f.write(f"NBA Simulation Summary (Process Pool)\n")
            f.write(f"Games Completed: {games_completed}\n")
            f.write(f"Average Points Per Game: {avg_points_per_game:.1f}\n\n")
            
            # Write game results
            f.write("Game Results:\n")
            for game_id, result in process_results.items():
                f.write(f"Game {game_id}: {result['team1']} {result['score1']} - {result['team2']} {result['score2']}, Winner: {result['winner']}\n")
    
    return process_results

def generate_player_stats_report():
    """Generate a detailed player stats report from the database"""
    try:
        conn = sqlite3.connect('nba_simulation.db')
        cursor = conn.cursor()
        
        # Get all player stats
        cursor.execute('''
        SELECT player_name, team, 
               SUM(points) as total_points, 
               SUM(rebounds) as total_rebounds,
               SUM(assists) as total_assists,
               SUM(steals) as total_steals,
               SUM(blocks) as total_blocks,
               SUM(minutes) as total_minutes,
               COUNT(*) as games_played
        FROM player_stats
        GROUP BY player_name, team
        ORDER BY total_points DESC
        ''')
        
        player_stats = cursor.fetchall()
        
        # Write to CSV
        with open('player_stats_report.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Player', 'Team', 'Games', 'Minutes', 'Points', 'Rebounds', 
                             'Assists', 'Steals', 'Blocks', 'PPG', 'RPG', 'APG'])
            
            for stats in player_stats:
                player, team, points, rebounds, assists, steals, blocks, minutes, games = stats
                
                # Calculate per-game averages
                ppg = round(points / games, 1) if games > 0 else 0
                rpg = round(rebounds / games, 1) if games > 0 else 0
                apg = round(assists / games, 1) if games > 0 else 0
                
                writer.writerow([player, team, games, round(minutes, 1), points, rebounds, 
                                 assists, steals, blocks, ppg, rpg, apg])
        
        logging.info(f"Player stats report generated with {len(player_stats)} players")
        
        conn.close()
        return len(player_stats)
    
    except Exception as e:
        logging.error(f"Error generating player stats report: {e}")
        return 0

def generate_team_stats_report():
    """Generate team performance stats from the database"""
    try:
        conn = sqlite3.connect('nba_simulation.db')
        cursor = conn.cursor()
        
        # Get team win/loss records
        cursor.execute('''
        SELECT team1, team2, winner, score1, score2 FROM games
        ''')
        
        games = cursor.fetchall()
        
        # Calculate team records
        team_stats = {}
        
        for game in games:
            team1, team2, winner, score1, score2 = game
            
            # Ensure teams are in the dictionary
            for team in (team1, team2):
                if team not in team_stats:
                    team_stats[team] = {
                        'wins': 0, 'losses': 0, 
                        'points_for': 0, 'points_against': 0,
                        'games': 0
                    }
            
            # Update stats
            team_stats[team1]['games'] += 1
            team_stats[team2]['games'] += 1
            
            team_stats[team1]['points_for'] += score1
            team_stats[team1]['points_against'] += score2
            
            team_stats[team2]['points_for'] += score2
            team_stats[team2]['points_against'] += score1
            
            if winner == team1:
                team_stats[team1]['wins'] += 1
                team_stats[team2]['losses'] += 1
            else:
                team_stats[team2]['wins'] += 1
                team_stats[team1]['losses'] += 1
        
        # Write to CSV
        with open('team_stats_report.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['Team', 'Wins', 'Losses', 'Win%', 'Points For', 'Points Against', 
                            'Point Differential', 'Points Per Game', 'Points Allowed Per Game'])
            
            for team, stats in sorted(team_stats.items(), key=lambda x: x[1]['wins'], reverse=True):
                games = stats['games']
                wins = stats['wins']
                losses = stats['losses']
                points_for = stats['points_for']
                points_against = stats['points_against']
                
                win_pct = round(wins / games * 100, 1) if games > 0 else 0
                diff = points_for - points_against
                ppg = round(points_for / games, 1) if games > 0 else 0
                papg = round(points_against / games, 1) if games > 0 else 0
                
                writer.writerow([team, wins, losses, f"{win_pct}%", points_for, points_against, 
                                diff, ppg, papg])
        
        logging.info(f"Team stats report generated with {len(team_stats)} teams")
        
        conn.close()
        return len(team_stats)
    
    except Exception as e:
        logging.error(f"Error generating team stats report: {e}")
        return 0

def generate_game_events_report(game_id):
    """Generate a detailed report of events for a specific game"""
    try:
        # Check if the game results are in memory
        with game_lock:
            if game_id in game_results and 'events' in game_results[game_id]:
                events = game_results[game_id]['events']
                team1 = game_results[game_id]['team1']
                team2 = game_results[game_id]['team2']
                score1 = game_results[game_id]['score1']
                score2 = game_results[game_id]['score2']
                
                # Write to a text file
                with open(f'game_{game_id}_events.txt', 'w') as f:
                    f.write(f"Game Events: {team1} vs {team2}\n")
                    f.write(f"Final Score: {team1} {score1} - {team2} {score2}\n\n")
                    
                    for timestamp, event in events:
                        formatted_time = datetime.fromtimestamp(timestamp).strftime('%H:%M:%S')
                        f.write(f"[{formatted_time}] {event}\n")
                
                logging.info(f"Game events report generated for game {game_id} with {len(events)} events")
                return len(events)
            else:
                logging.warning(f"Game {game_id} not found in memory or has no events")
                return 0
    
    except Exception as e:
        logging.error(f"Error generating game events report: {e}")
        return 0

def data_analysis():
    """Perform data analysis on simulation results"""
    try:
        conn = sqlite3.connect('nba_simulation.db')
        
        # Use pandas for analysis
        # Game analysis
        games_df = pd.read_sql_query("SELECT * FROM games", conn)
        
        # Player stats analysis
        player_stats_df = pd.read_sql_query("SELECT * FROM player_stats", conn)
        
        # Stadium operations analysis
        stadium_ops_df = pd.read_sql_query("SELECT * FROM stadium_ops", conn)
        
        # Sample analyses
        
        # 1. Average score per team
        avg_scores = pd.DataFrame({
            'Team1 Avg': [games_df['score1'].mean()],
            'Team2 Avg': [games_df['score2'].mean()],
            'Combined Avg': [(games_df['score1'] + games_df['score2']).mean() / 2]
        })
        
        # 2. Top scoring players
        top_scorers = player_stats_df.groupby('player_name').agg(
            games_played=('game_id', 'count'),
            total_points=('points', 'sum'),
            avg_points=('points', 'mean'),
            total_rebounds=('rebounds', 'sum'),
            total_assists=('assists', 'sum')
        ).sort_values('avg_points', ascending=False).head(10)
        
        # 3. Stadium operations efficiency
        stadium_efficiency = stadium_ops_df.groupby('operation_type').agg(
            avg_processed=('processed_count', 'mean'),
            total_operations=('id', 'count')
        )
        
        # Save results to CSV files
        avg_scores.to_csv('analysis_avg_scores.csv', index=False)
        top_scorers.to_csv('analysis_top_scorers.csv')
        stadium_efficiency.to_csv('analysis_stadium_efficiency.csv')
        
        # Create visualizations using matplotlib
        try:
            import matplotlib.pyplot as plt
            
            # 1. Score distribution
            plt.figure(figsize=(10, 6))
            plt.hist(games_df['score1'], alpha=0.5, label='Team 1')
            plt.hist(games_df['score2'], alpha=0.5, label='Team 2')
            plt.title('Distribution of Team Scores')
            plt.xlabel('Score')
            plt.ylabel('Frequency')
            plt.legend()
            plt.savefig('analysis_score_distribution.png')
            
            # 2. Top scorers bar chart
            plt.figure(figsize=(12, 8))
            top_10_scorers = top_scorers.head(10)
            plt.barh(top_10_scorers.index, top_10_scorers['avg_points'])
            plt.title('Top 10 Scorers - Average Points Per Game')
            plt.xlabel('Average Points')
            plt.tight_layout()
            plt.savefig('analysis_top_scorers.png')
            
            # 3. Stadium operations comparison
            plt.figure(figsize=(10, 6))
            plt.bar(stadium_efficiency.index, stadium_efficiency['avg_processed'])
            plt.title('Average Processing Count by Operation Type')
            plt.ylabel('Average Processed Count')
            plt.tight_layout()
            plt.savefig('analysis_stadium_ops.png')
            
            logging.info("Data visualizations created successfully")
        except ImportError:
            logging.warning("Matplotlib not available, skipping visualizations")
        
        conn.close()
        logging.info("Data analysis completed successfully")
        
        return {
            'games_analyzed': len(games_df),
            'players_analyzed': player_stats_df['player_name'].nunique(),
            'avg_score': avg_scores['Combined Avg'].values[0]
        }
    
    except Exception as e:
        logging.error(f"Error in data analysis: {e}")
        return {'error': str(e)}

if __name__ == "__main__":
    logging.info("NBA Simulation starting")
    
    # Ensure data directory exists
    os.makedirs('data', exist_ok=True)
    
    # Initialize the database
    init_database()
    
    # Run the simulation
    num_games = 10
    logging.info(f"Simulating {num_games} NBA games")
    
    # Choose simulation method based on system capabilities
    if os.cpu_count() >= 4:
        results = run_simulation_with_pool(num_games, num_workers=min(os.cpu_count() - 1, num_games))
    else:
        results = simulate_multiple_games(num_games)
    
    # Generate reports
    player_count = generate_player_stats_report()
    team_count = generate_team_stats_report()

    # Log simulation summary
    logging.info(f"Simulation complete: {num_games} games simulated")
    logging.info(f"Statistics generated for {player_count} players and {team_count} teams")
    
    # Display summary of results
    print("\n===== SIMULATION SUMMARY =====")
    print(f"Games simulated: {num_games}")
    print(f"Player statistics: {player_count}")
    print(f"Team statistics: {team_count}")
