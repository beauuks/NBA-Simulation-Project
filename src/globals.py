import threading
import queue 
import json

# Thread-safe data structures
game_results = {}
playoff_results = {}
game_lock = threading.Lock()

# Load the JSON file
with open('data/nba_data.json', 'r') as f:
    nba_data = json.load(f)

# Access the data
NBA_TEAMS = nba_data['NBA_TEAMS']
NBA_PLAYERS = nba_data['NBA_PLAYERS']
