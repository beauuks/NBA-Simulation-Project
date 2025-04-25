import random
import time
import threading
import logging
import datetime
import json

from src.globals import game_lock, game_results, NBA_PLAYERS
from src.database import save_game_to_db

# load player stats from JSON file
with open('data/player_stats.json', 'r') as f:
    player_stats = json.load(f)

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
            'two_pt': 0,
            'three_pt': 0,
            'free_throws': 0,
            'turnovers': 0,
            'rebounds': 0,
            'assists': 0,
            'steals': 0,
            'blocks': 0
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
        self.game_id = game_id
        self.team1 = team1
        self.team2 = team2
        self.team1_id = team1_id
        self.team2_id = team2_id
        self.arena = arena or f"{team1} Arena"
        self.date = date or datetime.now().strftime('%Y-%m-%d')
        
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
        roster1 = get_team_roster(self.team1_id) 
        for player_name in roster1:
            self.players[player_name] = Player(player_name, self.team1)
        
        # Team 2 players
        roster2 = get_team_roster(self.team2_id) 
        for player_name in roster2:
            self.players[player_name] = Player(player_name, self.team2)
    
    def add_event(self, event):
        with self.event_lock:
            self.events.append((time.time(), event))
            logging.info(f"[{self.team1} vs {self.team2}] {event}")
    
    def get_random_player(self, team):
        """Get a random player from a team"""
        team_players = [p for p in self.players.values() if p.team == team]
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
                success = random.random() < player_stats[offense_player]['2p%']  
                if success:
                    self.score[offense_team] += 2
                    offense_player.update_stat('points', 2)
                    offense_player.update_stat('two_pt', 1)
                    
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
                success = random.random() < player_stats[offense_player]['3p%'] 
                if success:
                    self.score[offense_team] += 3
                    offense_player.update_stat('points', 3)
                    offense_player.update_stat('three_pt', 1)
                    
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
                    if random.random() < player_stats[offense_player]['ft%']:  # 75% free throw success
                        made += 1
                
                if made > 0:
                    self.score[offense_team] += made
                    offense_player.update_stat('points', made)
                
                self.add_event(f"{offense_player.name} makes {made} of {shots} free throws")
            
            elif play_type == 'TO':
                offense_player.update_stat('turnovers')
                self.add_event(f"{offense_player.name} turns the ball over to {defense_team}")
            
            elif play_type == 'STEAL':
                defense_player.update_stat('steals')
                self.add_event(f"{defense_player.name} steals the ball from {offense_player.name}")
            
            elif play_type == 'BLOCK':
                defense_player.update_stat('blocks')
                self.add_event(f"{defense_player.name} blocks {offense_player.name}'s shot")
            
            # Short sleep
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