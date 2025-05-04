import logging
import uuid
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import random

from src.nba_classes import NBA_Game
from src.stadium_ops import StadiumOperation
from src.globals import NBA_TEAMS

def generate_nba_schedule(season_start_date=datetime(2023, 10, 24), num_games=82):
    """Generates a simplified NBA schedule with game IDs."""

    teams = list(NBA_TEAMS.values())
    schedule = []
    game_date = season_start_date
    days_between_games = 2

    total_games = num_games * len(teams) // 2

    while len(schedule) < total_games:
        home_team = random.choice(teams)
        away_team = random.choice(teams)

        if home_team != away_team:
            game_id = str(uuid.uuid4()) # unique ID for each game
            schedule.append({
                "game_id": game_id, # add game id
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
        all_futures = [] 
        stadium_ops = [] 

        for game in game_schedule: # loop through schedule dictionaries.
            game_id = game["game_id"] # access game_id from dictionary.
            team1 = game["home"]
            team2 = game["away"]
            arena = game["arena"]
            game_date = game["date"]
            
            # Find team IDs based on team names
            team1_id = None
            team2_id = None
            
            # Look up the team IDs using the team names
            for id, team_info in NBA_TEAMS.items():
                if team_info["name"] == team1:
                    team1_id = id
                if team_info["name"] == team2:
                    team2_id = id

            # submit stadium ops
            security = StadiumOperation(game_id, arena, "security")
            concessions = StadiumOperation(game_id, arena, "concessions")
            merchandise = StadiumOperation(game_id, arena, "merchandise")

            stadium_ops.extend([security, concessions, merchandise])

            security_future = executor.submit(security.run)
            concessions_future = executor.submit(concessions.run)
            merchandise_future = executor.submit(merchandise.run)

            # submit game
            game_instance = NBA_Game(team1, team2, game_id, arena, game_date, team1_id, team2_id)
            game_future = executor.submit(game_instance.run)

            all_futures.append(game_future)

            all_futures.extend([security_future, concessions_future, merchandise_future])

        # Wait for all games to complete
        # Use as_completed to process results as they finish and catch exceptions
        for future in as_completed([f for f in all_futures]):
            try:
                future.result()  # This will raise any exception that occurred during execution
            except Exception as e:
                logging.error(f"Error in thread: {e}")
                
        # Signal all stadium operations to stop
        for op in stadium_ops:
            op.stop_event.set()

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