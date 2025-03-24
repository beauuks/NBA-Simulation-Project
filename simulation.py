import time
import threading
import logging
import uuid
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
import random

from nba_classes import NBA_Game
from stadium_ops import StadiumOperation
from globals import NBA_TEAMS

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