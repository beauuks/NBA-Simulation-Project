import logging
import uuid
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor, as_completed
import random

from src.nba_classes import NBA_Game
from src.stadium_ops import StadiumOperation
from src.globals import NBA_TEAMS

#def generate_nba_schedule(season_start_date=datetime(2023, 10, 24), num_games=82):
#    teams = list(NBA_TEAMS.values())
#    schedule = []
#    game_date = season_start_date
#    
#    # Track which teams are playing on which dates
#    team_schedule = {team["name"]: [] for team in teams}
#    arena_schedule = {team["arena"]: [] for team in teams}
#    
#    while len(schedule) < (num_games * len(teams) // 2):
#        # Reset availability for this date
#        available_home_teams = [team for team in teams 
#                               if game_date.strftime("%Y-%m-%d") not in team_schedule[team["name"]]
#                               and game_date.strftime("%Y-%m-%d") not in arena_schedule[team["arena"]]]
#        
#        if not available_home_teams:
#            # No available home teams today, move to next day
#            game_date += timedelta(days=1)
#            continue
#            
#        home_team = random.choice(available_home_teams)
#        
#        # Find available away teams (not playing today and not the home team)
#        available_away_teams = [team for team in teams 
#                               if game_date.strftime("%Y-%m-%d") not in team_schedule[team["name"]]
#                               and team["name"] != home_team["name"]]
#        
#        if not available_away_teams:
#            # No available away teams, move to next day
#            game_date += timedelta(days=1)
#            continue
#            
#        away_team = random.choice(available_away_teams)
#        
#        # Add game to schedule
#        game_id = str(uuid.uuid4())
#        date_str = game_date.strftime("%Y-%m-%d")
#        
#        schedule.append({
#            "game_id": game_id,
#            "home": home_team["name"],
#            "away": away_team["name"],
#            "arena": home_team["arena"],
#            "date": date_str
#        })
#        
#        # Update team and arena availability
#        team_schedule[home_team["name"]].append(date_str)
#        team_schedule[away_team["name"]].append(date_str)
#        arena_schedule[home_team["arena"]].append(date_str)
#        
#        # If we've scheduled several games today, move to next day
#        if sum(1 for game in schedule if game["date"] == date_str) >= len(teams) // 4:
#            game_date += timedelta(days=1)
#    
#    return schedule

def generate_nba_schedule(season_start_date=datetime(2023, 10, 24), num_games=82):
    """ Generate the NBA regular season schedule """
    teams = list(NBA_TEAMS.values())
    eastern_teams = teams[:15]
    western_teams = teams[15:]

    def generate_conference_schedule(conference_teams):
        """Generate a schedule for a single conference """
        schedule = []
        game_date = season_start_date
        team_schedule = {team["name"]: [] for team in conference_teams}
        arena_schedule = {team["arena"]: [] for team in conference_teams}
        total_games = num_games * len(conference_teams) // 2  # Total games for this conference

        while len(schedule) < total_games:
            # Find available home teams for the current date
            available_home_teams = [
                team for team in conference_teams
                if game_date.strftime("%Y-%m-%d") not in team_schedule[team["name"]]
                and game_date.strftime("%Y-%m-%d") not in arena_schedule[team["arena"]]
            ]

            if not available_home_teams:
                # No available home teams for this date, move to the next day
                game_date += timedelta(days=1)
                continue

            home_team = random.choice(available_home_teams)

            # Find available away teams (not playing today and not the home team)
            available_away_teams = [
                team for team in conference_teams
                if game_date.strftime("%Y-%m-%d") not in team_schedule[team["name"]]
                and team["name"] != home_team["name"]
            ]

            if not available_away_teams:
                # No available away teams for this date, move to the next day
                game_date += timedelta(days=1)
                continue

            away_team = random.choice(available_away_teams)

            # Schedule the game
            game_id = str(uuid.uuid4())
            date_str = game_date.strftime("%Y-%m-%d")

            schedule.append({
                "game_id": game_id,
                "home": home_team["name"],
                "away": away_team["name"],
                "arena": home_team["arena"],
                "date": date_str
            })

            # Update team and arena schedules
            team_schedule[home_team["name"]].append(date_str)
            team_schedule[away_team["name"]].append(date_str)
            arena_schedule[home_team["arena"]].append(date_str)

            # If the maximum number of games for the day is reached, move to the next day
            if sum(1 for game in schedule if game["date"] == date_str) >= len(conference_teams) // 4:
                game_date += timedelta(days=1)

        return schedule

    # Generate schedules for both conferences
    eastern_schedule = generate_conference_schedule(eastern_teams)
    western_schedule = generate_conference_schedule(western_teams)

    # Return schedules for each conference
    return eastern_schedule, western_schedule

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