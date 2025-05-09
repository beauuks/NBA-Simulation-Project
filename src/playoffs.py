import random
from datetime import datetime, timedelta
import logging
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from src.nba_classes import NBA_Game
from src.globals import NBA_TEAMS, game_results
from src.database import save_game_to_db

def get_team_standings():
    """Get team standings from the database"""
    conn = sqlite3.connect('nba_simulation.db')
    cursor = conn.cursor()

    # Get all games from the database
    cursor.execute('SELECT team1, team2, winner FROM games')
    games = cursor.fetchall()
    conn.close()

    # Initialize standings
    standings = {}
    for team_info in NBA_TEAMS.values():
        team_name = team_info['name']
        standings[team_name] = {
            'name': team_name,
            'arena': team_info['arena'],
            'conference': 'East' if NBA_TEAMS.values().index(team_info) < 15 else 'West',
            'wins': 0,
            'losses': 0
        }

    # Calculate wins and losses
    for team1, team2, winner in games:
        if team1 in standings and team2 in standings:
            if winner == team1:
                standings[team1]['wins'] += 1
                standings[team2]['losses'] += 1
            elif winner == team2:
                standings[team2]['wins'] += 1
                standings[team1]['losses'] += 1

    return standings

def create_playoff_bracket():
    """Create playoff brackets based on team standings"""
    standings = get_team_standings()
    
    # Split teams by conference
    east_teams = [team for team in standings.values() if team['conference'] == 'East']
    west_teams = [team for team in standings.values() if team['conference'] == 'West']
    
    # Sort by win percentage
    east_teams.sort(key=lambda x: x['wins']/(x['wins']+x['losses']) if x['wins']+x['losses'] > 0 else 0, reverse=True)
    west_teams.sort(key=lambda x: x['wins']/(x['wins']+x['losses']) if x['wins']+x['losses'] > 0 else 0, reverse=True)
    
    # Take top 8 teams from each conference
    top_east = east_teams[:8]
    top_west = west_teams[:8]
    
    # Create matchups
    east_matchups = [
        (top_east[0]['name'], top_east[7]['name']),
        (top_east[1]['name'], top_east[6]['name']),
        (top_east[2]['name'], top_east[5]['name']),
        (top_east[3]['name'], top_east[4]['name'])
    ]
    
    west_matchups = [
        (top_west[0]['name'], top_west[7]['name']),
        (top_west[1]['name'], top_west[6]['name']),
        (top_west[2]['name'], top_west[5]['name']),
        (top_west[3]['name'], top_west[4]['name'])
    ]
    
    return {
        'Eastern Conference': east_matchups,
        'Western Conference': west_matchups
    }

def generate_playoff_schedule(playoff_bracket, start_date=datetime(2024, 4, 20)):
    """Generate a playoff schedule from the bracket"""
    schedule = []
    current_date = start_date
    series_games = 7  # Best of 7 series
    
    # Generate schedule for each round
    rounds = [
        {"name": "First Round", "matchups": playoff_bracket},
        {"name": "Conference Semifinals", "matchups": {}},
        {"name": "Conference Finals", "matchups": {}},
        {"name": "NBA Finals", "matchups": {}}
    ]
    
    # Schedule first round
    for conference, matchups in rounds[0]["matchups"].items():
        for i, (team1, team2) in enumerate(matchups):
            # Find team IDs and arenas
            team1_info = next(info for info in NBA_TEAMS.values() if info['name'] == team1)
            team2_info = next(info for info in NBA_TEAMS.values() if info['name'] == team2)
            
            # Alternate home court
            home_games = [0, 1, 4, 6]  # Games 1, 2, 5, 7 at home court
            
            # Schedule all potential games
            for game_num in range(1, series_games + 1):
                home_team = team1 if game_num - 1 in home_games else team2
                away_team = team2 if home_team == team1 else team1
                arena = team1_info['arena'] if home_team == team1 else team2_info['arena']
                
                game_id = f"{conference}-R1-{i+1}-G{game_num}"
                
                schedule.append({
                    'game_id': game_id,
                    'home': home_team,
                    'away': away_team,
                    'arena': arena,
                    'date': current_date.strftime('%Y-%m-%d'),
                    'series': f"{conference} First Round: {team1} vs {team2}",
                    'game_num': game_num,
                    'must_win': False  # Will be updated during simulation
                })
                
                # Add 1-2 days between games
                current_date += timedelta(days=random.choice([1, 2]))
            
            # Add break between series
            current_date += timedelta(days=2)
    
    return schedule

def simulate_playoff_series(series_schedule):
    """Simulate a playoff series based on the schedule"""
    series_results = {}
    
    # Group games by series
    series_games = {}
    for game in series_schedule:
        if game['series'] not in series_games:
            series_games[game['series']] = []
        series_games[game['series']].append(game)
    
    # Simulate each series
    for series_name, games in series_games.items():
        # Sort games by game number
        games.sort(key=lambda x: x['game_num'])
        
        # Extract teams
        team1 = games[0]['home']
        team2 = games[0]['away']
        
        # Track wins
        wins = {team1: 0, team2: 0}
        played_games = []
        
        # Simulate games until one team reaches 4 wins
        for game in games:
            if max(wins.values()) < 4:  # Series not decided yet
                # Find team IDs
                team1_id = next((id for id, info in NBA_TEAMS.items() if info['name'] == game['home']), None)
                team2_id = next((id for id, info in NBA_TEAMS.items() if info['name'] == game['away']), None)
                
                # Create and run game
                game_instance = NBA_Game(
                    game['home'], 
                    game['away'], 
                    game['game_id'],
                    arena=game['arena'],
                    date=game['date'],
                    team1_id=team1_id,
                    team2_id=team2_id
                )
                game_instance.run()
                
                # Get result and update wins
                if game['game_id'] in game_results:
                    result = game_results[game['game_id']]
                    winner = result['winner']
                    wins[winner] += 1
                    played_games.append({
                        'game_num': game['game_num'],
                        'home': game['home'],
                        'away': game['away'],
                        'winner': winner,
                        'score': f"{result['score1']}-{result['score2']}"
                    })
                
                # Mark remaining games as must-win if applicable
                if wins[team1] == 3:
                    for g in games:
                        if g['game_num'] > game['game_num']:
                            g['must_win'] = True
                elif wins[team2] == 3:
                    for g in games:
                        if g['game_num'] > game['game_num']:
                            g['must_win'] = True
            else:
                # Series already decided, skip remaining games
                continue
        
        # Determine series winner
        series_winner = team1 if wins[team1] > wins[team2] else team2
        series_results[series_name] = {
            'winner': series_winner,
            'score': f"{wins[team1]}-{wins[team2]}",
            'games': played_games
        }
    
    return series_results

def simulate_playoffs(start_date=datetime(2024, 4, 20)):
    """Simulate the entire NBA playoffs"""
    logging.info("Starting NBA Playoffs simulation")
    
    # Create playoff bracket
    bracket = create_playoff_bracket()
    
    # Generate first round schedule
    schedule = generate_playoff_schedule(bracket, start_date)
    
    # Simulate first round
    logging.info("Simulating First Round")
    first_round_results = simulate_playoff_series(schedule)
    
    # Log results
    for series_name, result in first_round_results.items():
        logging.info(f"{series_name}: {result['winner']} wins {result['score']}")
    
    # Create second round matchups
    semifinals = {
        'Eastern Conference': [],
        'Western Conference': []
    }
    
    for conference in ['Eastern Conference', 'Western Conference']:
        winners = [result['winner'] for name, result in first_round_results.items() if conference in name]
        semifinals[conference] = [(winners[0], winners[1]), (winners[2], winners[3])]
    
    # Generate second round schedule
    second_round_start = start_date + timedelta(days=16)  # ~2 weeks after playoffs start
    second_round_schedule = generate_playoff_schedule(semifinals, second_round_start)
    
    # Simulate second round
    logging.info("Simulating Conference Semifinals")
    semifinals_results = simulate_playoff_series(second_round_schedule)
    
    # Log results
    for series_name, result in semifinals_results.items():
        logging.info(f"{series_name}: {result['winner']} wins {result['score']}")
    
    # Create conference finals matchups
    conf_finals = {
        'Eastern Conference': [],
        'Western Conference': []
    }
    
    for conference in ['Eastern Conference', 'Western Conference']:
        winners = [result['winner'] for name, result in semifinals_results.items() if conference in name]
        conf_finals[conference] = [(winners[0], winners[1])]
    
    # Generate conference finals schedule
    conf_finals_start = second_round_start + timedelta(days=14)
    conf_finals_schedule = generate_playoff_schedule(conf_finals, conf_finals_start)
    
    # Simulate conference finals
    logging.info("Simulating Conference Finals")
    conf_finals_results = simulate_playoff_series(conf_finals_schedule)
    
    # Log results
    for series_name, result in conf_finals_results.items():
        logging.info(f"{series_name}: {result['winner']} wins {result['score']}")
    
    # Create NBA Finals matchup
    east_winner = next(result['winner'] for name, result in conf_finals_results.items() if 'Eastern Conference' in name)
    west_winner = next(result['winner'] for name, result in conf_finals_results.items() if 'Western Conference' in name)
    
    finals = {
        'NBA Finals': [(east_winner, west_winner)]
    }
    
    # Generate NBA Finals schedule
    finals_start = conf_finals_start + timedelta(days=10)
    finals_schedule = generate_playoff_schedule(finals, finals_start)
    
    # Simulate NBA Finals
    logging.info("Simulating NBA Finals")
    finals_results = simulate_playoff_series(finals_schedule)
    
    # Log results
    for series_name, result in finals_results.items():
        logging.info(f"{series_name}: {result['winner']} wins {result['score']}")
    
    # Determine champion
    champion = next(result['winner'] for result in finals_results.values())
    logging.info(f"NBA CHAMPION: {champion}")
    
    # Combine all results
    all_results = {**first_round_results, **semifinals_results, **conf_finals_results, **finals_results}
    return all_results

def simulate_playoff_games_parallel(schedule):
    """Simulate playoff games in parallel using threads"""
    with ThreadPoolExecutor(max_workers=min(8, len(schedule))) as executor:
        futures = []
        
        for game in schedule:
            # Find team IDs
            team1_id = next((id for id, info in NBA_TEAMS.items() if info['name'] == game['home']), None)
            team2_id = next((id for id, info in NBA_TEAMS.items() if info['name'] == game['away']), None)
            
            # Create game instance
            game_instance = NBA_Game(
                game['home'],
                game['away'],
                game['game_id'],
                arena=game['arena'],
                date=game['date'],
                team1_id=team1_id,
                team2_id=team2_id
            )
            
            # Submit game to thread pool
            futures.append(executor.submit(game_instance.run))
        
        # Wait for all games to complete
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                logging.error(f"Error in playoff game simulation: {e}")