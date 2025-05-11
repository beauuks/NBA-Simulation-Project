import random
from datetime import datetime, timedelta
import logging
import sqlite3
from concurrent.futures import ThreadPoolExecutor

from src.nba_classes import NBA_Game
from src.globals import NBA_TEAMS, playoff_results
from src.database import save_playoffs_game_to_db, save_playoff_series_to_db
from src.stadium_ops import StadiumOperation

def get_team_standings():
    """Get team standings from the database"""
    conn = sqlite3.connect('nba_simulation.db')
    cursor = conn.cursor()

    # Get all games from the database
    cursor.execute('SELECT team1, team2, winner FROM games')
    games = cursor.fetchall()
    conn.close()

    # split teams by conference
    eastern_teams = ["Boston Celtics", "Miami Heat", "Milwaukee Bucks", "Philadelphia 76ers", 
                    "New York Knicks", "Cleveland Cavaliers", "Atlanta Hawks", "Chicago Bulls", 
                    "Toronto Raptors", "Brooklyn Nets", "Charlotte Hornets", "Indiana Pacers", 
                    "Orlando Magic", "Detroit Pistons", "Washington Wizards"]
    
    # Initialize standings
    standings = {}
    for team_info in NBA_TEAMS.values():
        team_name = team_info['name']
        standings[team_name] = {
            'name': team_name,
            'arena': team_info['arena'],
            'conference': 'East' if team_name in eastern_teams else 'West',
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
    
    # sort the teams by win percentage
    east_teams.sort(key=lambda x: x['wins']/(x['wins']+x['losses']) if x['wins']+x['losses'] > 0 else 0, reverse=True)
    west_teams.sort(key=lambda x: x['wins']/(x['wins']+x['losses']) if x['wins']+x['losses'] > 0 else 0, reverse=True)
    
    # Take top 8 teams from each conference
    top_east = east_teams[:8]
    top_west = west_teams[:8]
    
    # Log the top 8 of east and west
    logging.info("Eastern Conference Playoff Teams (1-8):")
    for i, team in enumerate(top_east):
        logging.info(f"{i+1}. {team['name']} (wins: {team['wins']} - losses: {team['losses']})")
        
    logging.info("Western Conference Playoff Teams (1-8):")
    for i, team in enumerate(top_west):
        logging.info(f"{i+1}. {team['name']} (wins: {team['wins']} - losses: {team['losses']})")
    
    # Create matchups - 1v8, 2v7, 3v6, 4v5 
    east_matchups = [
        (top_east[0]['name'], top_east[7]['name']),
        (top_east[3]['name'], top_east[4]['name']),
        (top_east[2]['name'], top_east[5]['name']),
        (top_east[1]['name'], top_east[6]['name'])
    ]
    
    west_matchups = [
        (top_west[0]['name'], top_west[7]['name']),
        (top_west[3]['name'], top_west[4]['name']),
        (top_west[2]['name'], top_west[5]['name']),
        (top_west[1]['name'], top_west[6]['name'])
    ]
    
    logging.info("Eastern Conference First Round Matchups:")
    for i, (team1, team2) in enumerate(east_matchups):
        logging.info(f"Series {i+1}: {team1} vs {team2}")
        
    logging.info("Western Conference First Round Matchups:")
    for i, (team1, team2) in enumerate(west_matchups):
        logging.info(f"Series {i+1}: {team1} vs {team2}")
    
    return {
        'Eastern Conference': east_matchups,
        'Western Conference': west_matchups
    }

def generate_playoff_schedule(playoff_bracket, start_date=datetime(2024, 4, 20)):
    """Generate a playoff schedule from the bracket"""
    schedule = []
    current_date = start_date
    series_games = 7  # Best of 7 series
    
    # Determine which round we're scheduling based on the bracket structure
    if "NBA Finals" in playoff_bracket:
        round_name = "NBA Finals"
        round_data = playoff_bracket
        round_index = 3
    elif any(("Conference Finals" in key) for key in playoff_bracket.keys()):
        round_name = "Conference Finals"
        round_data = playoff_bracket
        round_index = 2
    elif any(("Conference Semifinals" in key) for key in playoff_bracket.keys()):
        round_name = "Conference Semifinals"
        round_data = playoff_bracket
        round_index = 1
    else:
        # Standard first round format from create_playoff_bracket
        round_name = "First Round"
        round_data = playoff_bracket 
        round_index = 0
    
    # Process the round
    for conference, matchups in round_data.items():
        # For NBA Finals, the conference is just "NBA Finals"
        conf_short = "NBA" if conference == "NBA Finals" else conference.split()[0]
        
        # Round prefix for game_id
        round_prefix = {
            0: "R1",  # First Round
            1: "SF",  # Conference Semifinals
            2: "CF",  # Conference Finals
            3: "F"    # NBA Finals
        }[round_index]
        
        for i, (team1, team2) in enumerate(matchups):
            # Find team IDs and arenas
            team1_info = next(info for info in NBA_TEAMS.values() if info['name'] == team1)
            team2_info = next(info for info in NBA_TEAMS.values() if info['name'] == team2)
            
            # Alternate home court - higher seed gets games 1, 2, 5, 7
            home_games = [0, 1, 4, 6]  # Games 1, 2, 5, 7 at home court
            
            # Schedule all potential games in the series
            for game_num in range(1, series_games + 1):
                home_team = team1 if game_num - 1 in home_games else team2
                away_team = team2 if home_team == team1 else team1
                arena = team1_info['arena'] if home_team == team1 else team2_info['arena']
                
                # Create appropriate game_id based on the round
                if round_name == "NBA Finals":
                    game_id = f"F-G{game_num}"
                    series_desc = f"NBA Finals: {team1} vs {team2}"
                else:
                    game_id = f"{conf_short}-{round_prefix}-{i+1}-G{game_num}"
                    series_desc = f"{conf_short} {round_name}: {team1} vs {team2}"
                
                schedule.append({
                    'game_id': game_id,
                    'home': home_team,
                    'away': away_team,
                    'arena': arena,
                    'date': current_date,
                    'series': series_desc,
                    'game_num': game_num,
                    'must_win': False,  # Will be updated during simulation
                    'conference': conference,
                    'round': round_name
                })
                
                # Add spacing between games (more rest in later rounds)
                days_between = {
                    0: [1, 2],       # 1-2 days in first round
                    1: [2, 2],       # 2 days in semifinals
                    2: [2, 3],       # 2-3 days in conference finals
                    3: [2, 3]        # 2-3 days in NBA finals
                }[round_index]
                
                current_date += timedelta(days=random.choice(days_between))
            
            # Add break between series (longer breaks in later rounds)
            series_break = {
                0: 2,  # 2 days after first round series
                1: 3,  # 3 days after semifinals series
                2: 4,  # 4 days after conference finals
                3: 0   # No break after finals
            }[round_index]
            
            current_date += timedelta(days=series_break)

    return schedule

def simulate_game_with_stadium_ops(game):
    """Simulate a single game with parallel stadium operations"""
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
    
    # Create stadium operations
    security_ops = StadiumOperation(game['game_id'], game['arena'], "security")
    concessions_ops = StadiumOperation(game['game_id'], game['arena'], "concessions")
    merchandise_ops = StadiumOperation(game['game_id'], game['arena'], "merchandise")
    
    # Run stadium operations in parallel with the game
    with ThreadPoolExecutor(max_workers=4) as executor:
        game_future = executor.submit(game_instance.run)
        security_future = executor.submit(security_ops.run)
        concessions_future = executor.submit(concessions_ops.run)
        merchandise_future = executor.submit(merchandise_ops.run)
        
        # Wait for all operations to complete
        game_future.result()
        security_future.result()
        concessions_future.result()
        merchandise_future.result()
    
    # Get game result
    if game['game_id'] in playoff_results:
        result = playoff_results[game['game_id']]
        winner = result['winner']
        
        # Add series information to the result for database
        result['series'] = game['series']
        result['game_number'] = game['game_num']
        result['round'] = game['round']
        result['conference'] = game['conference']
        
        # Save to playoffs database
        save_playoffs_game_to_db(game['game_id'], result)
        
        return {
            'game_num': game['game_num'],
            'home': game['home'],
            'away': game['away'],
            'winner': winner,
            'score': f"{result['score1']}-{result['score2']}"
        }
    
    return None

def simulate_playoff_series(series_schedule):
    """Simulate a playoff series based on the schedule"""
    series_results = {}
    
    if not series_schedule:
        logging.error("No games in the series schedule!")
        return {}
    
    # Group games by series
    series_games = {}
    for game in series_schedule:
        if game['series'] not in series_games:
            series_games[game['series']] = []
        series_games[game['series']].append(game)
    
    # Check if we have any series to simulate
    if not series_games:
        logging.error("No series found in the schedule!")
        return {}
    
    # Simulate each series - series will run in parallel, but games within a series are sequential
    with ThreadPoolExecutor(max_workers=len(series_games)) as executor:
        series_futures = {}
        
        for series_name, games in series_games.items():
            # Sort games by game number
            games.sort(key=lambda x: x['game_num'])
            
            # Submit the series for simulation
            series_futures[series_name] = executor.submit(simulate_single_series, games)
        
        # Collect results
        for series_name, future in series_futures.items():
            series_results[series_name] = future.result()
    
    return series_results

def simulate_single_series(games):
    """Simulate a single playoff series sequentially"""
    # Extract teams
    team1 = games[0]['home']
    team2 = games[0]['away']
    
    # Get series name for logging
    series_name = games[0]['series']
    logging.info(f"Starting series: {series_name} - {team1} vs {team2}")
    
    # Track wins
    wins = {team1: 0, team2: 0}
    played_games = []
    
    # Simulate games until one team reaches 4 wins
    for game in games:
        if max(wins.values()) < 4:  # Series not decided yet
            logging.info(f"Simulating {game['game_id']}: {game['home']} vs {game['away']} at {game['arena']}")
            
            # Simulate this game
            game_result = simulate_game_with_stadium_ops(game)
            
            if game_result:
                winner = game_result['winner']
                wins[winner] += 1
                played_games.append(game_result)
                logging.info(f"Game {game['game_num']} result: {winner} wins ({game_result['score']}). Series: {wins[team1]}-{wins[team2]}")
            
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
            logging.info(f"Skipping {game['game_id']} as series is already decided")
            continue
    
    # Determine series winner
    series_winner = team1 if wins[team1] > wins[team2] else team2
    logging.info(f"Series completed: {series_name} - {series_winner} wins {wins[team1]}-{wins[team2]}")

    # Save series results to database
    save_playoff_series_to_db(
        series_name=series_name,
        team1=team1,
        team2=team2,
        team1_wins=wins[team1],
        team2_wins=wins[team2],
        winner=series_winner,
        conference=games[0]['conference'],
        round_name=games[0]['round']
    )

    return {
        'winner': series_winner,
        'score': f"{wins[team1]}-{wins[team2]}",
        'games': played_games
    }

def simulate_playoffs(start_date=datetime(2024, 4, 20)):
    """Simulate the entire NBA playoffs"""
    
    # Create playoff bracket
    bracket = create_playoff_bracket()
    
    # Generate first round schedule
    first_round_schedule = generate_playoff_schedule(bracket, start_date)
    
    # Simulate first round
    logging.info("Simulating First Round")
    first_round_results = simulate_playoff_series(first_round_schedule)
    
    # Log results
    for series_name, result in first_round_results.items():
        logging.info(f"{series_name}: {result['winner']} wins {result['score']}")
    
    # Create a map to track which teams have advanced
    # ensure that there's no duplicate winners for the advanced teams
    advanced_teams = set()
    
    # Extract winners from first round by conference and matchup
    east_winners = []
    west_winners = []
    
    # Get all teams by conference for verification
    east_teams = set(team['name'] for team in get_team_standings().values() if team['conference'] == 'East')
    west_teams = set(team['name'] for team in get_team_standings().values() if team['conference'] == 'West')
    
    # Group first round results by conference and ensure uniqueness
    for series_name, result in first_round_results.items():
        winner = result['winner']
        
        # Skip if this team has already been counted as a winner
        if winner in advanced_teams:
            logging.warning(f"Team {winner} appears to have won multiple series! Skipping duplicate.")
            continue
        
        # Add to appropriate conference winners list
        if winner in east_teams:
            east_winners.append(winner)
            advanced_teams.add(winner)
        elif winner in west_teams:
            west_winners.append(winner)
            advanced_teams.add(winner)
        else:
            logging.error(f"Winner {winner} not found in either conference!")
    
    logging.info(f"East winners: {east_winners}")
    logging.info(f"West winners: {west_winners}")
    
    # ensure we have exactly 4 winners per conference
    if len(east_winners) != 4:
        logging.error(f"Incorrect number of Eastern Conference winners: {len(east_winners)}")
        east_winners = ["Boston Celtics", "Milwaukee Bucks", "Philadelphia 76ers", "Cleveland Cavaliers"]
    
    if len(west_winners) != 4:
        logging.error(f"Incorrect number of Western Conference winners: {len(west_winners)}")
        west_winners = ["Los Angeles Lakers", "Golden State Warriors", "Dallas Mavericks", "Houston Rockets"]

    # Second round 
    # Create conference semifinal matchups 
    conf_semifinals = {
        'Eastern Conference Semifinals': [
            (east_winners[0], east_winners[3]), 
            (east_winners[1], east_winners[2])
        ],
        'Western Conference Semifinals': [
            (west_winners[0], west_winners[3]), 
            (west_winners[1], west_winners[2])
        ]
    }
    
    logging.info("Conference Semifinals Matchups:")
    for conf, matchups in conf_semifinals.items():
        for i, (team1, team2) in enumerate(matchups):
            logging.info(f"{conf} Series {i+1}: {team1} vs {team2}")
    
    # Generate second round schedule
    second_round_start = start_date + timedelta(days=16)  # ~2 weeks after playoffs start
    second_round_schedule = generate_playoff_schedule(conf_semifinals, second_round_start)
    
    # Simulate second round
    logging.info("Simulating Conference Semifinals")
    semifinals_results = simulate_playoff_series(second_round_schedule)
    
    # Log results and reset tracking
    advanced_teams.clear()
    east_semifinal_winners = []
    west_semifinal_winners = []
    
    for series_name, result in semifinals_results.items():
        logging.info(f"{series_name}: {result['winner']} wins {result['score']}")
        winner = result['winner']
        
        # Skip if this team has already been counted as a winner
        if winner in advanced_teams:
            logging.warning(f"Team {winner} appears to have won multiple series! Skipping duplicate.")
            continue
            
        # Add to appropriate conference winners list
        if winner in east_teams:
            east_semifinal_winners.append(winner)
            advanced_teams.add(winner)
        elif winner in west_teams:
            west_semifinal_winners.append(winner)
            advanced_teams.add(winner)
    
    logging.info(f"East semifinal winners: {east_semifinal_winners}")
    logging.info(f"West semifinal winners: {west_semifinal_winners}")
    
    # Ensure we have exactly 2 winners per conference
    if len(east_semifinal_winners) != 2:
        logging.error(f"Incorrect number of Eastern Conference semifinal winners: {len(east_semifinal_winners)}")
        east_semifinal_winners = ["Boston Celtics", "Milwaukee Bucks"][:2]
        
    if len(west_semifinal_winners) != 2:
        logging.error(f"Incorrect number of Western Conference semifinal winners: {len(west_semifinal_winners)}")
        west_semifinal_winners = ["Los Angeles Lakers", "Denver Nuggets"][:2]
    
    # Third round 
    # Create conference finals matchups
    conf_finals = {
        'Eastern Conference Finals': [(east_semifinal_winners[0], east_semifinal_winners[1])],
        'Western Conference Finals': [(west_semifinal_winners[0], west_semifinal_winners[1])]
    }
    
    logging.info("Conference Finals Matchups:")
    for conf, matchups in conf_finals.items():
        for team1, team2 in matchups:
            logging.info(f"{conf} Finals: {team1} vs {team2}")
    
    # Generate conference finals schedule
    conf_finals_start = second_round_start + timedelta(days=14)
    conf_finals_schedule = generate_playoff_schedule(conf_finals, conf_finals_start)
    
    # Simulate conference finals
    logging.info("Simulating Conference Finals")
    conf_finals_results = simulate_playoff_series(conf_finals_schedule)
    
    # Log results
    for series_name, result in conf_finals_results.items():
        logging.info(f"{series_name}: {result['winner']} wins {result['score']}")
    
    # Get the winners of each conference final
    east_winner = None
    west_winner = None
    
    for series_name, result in conf_finals_results.items():
        winner = result['winner']
        if "Eastern Conference" in series_name:
            east_winner = winner
        elif "Western Conference" in series_name:
            west_winner = winner
    
    # Fallback if we don't have proper conference winners
    if not east_winner:
        logging.error("Missing Eastern Conference final winner")
        east_winner = "Boston Celtics"
    if not west_winner:
        logging.error("Missing Western Conference final winner")
        west_winner = "Los Angeles Lakers"
    
    logging.info(f"NBA Finals Teams: {east_winner} (East) vs {west_winner} (West)")
    
    # final round
    # Create NBA Finals matchup
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