import random
from datetime import datetime, timedelta
import logging

from src.nba_classes import NBA_Game
from src.globals import NBA_TEAMS


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
