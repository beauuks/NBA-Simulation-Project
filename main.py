
import logging
from src.globals import game_results, game_lock, stats_queue, playoff_results, playoff_bracket, NBA_TEAMS, NBA_PLAYERS
from src.database import init_database, generate_stats_report
from src.nba_classes import NBA_Game, Player
from src.stadium_ops import StadiumOperation
from src.regular_season import generate_nba_schedule, simulate_parallel_games, simulate_conferences
from src.playoffs import determine_top_conference_teams, simulate_full_playoffs, generate_playoff_summary, create_playoff_bracket, create_realistic_playoff_schedule


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(message)s',
    handlers=[
        logging.FileHandler("nba_simulation.log"),
        logging.StreamHandler()
    ]
)


if __name__ == "__main__":
    init_database()

    # regular season
    logging.info("Starting NBA regular season simulation")
    all_games = generate_nba_schedule(num_games=10)

    # TODO: Split games based on the actual teams in each conference
    # splitting games into eastern and western conferences
    mid_point = len(all_games) // 2
    eastern_games = all_games[:mid_point]
    western_games = all_games[mid_point:]
    
    simulate_conferences(eastern_games, western_games)
    generate_stats_report()

    # get top teams for the playoffs
    east_teams, west_teams = determine_top_conference_teams(NBA_TEAMS)

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