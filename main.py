
import logging
from src.globals import game_results, game_lock, stats_queue, playoff_results, playoff_bracket, NBA_TEAMS, NBA_PLAYERS
from src.database import init_database, generate_stats_report, generate_playoffs_report
from src.nba_classes import NBA_Game, Player
from src.stadium_ops import StadiumOperation
from src.regular_season import generate_nba_schedule, simulate_conferences
from src.playoffs import simulate_playoffs


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(message)s',
    handlers=[
        logging.FileHandler("nba_simulation.log"),
        logging.StreamHandler()
    ]
)


def main():
    """Main function to run the NBA season simulation"""
    init_database()

    # regular season
    logging.info("Starting NBA regular season simulation")
    eastern_games, western_games = generate_nba_schedule(num_games=3)
    
    simulate_conferences(eastern_games, western_games)
    generate_stats_report()
    
    logging.info("\n" + "=" * 60)
    logging.info("Starting NBA Playoffs Simulation")
    
    all_results = simulate_playoffs()
    generate_playoffs_report()

if __name__ == "__main__":
    main()