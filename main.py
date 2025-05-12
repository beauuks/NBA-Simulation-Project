
import logging
from src.database import init_database, generate_stats_report, generate_playoffs_report
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
    eastern_games, western_games = generate_nba_schedule(num_games=10)
    
    simulate_conferences(eastern_games, western_games)
    generate_stats_report()
    
    logging.info("\n" + "=" * 60)
    logging.info("Starting NBA Playoffs Simulation")
    
    all_results = simulate_playoffs()
    generate_playoffs_report()

if __name__ == "__main__":
    main()