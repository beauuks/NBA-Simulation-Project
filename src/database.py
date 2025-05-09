import sqlite3
import logging
from datetime import datetime

# Database functions
def init_database():
    """Initialize SQLite db and create tables (game, player, stadium operations)"""
    try:
        conn = sqlite3.connect('nba_simulation.db')
        cursor = conn.cursor()

        # games table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS games (
            id TEXT PRIMARY KEY,
            team1 TEXT,
            team2 TEXT,
            score1 INTEGER,
            score2 INTEGER,
            winner TEXT,
            arena TEXT,
            game_date TEXT
        )
        ''')
        
        # player stats table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS player_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT,
            player_name TEXT,
            team TEXT,
            points INTEGER,
            two_pt INTEGER,
            three_pt INTEGER,
            free_throws INTEGER,
            turnovers INTEGER,
            rebounds INTEGER,
            assists INTEGER,
            steals INTEGER,
            blocks INTEGER,
            FOREIGN KEY (game_id) REFERENCES games (id)
        )
        ''')
        
        # playoffs table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS playoffs_games (
            id TEXT PRIMARY KEY,
            team1 TEXT,
            team2 TEXT,
            score1 INTEGER,
            score2 INTEGER,
            winner TEXT,
            arena TEXT,
            game_date TEXT,
            series TEXT,
            game_number INTEGER,
            conference TEXT,
            round TEXT
        )
        ''')

        # playoffs series table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS playoffs_series (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            series_name TEXT,
            team1 TEXT,
            team2 TEXT,
            team1_wins INTEGER,
            team2_wins INTEGER,
            winner TEXT,
            conference TEXT,
            round TEXT,
            year INTEGER
        )
        ''')

        # stadium operations table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS stadium_ops (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id TEXT,
            arena TEXT,
            operation_type TEXT,
            processed_count INTEGER,
            details TEXT,
            FOREIGN KEY (game_id) REFERENCES games (id)
        )
        ''')
        
        conn.commit()
        conn.close()
        logging.info("Database initialized successfully")

    except sqlite3.Error as e:
        logging.error(f"Database initialization failed: {e}")
        raise

def save_game_to_db(game_id, result):
    """Save game results to database"""
    try:
        with sqlite3.connect('nba_simulation.db') as conn:
            cursor = conn.cursor()
            
            conn.execute('BEGIN')

            # Insert game result
            cursor.execute(
                "INSERT OR REPLACE INTO games VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (str(game_id), str(result['team1']), str(result['team2']), 
                int(result['score1']), int(result['score2']), str(result['winner']),
                str(result.get('arena', 'Unknown Arena')), 
                str(result.get('date', datetime.now().strftime('%Y-%m-%d'))))
            )
            
            # insert player stats
            if 'player_stats' in result:
                for player, stats in result['player_stats'].items():
                    cursor.execute(
                        "INSERT INTO player_stats (game_id, player_name, team, points, two_pt, three_pt, free_throws, turnovers, rebounds, assists, steals, blocks) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (str(game_id), str(player), str(stats['team']), int(stats['points']), int(stats['two_pt']), int(stats['three_pt']), int(stats['free_throws']), 
                        int(stats['turnovers']), int(stats['rebounds']), int(stats['assists']), int(stats['steals']), int(stats['blocks']))
                    )

    except sqlite3.Error as e:
        logging.error(f"Database error while saving game: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred while saving game to database: {e}")
        raise

def save_playoffs_game_to_db(game_id, result):
    """Save playoff game results to database with series information"""
    try:
        with sqlite3.connect('nba_simulation.db') as conn:
            cursor = conn.cursor()
            
            conn.execute('BEGIN')

            # Extract series information
            series = result.get('series', '')
            game_number = result.get('game_number', 0)
            
            # Determine conference and round from series name
            conference = 'NBA Finals'
            round_name = 'Finals'
            
            if 'Eastern Conference' in series:
                conference = 'Eastern Conference'
                if 'First Round' in series:
                    round_name = 'First Round'
                elif 'Semifinals' in series:
                    round_name = 'Semifinals'
                else:
                    round_name = 'Conference Finals'
            elif 'Western Conference' in series:
                conference = 'Western Conference'
                if 'First Round' in series:
                    round_name = 'First Round'
                elif 'Semifinals' in series:
                    round_name = 'Semifinals'
                else:
                    round_name = 'Conference Finals'

            # Insert playoff game result
            cursor.execute(
                "INSERT OR REPLACE INTO playoffs_games VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    str(game_id), 
                    str(result['team1']), 
                    str(result['team2']), 
                    int(result['score1']), 
                    int(result['score2']), 
                    str(result['winner']),
                    str(result.get('arena', 'Unknown Arena')), 
                    str(result.get('date', datetime.now().strftime('%Y-%m-%d'))),
                    str(series),
                    int(game_number),
                    str(conference),
                    str(round_name)
                )
            )
            
            # Update or insert series information
            cursor.execute(
                "SELECT id, team1_wins, team2_wins FROM playoffs_series WHERE series_name = ? AND year = ?",
                (series, datetime.now().year)
            )
            existing_series = cursor.fetchone()
            
            team1_wins = 0
            team2_wins = 0
            
            # Count wins in this series
            cursor.execute(
                "SELECT winner, COUNT(*) FROM playoffs_games WHERE series = ? GROUP BY winner",
                (series,)
            )
            for winner, count in cursor.fetchall():
                if winner == result['team1']:
                    team1_wins = count
                elif winner == result['team2']:
                    team2_wins = count
            
            # Determine winner if applicable
            series_winner = None
            if team1_wins >= 4:
                series_winner = result['team1']
            elif team2_wins >= 4:
                series_winner = result['team2']
            
            # Update or insert series record
            if existing_series:
                cursor.execute(
                    "UPDATE playoffs_series SET team1_wins = ?, team2_wins = ?, winner = ? WHERE id = ?",
                    (team1_wins, team2_wins, series_winner, existing_series[0])
                )
            else:
                cursor.execute(
                    "INSERT INTO playoffs_series (series_name, team1, team2, team1_wins, team2_wins, winner, conference, round, year) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        series, 
                        result['team1'], 
                        result['team2'], 
                        team1_wins, 
                        team2_wins, 
                        series_winner,
                        conference,
                        round_name,
                        datetime.now().year
                    )
                )
            
            # Insert player stats
            if 'player_stats' in result:
                for player, stats in result['player_stats'].items():
                    cursor.execute(
                        "INSERT INTO player_stats (game_id, player_name, team, points, two_pt, three_pt, free_throws, turnovers, rebounds, assists, steals, blocks) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                        (str(game_id), str(player), str(stats['team']), int(stats['points']), int(stats['two_pt']), int(stats['three_pt']), int(stats['free_throws']), 
                        int(stats['turnovers']), int(stats['rebounds']), int(stats['assists']), int(stats['steals']), int(stats['blocks']))
                    )

            conn.commit()

    except sqlite3.Error as e:
        logging.error(f"Database error while saving playoff game: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred while saving playoff game to database: {e}")
        raise


def save_stadium_ops_to_db(game_id, arena, operation_type, processed_count, details=None):
    """Save stadium operations data to database"""
    try:
        with sqlite3.connect('nba_simulation.db') as conn: 
            cursor = conn.cursor()
            
            cursor.execute(
                "INSERT INTO stadium_ops (game_id, arena, operation_type, processed_count, details) VALUES (?, ?, ?, ?, ?)",
                (game_id, arena, operation_type, processed_count, details or "")
            )

    except sqlite3.Error as e:
        logging.error(f"Database error while saving stadium operations: {e}")
        raise
    except Exception as e:
        logging.error(f"An unexpected error occurred while saving stadium operations to database: {e}")
        raise

def generate_stats_report():
    """Generate a report of game stats from the database"""
    try:
        # Create logs directory if it doesn't exist
        logs_dir = 'logs'
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir)
        
        # Create a file handler for the stats report
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = os.path.join(logs_dir, f"regular_season_stats_{timestamp}.log")
        file_handler = logging.FileHandler(log_filename)
        file_handler.setLevel(logging.INFO)
        
        # Configure formatter
        formatter = logging.Formatter('%(message)s')
        file_handler.setFormatter(formatter)
        
        # Get the root logger and add the file handler
        root_logger = logging.getLogger()
        root_logger.addHandler(file_handler)
        
        # Store all log messages to display both to console and file
        report_messages = []
        report_messages.append("\n===== NBA SIMULATION STATS REPORT =====")
        report_messages.append(f"Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        with sqlite3.connect('nba_simulation.db') as conn:
            cursor = conn.cursor()
            
            # Get top scoring teams
            cursor.execute('''
            SELECT team1, SUM(score1) as points
            FROM games
            GROUP BY team1
            ORDER BY points DESC
            LIMIT 5
            ''')
            top_teams = cursor.fetchall()
            
            report_messages.append("\nTOP SCORING TEAMS:")
            for i, (team, points) in enumerate(top_teams, 1):
                report_messages.append(f"{i}. {team}: {points} points")
            
            # Get top scoring players
            cursor.execute('''
            SELECT player_name, SUM(points) as total_points
            FROM player_stats
            GROUP BY player_name
            ORDER BY total_points DESC
            LIMIT 10
            ''')
            top_players = cursor.fetchall()
            
            report_messages.append("\nTOP SCORING PLAYERS:")
            for i, (player, points) in enumerate(top_players, 1):
                report_messages.append(f"{i}. {player}: {points} points")
            
            # Get stadium operation stats
            cursor.execute('''
            SELECT operation_type, AVG(processed_count) as avg_count
            FROM stadium_ops
            GROUP BY operation_type
            ''')
            ops_stats = cursor.fetchall()
            
            report_messages.append("\nSTADIUM OPERATIONS AVERAGES:")
            for op_type, avg in ops_stats:
                report_messages.append(f"{op_type.capitalize()}: {avg:.1f} average processed")
            
            report_messages.append("\n======================================")
        
        # Log all messages to both the main log and the file
        for message in report_messages:
            logging.info(message)
        
        # Remove the file handler after logging
        root_logger.removeHandler(file_handler)
        file_handler.close()
        
        logging.info(f"Regular season stats report saved to {log_filename}")

    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

def generate_playoffs_report():
    """Generate a report of playoff stats from the database"""
    try:
        # Create logs directory if it doesn't exist
        logs_dir = 'logs'
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir)
        
        # Create a file handler for the playoffs stats log
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = os.path.join(logs_dir, f"playoffs_stats_{timestamp}.log")
        file_handler = logging.FileHandler(log_filename)
        file_handler.setLevel(logging.INFO)
        
        # Configure formatter
        formatter = logging.Formatter('%(message)s')
        file_handler.setFormatter(formatter)
        
        # Get the root logger and add the file handler
        root_logger = logging.getLogger()
        root_logger.addHandler(file_handler)
        
        # Store all log messages to display both to console and file
        report_messages = []
        report_messages.append("\n===== 🏆 NBA PLAYOFFS REPORT 🏆 =====")
        report_messages.append(f"Report generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        with sqlite3.connect('nba_simulation.db') as conn:
            cursor = conn.cursor()
            
            # Get all playoff series
            cursor.execute('''
            SELECT series_name, team1, team2, team1_wins, team2_wins, winner, conference, round
            FROM playoffs_series
            WHERE year = ?
            ORDER BY 
                CASE round
                    WHEN 'First Round' THEN 1
                    WHEN 'Semifinals' THEN 2
                    WHEN 'Conference Finals' THEN 3
                    WHEN 'Finals' THEN 4
                END,
                conference
            ''', (datetime.now().year,))
            
            series = cursor.fetchall()
            
            # Get champion info if available
            cursor.execute('''
            SELECT winner FROM playoffs_series 
            WHERE round = 'Finals' AND winner IS NOT NULL AND year = ?
            ''', (datetime.now().year,))
            
            champion = cursor.fetchone()
            
            if champion:
                report_messages.append(f"\nNBA CHAMPION: {champion[0]}")
            
            report_messages.append("\nPLAYOFF SERIES RESULTS:")
            current_round = None
            current_conference = None
            
            for s_name, t1, t2, t1_wins, t2_wins, winner, conf, round_name in series:
                if round_name != current_round:
                    report_messages.append(f"\n{round_name}:")
                    current_round = round_name
                    current_conference = None
                
                if conf != current_conference:
                    report_messages.append(f"\n  {conf}:")
                    current_conference = conf
                
                status = f"{t1_wins}-{t2_wins}"
                if winner:
                    status += f" ({winner} wins)"
                else:
                    status += " (In progress)"
                
                report_messages.append(f"    {t1} vs {t2}: {status}")
            
            # Get top playoff scorers
            cursor.execute('''
            SELECT ps.player_name, SUM(ps.points) as total_points
            FROM player_stats ps
            JOIN playoffs_games pg ON ps.game_id = pg.id
            GROUP BY ps.player_name
            ORDER BY total_points DESC
            LIMIT 10
            ''')
            top_scorers = cursor.fetchall()
            
            report_messages.append("\nPLAYOFF TOP SCORERS:")
            for i, (player, points) in enumerate(top_scorers, 1):
                report_messages.append(f"{i}. {player}: {points} points")
            
            # Get high-scoring playoff games
            cursor.execute('''
            SELECT team1, team2, score1, score2, winner, game_date
            FROM playoffs_games
            ORDER BY (score1 + score2) DESC
            LIMIT 5
            ''')
            high_scoring_games = cursor.fetchall()
            
            report_messages.append("\nHIGHEST SCORING PLAYOFF GAMES:")
            for i, (team1, team2, score1, score2, winner, date) in enumerate(high_scoring_games, 1):
                total_score = score1 + score2
                report_messages.append(f"{i}. {team1} {score1} - {team2} {score2} ({total_score} pts total) on {date}, Winner: {winner}")
            
            report_messages.append("\n===================================")
            
        # Log all messages to both the main log and the file
        for message in report_messages:
            logging.info(message)
        
        # Remove the file handler after logging
        root_logger.removeHandler(file_handler)
        file_handler.close()
        
        logging.info(f"Playoffs stats report saved to {log_filename}")

    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")