import sqlite3
import logging
import datetime
import json

# Database functions
def init_database():
    conn = sqlite3.connect('nba_simulation.db')
    cursor = conn.cursor()

    # games table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS games (
        id INTEGER PRIMARY KEY,
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
        game_id INTEGER,
        player_name TEXT,
        team TEXT,
        points INTEGER,
        `2pt` INTEGER,
        `3pt` INTEGER,
        free_throws INTEGER,
        turnovers INTEGER,
        rebounds INTEGER,
        assists INTEGER,
        steals INTEGER,
        blocks INTEGER,
        FOREIGN KEY (game_id) REFERENCES games (id)
    )
    ''')
    
    # stadium operations table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS stadium_ops (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game_id INTEGER,
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

def save_game_to_db(game_id, result):
    """Save game results to database"""
    conn = sqlite3.connect('nba_simulation.db')
    cursor = conn.cursor()
    
    # TODO: CHeck if it inserts the real date of the game (use the real schedule of the NBA season 2023-2024)
    # Insert game result
    cursor.execute(
        "INSERT OR REPLACE INTO games VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (game_id, result['team1'], result['team2'], 
         result['score1'], result['score2'], result['winner'],
         result.get('arena', 'Unknown Arena'), 
         result.get('date', datetime.now().strftime('%Y-%m-%d')))
    )
    
    # Insert player stats if available
    if 'player_stats' in result:
        for player, stats in result['player_stats'].items():
            cursor.execute(
                "INSERT INTO player_stats (game_id, player_name, team, points, `2pt`, `3pt`, free_throws, turnovers, rebounds, assists, steals, blocks) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (game_id, player, stats['team'], stats['points'], stats['2pt'], stats['3pt'], stats['free_throws'], 
                 stats['turnovers'], stats['rebounds'], stats['assists'], stats['steals'], stats['blocks'])
            )
    
    conn.commit()
    conn.close()

def save_stadium_ops_to_db(game_id, arena, operation_type, processed_count, details=None):
    """Save stadium operations data to database"""
    conn = sqlite3.connect('nba_simulation.db')
    cursor = conn.cursor()
    
    cursor.execute(
        "INSERT INTO stadium_ops (game_id, arena, operation_type, processed_count, details) VALUES (?, ?, ?, ?, ?)",
        (game_id, arena, operation_type, processed_count, details or "")
    )
    
    conn.commit()
    conn.close()

def generate_stats_report():
    """Generate a report of game stats from the database"""
    try:
        conn = sqlite3.connect('nba_simulation.db')
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
        
        # Get top scoring players
        cursor.execute('''
        SELECT player_name, SUM(points) as total_points
        FROM player_stats
        GROUP BY player_name
        ORDER BY total_points DESC
        LIMIT 10
        ''')
        top_players = cursor.fetchall()
        
        # Get stadium operation stats
        cursor.execute('''
        SELECT operation_type, AVG(processed_count) as avg_count
        FROM stadium_ops
        GROUP BY operation_type
        ''')
        ops_stats = cursor.fetchall()
        
        conn.close()
        
        # Log the report
        logging.info("\n===== NBA SIMULATION STATS REPORT =====")
        
        logging.info("\nTOP SCORING TEAMS:")
        for i, (team, points) in enumerate(top_teams, 1):
            logging.info(f"{i}. {team}: {points} points")
        
        logging.info("\nTOP SCORING PLAYERS:")
        for i, (player, points) in enumerate(top_players, 1):
            logging.info(f"{i}. {player}: {points} points")
        
        logging.info("\nSTADIUM OPERATIONS AVERAGES:")
        for op_type, avg in ops_stats:
            logging.info(f"{op_type.capitalize()}: {avg:.1f} average processed")
        
        logging.info("\n======================================")

    except sqlite3.Error as e:
        logging.error(f"Database error: {e}")
    except Exception as e:
        logging.error(f"An error occurred: {e}")