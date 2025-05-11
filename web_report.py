import sqlite3
import json
import os
from datetime import datetime

def load_playoff_data_from_db(db_path='nba_simulation.db'):
    """Load playoff data from the database"""
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check if playoffs_series table exists
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='playoffs_series'")
        if not cursor.fetchone():
            print("Playoffs data not found in database.")
            return None
        
        # Query playoff series data
        cursor.execute("""
            SELECT series_name, team1, team2, winner, conference, round 
            FROM playoffs_series 
            ORDER BY id
        """)
        
        series_data = cursor.fetchall()
        conn.close()
        
        if not series_data:
            print("No playoff series data found.")
            return None
        
        print(f"Found {len(series_data)} playoff series in the database.")
        return series_data
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return None

def organize_playoff_data(series_data):
    """Organize playoff data into a bracket structure"""
    # Initialize bracket structure
    bracket = {
        'Eastern Conference': {
            'First Round': [],
            'Semifinals': [],
            'Finals': []
        },
        'Western Conference': {
            'First Round': [],
            'Semifinals': [],
            'Finals': []
        },
        'NBA Finals': []
    }
    
    # Process series data into bracket structure
    for series in series_data:
        series_name, team1, team2, winner, conference, round_name = series

        if "NBA Finals" in series_name or conference == "NBA Finals" or round_name == "NBA Finals":
            bracket['NBA Finals'].append((team1, team2, winner))
            continue

        # Normalize conference name
        conf_key = None
        if 'Eastern Conference' in conference:
            conf_key = 'Eastern Conference'
        elif 'Western Conference' in conference:
            conf_key = 'Western Conference'
        else:
            print(f"Warning: Unknown conference format: {conference}")
            continue
        
        # Normalize round name and add to the proper section
        if 'First Round' in round_name:
            bracket[conf_key]['First Round'].append((team1, team2, winner))
        elif 'Semifinals' in round_name or 'Conference Semifinals' in round_name:
            bracket[conf_key]['Semifinals'].append((team1, team2, winner))
        elif 'Finals' in round_name or 'Conference Finals' in round_name:
            bracket[conf_key]['Finals'].append((team1, team2, winner))
        else:
            print(f"Warning: Unknown round format: {round_name}")
    
    return bracket

def create_bracket_html(bracket, output_file='nba_playoff_bracket.html'):
    """Create an HTML file with the playoff bracket"""
    # HTML template for the bracket
    html_template = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>NBA Playoffs Bracket</title>
        <style>
            body {{
                font-family: 'Arial', sans-serif;
                background: #111827;
                margin: 0;
                padding: 20px;
                color: #e5e7eb;
            }}
            .container {{
                max-width: 1200px;
                margin: 0 auto;
                background-color: #1f2937;
                border-radius: 10px;
                padding: 20px;
                box-shadow: 0 4px 20px rgba(0, 0, 0, 0.5);
            }}
            h1 {{
                text-align: center;
                color: #f9fafb;
                text-transform: uppercase;
                background: linear-gradient(90deg, #c2410c, #1d4ed8);
                -webkit-background-clip: text;
                -webkit-text-fill-color: transparent;
                padding: 10px 0;
                font-size: 2.5em;
                text-shadow: 0 2px 4px rgba(0, 0, 0, 0.3);
                margin-bottom: 30px;
                border-bottom: 1px solid #374151;
            }}
            .brackets {{
                display: flex;
                justify-content: space-between;
                margin-top: 30px;
            }}
            .conference {{
                width: 48%;
            }}
            .conference-title {{
                text-align: center;
                font-weight: bold;
                margin-bottom: 20px;
                font-size: 1.4em;
                padding: 8px 0;
                border-radius: 6px;
            }}
            .conference.east .conference-title {{
                background-color: #1d4ed8;
                color: white;
            }}
            .conference.west .conference-title {{
                background-color: #c2410c;
                color: white;
            }}
            .finals {{
                text-align: center;
                margin-top: 20px;
            }}
            .finals-title {{
                font-weight: bold;
                margin-bottom: 15px;
                font-size: 1.6em;
                background: linear-gradient(90deg, #f59e0b, #b45309);
                color: white;
                padding: 10px;
                border-radius: 6px;
            }}
            .round {{
                margin-bottom: 30px;
            }}
            .round-title {{
                text-align: center;
                margin-bottom: 15px;
                font-weight: bold;
                color: #d1d5db;
                background-color: #374151;
                padding: 6px;
                border-radius: 4px;
                text-transform: uppercase;
                letter-spacing: 1px;
                font-size: 0.9em;
            }}
            .matchup {{
                border: 1px solid #4b5563;
                border-radius: 5px;
                margin-bottom: 15px;
                padding: 12px;
                background-color: #2d3748;
                box-shadow: 0 2px 5px rgba(0, 0, 0, 0.2);
            }}
            .team {{
                padding: 8px;
                border-radius: 4px;
                margin: 5px 0;
                display: flex;
                justify-content: space-between;
                align-items: center;
            }}
            .team-name {{
                font-weight: normal;
            }}
            .team-score {{
                font-weight: bold;
            }}
            .team.winner {{
                background-color: rgba(74, 222, 128, 0.2);
                font-weight: bold;
            }}
            .vs {{
                text-align: center;
                font-size: 0.8em;
                color: #9ca3af;
                margin: 5px 0;
            }}
            .finals-matchup {{
                border: 2px solid #f59e0b;
                border-radius: 5px;
                padding: 15px;
                margin: 0 auto;
                max-width: 300px;
                background-color: #2d3748;
                box-shadow: 0 4px 10px rgba(0, 0, 0, 0.3);
            }}
            .champion {{
                margin-top: 20px;
                text-align: center;
                font-size: 1.5em;
                font-weight: bold;
                color: #f59e0b;
                text-transform: uppercase;
                letter-spacing: 1px;
                animation: pulse 2s infinite;
                padding: 10px;
                border: 2px solid #f59e0b;
                border-radius: 6px;
                background-color: rgba(245, 158, 11, 0.1);
            }}
            @keyframes pulse {{
                0% {{ box-shadow: 0 0 0 0 rgba(245, 158, 11, 0.4); }}
                70% {{ box-shadow: 0 0 0 10px rgba(245, 158, 11, 0); }}
                100% {{ box-shadow: 0 0 0 0 rgba(245, 158, 11, 0); }}
            }}
            .trophy {{
                font-size: 24px;
                margin-right: 10px;
            }}
            .timestamp {{
                text-align: center;
                margin-top: 30px;
                color: #9ca3af;
                font-size: 0.8em;
                border-top: 1px solid #374151;
                padding-top: 15px;
            }}
            .bracket-container {{
                position: relative;
            }}
            .series-result {{
                font-size: 0.85em;
                color: #9ca3af;
                margin-top: 5px;
                text-align: center;
            }}
        </style>
    </head>
    <body>
        <div class="container">
            <h1>NBA Playoffs Bracket</h1>
            
            <div class="bracket-container">
                <div class="brackets">
                    <div class="conference east">
                        <div class="conference-title">Eastern Conference</div>
                        
                        <div class="round">
                            <div class="round-title">First Round</div>
                            <div id="east-r1-matches">
                                {east_first_round}
                            </div>
                        </div>
                        
                        <div class="round">
                            <div class="round-title">Conference Semifinals</div>
                            <div id="east-semis-matches">
                                {east_semifinals}
                            </div>
                        </div>
                        
                        <div class="round">
                            <div class="round-title">Conference Finals</div>
                            <div id="east-finals-match">
                                {east_finals}
                            </div>
                        </div>
                    </div>
                    
                    <div class="conference west">
                        <div class="conference-title">Western Conference</div>
                        
                        <div class="round">
                            <div class="round-title">First Round</div>
                            <div id="west-r1-matches">
                                {west_first_round}
                            </div>
                        </div>
                        
                        <div class="round">
                            <div class="round-title">Conference Semifinals</div>
                            <div id="west-semis-matches">
                                {west_semifinals}
                            </div>
                        </div>
                        
                        <div class="round">
                            <div class="round-title">Conference Finals</div>
                            <div id="west-finals-match">
                                {west_finals}
                            </div>
                        </div>
                    </div>
                </div>
                
                <div class="finals">
                    <div class="finals-title">NBA Finals</div>
                    <div id="nba-finals-match" class="finals-matchup">
                        {nba_finals}
                    </div>
                    <div id="champion-display" class="champion">
                        {champion}
                    </div>
                </div>
            </div>
            
            <div class="timestamp">
                Generated on {timestamp}
            </div>
        </div>
    </body>
    </html>
    """
    
    # Format team matchup HTML
    def format_matchup(team1, team2, score, winner):
        team1_class = "winner" if team1 == winner else ""
        team2_class = "winner" if team2 == winner else ""
        
        # Extract the scores
        if score:
            team1_wins, team2_wins = score.split('-')
        else:
            team1_wins, team2_wins = "0", "0"
            
        return f"""
        <div class="matchup">
            <div class="team {team1_class}">
                <span class="team-name">{team1}</span>
                <span class="team-score">{team1_wins}</span>
            </div>
            <div class="vs">VS</div>
            <div class="team {team2_class}">
                <span class="team-name">{team2}</span>
                <span class="team-score">{team2_wins}</span>
            </div>
            <div class="series-result">{winner} wins {score}</div>
        </div>
        """
    
    # Helper function to format the matchups for a specific round in a conference
    def format_round_matchups(conference, round_name):
        matchups_html = ""
        # Check if the conference and round exist in the bracket
        if conference in bracket and round_name in bracket[conference]:
            for matchup in bracket[conference][round_name]:
                team1, team2, winner = matchup
                # Calculate the score string
                score = "0-0"  # Default
                for s in series_results:
                    if (s['team1'] == team1 and s['team2'] == team2) or (s['team1'] == team2 and s['team2'] == team1):
                        score = s['score']
                        break
                        
                matchups_html += format_matchup(team1, team2, score, winner)
                
        return matchups_html if matchups_html else "<div class='matchup'>No matchups available</div>"
    
    # Get series results for score display
    conn = sqlite3.connect('nba_simulation.db')
    cursor = conn.cursor()
    cursor.execute('''
    SELECT team1, team2, team1_wins || '-' || team2_wins as score, winner 
    FROM playoffs_series
    ''')
    series_results = [{'team1': t1, 'team2': t2, 'score': score, 'winner': winner} 
                      for t1, t2, score, winner in cursor.fetchall()]
    conn.close()
    
    # Generate HTML for each section
    east_first_round = format_round_matchups("Eastern Conference", "First Round")
    east_semifinals = format_round_matchups("Eastern Conference", "Semifinals")
    east_finals = format_round_matchups("Eastern Conference", "Finals")
    
    west_first_round = format_round_matchups("Western Conference", "First Round")
    west_semifinals = format_round_matchups("Western Conference", "Semifinals")
    west_finals = format_round_matchups("Western Conference", "Finals")
    
    # NBA Finals
    nba_finals_html = ""
    if "NBA Finals" in bracket:
        for matchup in bracket["NBA Finals"]:
            team1, team2, winner = matchup
            # Find the score
            score = "0-0"  # Default
            for s in series_results:
                if (s['team1'] == team1 and s['team2'] == team2) or (s['team1'] == team2 and s['team2'] == team1):
                    score = s['score']
                    break
                    
            nba_finals_html += format_matchup(team1, team2, score, winner)
    
    if not nba_finals_html:
        nba_finals_html = "<div class='matchup'>Finals not yet determined</div>"
    
    # Champion display
    champion_html = ""
    if "NBA Finals" in bracket and bracket["NBA Finals"] and bracket["NBA Finals"][0][2]:
        champion = bracket["NBA Finals"][0][2]
        champion_html = f"<span class='trophy'>🏆</span> {champion}"
    
    # Current timestamp
    current_time = datetime.now().strftime("%B %d, %Y at %H:%M:%S")
    
    # Fill in the template
    html_content = html_template.format(
        east_first_round=east_first_round,
        east_semifinals=east_semifinals,
        east_finals=east_finals,
        west_first_round=west_first_round,
        west_semifinals=west_semifinals,
        west_finals=west_finals,
        nba_finals=nba_finals_html,
        champion=champion_html,
        timestamp=current_time
    )
    
    # Write to file
    with open(output_file, 'w') as f:
        f.write(html_content)
    
    print(f"Bracket HTML saved to {output_file}")
    return output_file

if __name__ == "__main__":
    # Load playoff data from database
    print("Loading playoff data from database...")
    series_data = load_playoff_data_from_db()
    
    if not series_data:
        print("No playoff data found. Please run the simulation first.")
        exit(1)
    
    # Organize data into bracket structure
    print("Organizing data into bracket structure...")
    bracket = organize_playoff_data(series_data)
    
    # Create output directory if it doesn't exist
    output_dir = 'playoffs_report'
    if not os.path.exists(output_dir):
        print(f"Creating output directory: {output_dir}")
        os.makedirs(output_dir)
    
    # Create HTML visualization
    output_path = os.path.join(output_dir, 'index.html')
    html_file = create_bracket_html(bracket, output_file=output_path)
    
    print(f"\nDone!")