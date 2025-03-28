import os
import json
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
import numpy as np
import pandas as pd
import requests
from io import BytesIO
import seaborn as sns
from datetime import datetime
from matplotlib.gridspec import GridSpec
import matplotlib.patches as mpatches
from PIL import Image, ImageDraw

# Assuming globals.py has been imported with game_results and playoff_results
from src.globals import game_results, playoff_results, playoff_bracket, NBA_TEAMS

# Create directories for assets and output
os.makedirs('assets/logos', exist_ok=True)
os.makedirs('output', exist_ok=True)

# Dictionary mapping team names to their logo URLs
# You can update these URLs with official NBA team logos
TEAM_LOGOS = {
    "Atlanta Hawks": "https://content.sportslogos.net/logos/6/220/thumbs/22091682016.gif",
    "Boston Celtics": "https://content.sportslogos.net/logos/6/213/thumbs/slhg02hbef3j1ov4lsnwyol5o.gif",
    "Brooklyn Nets": "https://content.sportslogos.net/logos/6/3786/thumbs/hsuff5m3dgiv20kovde422r1f.gif",
    "Charlotte Hornets": "https://content.sportslogos.net/logos/6/5120/thumbs/512019262015.gif",
    "Chicago Bulls": "https://content.sportslogos.net/logos/6/221/thumbs/hj3gmh82w9hffmeh3fjm5h874.gif",
    "Cleveland Cavaliers": "https://content.sportslogos.net/logos/6/222/thumbs/22269212018.gif",
    "Dallas Mavericks": "https://content.sportslogos.net/logos/6/228/thumbs/22834632018.gif",
    "Denver Nuggets": "https://content.sportslogos.net/logos/6/229/thumbs/22989262019.gif",
    "Detroit Pistons": "https://content.sportslogos.net/logos/6/223/thumbs/22321642018.gif",
    "Golden State Warriors": "https://content.sportslogos.net/logos/6/235/thumbs/23531522020.gif",
    "Houston Rockets": "https://content.sportslogos.net/logos/6/230/thumbs/23068322020.gif",
    "Indiana Pacers": "https://content.sportslogos.net/logos/6/224/thumbs/22448122018.gif",
    "Los Angeles Clippers": "https://content.sportslogos.net/logos/6/236/thumbs/23637762019.gif",
    "Los Angeles Lakers": "https://content.sportslogos.net/logos/6/237/thumbs/uig7aiht8jnpl1szbi57zzlsh.gif",
    "Memphis Grizzlies": "https://content.sportslogos.net/logos/6/231/thumbs/23143732019.gif",
    "Miami Heat": "https://content.sportslogos.net/logos/6/214/thumbs/burm5gh2wvjti3xhei5h16k8e.gif",
    "Milwaukee Bucks": "https://content.sportslogos.net/logos/6/225/thumbs/22582752016.gif",
    "Minnesota Timberwolves": "https://content.sportslogos.net/logos/6/232/thumbs/23296692018.gif",
    "New Orleans Pelicans": "https://content.sportslogos.net/logos/6/4962/thumbs/496226812014.gif",
    "New York Knicks": "https://content.sportslogos.net/logos/6/216/thumbs/2nn48xofg0hms8k326cqdmuis.gif",
    "Oklahoma City Thunder": "https://content.sportslogos.net/logos/6/2687/thumbs/khmovcnezy06c3nm05ccn0oj2.gif",
    "Orlando Magic": "https://content.sportslogos.net/logos/6/217/thumbs/wd9ic7qafgfb0ffsvoimynyy3.gif",
    "Philadelphia 76ers": "https://content.sportslogos.net/logos/6/218/thumbs/21870342016.gif",
    "Phoenix Suns": "https://content.sportslogos.net/logos/6/238/thumbs/23843702014.gif",
    "Portland Trail Blazers": "https://content.sportslogos.net/logos/6/239/thumbs/23997252018.gif",
    "Sacramento Kings": "https://content.sportslogos.net/logos/6/240/thumbs/24040432017.gif",
    "San Antonio Spurs": "https://content.sportslogos.net/logos/6/233/thumbs/23325472018.gif",
    "Toronto Raptors": "https://content.sportslogos.net/logos/6/227/thumbs/22745782016.gif",
    "Utah Jazz": "https://content.sportslogos.net/logos/6/234/thumbs/23467492017.gif",
    "Washington Wizards": "https://content.sportslogos.net/logos/6/219/thumbs/21956712016.gif"
}

def download_team_logos():
    """Download all team logos and save them locally"""
    print("Downloading team logos...")
    for team_name, logo_url in TEAM_LOGOS.items():
        try:
            response = requests.get(logo_url)
            if response.status_code == 200:
                file_path = f'assets/logos/{team_name.replace(" ", "_")}.png'
                with open(file_path, 'wb') as f:
                    f.write(response.content)
                print(f"Downloaded logo for {team_name}")
            else:
                print(f"Failed to download logo for {team_name}: HTTP {response.status_code}")
        except Exception as e:
            print(f"Error downloading logo for {team_name}: {e}")

def get_logo_path(team_name):
    """Get local path to team logo"""
    return f'assets/logos/{team_name.replace(" ", "_")}.png'

def create_team_records_visualization():
    """Create visualization of team records for regular season"""
    print("Generating team records visualization...")
    
    # Get team records from game_results
    teams_with_records = []
    for game in game_results.values():
        if game.get('team1') and game.get('winner'):
            winner = game.get('winner')
            loser = game.get('team2') if winner == game.get('team1') else game.get('team1')

            team1_exists = False
            team2_exists = False

            for team_record in teams_with_records:
                if team_record['name'] == game.get('team1'):
                    team1_exists = True
                    if winner == game.get('team1'):
                        team_record['wins'] += 1
                    else:
                        team_record['losses'] += 1
                if team_record['name'] == game.get('team2'):
                    team2_exists = True
                    if winner == game.get('team2'):
                        team_record['wins'] += 1
                    else:
                        team_record['losses'] += 1

            if not team1_exists:
                if winner == game.get('team1'):
                    teams_with_records.append({'name': game.get('team1'), 'wins': 1, 'losses': 0})
                else:
                    teams_with_records.append({'name': game.get('team1'), 'wins': 0, 'losses': 1})

            if not team2_exists:
                if winner == game.get('team2'):
                    teams_with_records.append({'name': game.get('team2'), 'wins': 1, 'losses': 0})
                else:
                    teams_with_records.append({'name': game.get('team2'), 'wins': 0, 'losses': 1})
    
    # Separate East and West teams
    east_teams = []
    west_teams = []
    
    for team in teams_with_records:
        team_name = team['name']
        if team_name in NBA_TEAMS:
            conference = NBA_TEAMS[team_name]['conference']
            win_pct = team['wins'] / (team['wins'] + team['losses'])
            
            team_data = {
                'name': team_name,
                'wins': team['wins'],
                'losses': team['losses'],
                'win_pct': win_pct
            }
            
            if conference == 'East':
                east_teams.append(team_data)
            elif conference == 'West':
                west_teams.append(team_data)
    
    # Sort teams by win percentage
    east_teams = sorted(east_teams, key=lambda x: x['win_pct'], reverse=True)
    west_teams = sorted(west_teams, key=lambda x: x['win_pct'], reverse=True)
    
    # Create visualization
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 12))
    plt.subplots_adjust(wspace=0.3)
    
    # Set background color
    fig.patch.set_facecolor('#f8f9fa')
    ax1.set_facecolor('#f8f9fa')
    ax2.set_facecolor('#f8f9fa')
    
    # Helper function to create conference standings
    def plot_conference_standings(ax, teams, title):
        team_names = [team['name'] for team in teams]
        wins = [team['wins'] for team in teams]
        losses = [team['losses'] for team in teams]
        
        # Create DataFrame for easy plotting
        df = pd.DataFrame({
            'Team': team_names,
            'Wins': wins,
            'Losses': losses,
            'Win %': [round(w / (w + l) * 100, 1) for w, l in zip(wins, losses)]
        })
        
        # Colors for playoff and non-playoff teams
        colors = ['#3CB371' if i < 8 else '#A9A9A9' for i in range(len(teams))]
        
        # Plot stacked bar chart
        bars = ax.barh(df['Team'], df['Wins'], color='#1f77b4', height=0.6, label='Wins')
        ax.barh(df['Team'], df['Losses'], left=df['Wins'], color='#ff7f0e', height=0.6, label='Losses')
        
        # Add team logos
        for i, team in enumerate(teams):
            try:
                logo = mpimg.imread(get_logo_path(team['name']))
                logo_box = mpatches.FancyBboxPatch(
                    (0, i - 0.3), 0.8, 0.6, 
                    boxstyle=mpatches.BoxStyle("Round", pad=0.02),
                    facecolor='white', edgecolor='white', alpha=0
                )
                ax.add_patch(logo_box)
                
                imagebox = mpatches.OffsetBox(
                    offsetbox=mpatches.AnnotationBbox(
                        mpatches.OffsetImage(logo, zoom=0.1), 
                        (0, i), 
                        bboxprops=dict(facecolor='white', edgecolor='none', alpha=0)
                    ),
                    offset=(-25, 0)
                )
                ax.add_artist(imagebox)
            except FileNotFoundError:
                # If logo file not found, skip
                pass
        
        # Add win-loss record and win percentage text
        for i, (w, l, pct) in enumerate(zip(df['Wins'], df['Losses'], df['Win %'])):
            ax.text(w + l + 1, i, f"{w}-{l} ({pct}%)", 
                    ha='left', va='center', fontsize=10)
        
        # Customize plot
        ax.set_title(title, fontsize=16, weight='bold', pad=20)
        ax.set_xlim(0, max(df['Wins'] + df['Losses']) + 15)
        ax.set_xlabel('Games', fontsize=12)
        ax.set_yticks(range(len(team_names)))
        ax.set_yticklabels([])  # Hide team names since we'll use logos
        ax.grid(axis='x', alpha=0.3)
        
        # Add playoff line
        if len(teams) > 8:
            ax.axhline(y=7.5, color='r', linestyle='--', alpha=0.5)
            ax.text(1, 8.2, 'Playoff Cut', fontsize=10, color='r')
        
        # Add team names (with padding for logos)
        for i, team in enumerate(teams):
            ax.text(2, i, team['name'], ha='left', va='center', fontsize=11, weight='bold')
    
    # Plot both conferences
    plot_conference_standings(ax1, east_teams, 'Eastern Conference Standings')
    plot_conference_standings(ax2, west_teams, 'Western Conference Standings')
    
    # Add title and legend
    fig.suptitle('NBA Regular Season Final Standings', fontsize=20, weight='bold', y=0.98)
    fig.text(0.5, 0.01, f'© NBA Simulation Project - Generated {datetime.now().strftime("%Y-%m-%d")}',
             ha='center', fontsize=10, alpha=0.7)
    
    # Add legend
    handles = [
        mpatches.Patch(color='#3CB371', label='Playoff Teams'),
        mpatches.Patch(color='#A9A9A9', label='Non-Playoff Teams'),
        mpatches.Patch(color='#1f77b4', label='Wins'),
        mpatches.Patch(color='#ff7f0e', label='Losses')
    ]
    fig.legend(handles=handles, loc='upper center', ncol=4, bbox_to_anchor=(0.5, 0.07), frameon=False)
    
    # Save figure
    plt.tight_layout(rect=[0, 0.08, 1, 0.95])
    plt.savefig('output/regular_season_standings.png', dpi=300, bbox_inches='tight')
    print("Regular season standings visualization saved to output/regular_season_standings.png")
    plt.close()

def create_playoff_bracket_visualization():
    """Create visualization of playoff bracket"""
    print("Generating playoff bracket visualization...")
    
    # Define dimensions and spacing
    fig_width, fig_height = 16, 12
    fig = plt.figure(figsize=(fig_width, fig_height), facecolor='#f8f9fa')
    
    # Create a blank canvas for drawing
    canvas = Image.new('RGBA', (int(fig_width * 100), int(fig_height * 100)), (248, 249, 250, 255))
    draw = ImageDraw.Draw(canvas)
    
    # Extract playoff results and structure
    east_teams = [series[0]['name'] for series in playoff_bracket['Eastern Conference']['First Round']]
    west_teams = [series[0]['name'] for series in playoff_bracket['Western Conference']['First Round']]
    
    # Function to draw a match box
    def draw_match_box(x, y, width, height, team1, team2, winner=None):
        # Draw border
        draw.rectangle([x, y, x+width, y+height], outline=(0, 0, 0, 255), width=2)
        
        # Draw team boxes
        draw.rectangle([x, y, x+width, y+height/2], fill=(240, 240, 240, 255), outline=(0, 0, 0, 255))
        draw.rectangle([x, y+height/2, x+width, y+height], fill=(240, 240, 240, 255), outline=(0, 0, 0, 255))
        
        # Highlight winner if known
        if winner and team1 == winner:
            draw.rectangle([x+2, y+2, x+width-2, y+height/2-2], fill=(200, 255, 200, 255))
        elif winner and team2 == winner:
            draw.rectangle([x+2, y+height/2+2, x+width-2, y+height-2], fill=(200, 255, 200, 255))
        
        # Get team logos
        try:
            logo1 = Image.open(get_logo_path(team1))
            logo1 = logo1.resize((int(height/4), int(height/4)))
            canvas.paste(logo1, (int(x + 5), int(y + height/8 - height/8)), logo1)
        except (FileNotFoundError, IOError):
            pass
        
        try:
            logo2 = Image.open(get_logo_path(team2))
            logo2 = logo2.resize((int(height/4), int(height/4)))
            canvas.paste(logo2, (int(x + 5), int(y + 5*height/8 - height/8)), logo2)
        except (FileNotFoundError, IOError):
            pass
    
    # Draw the brackets for each round
    # First Round - East
    y_offset = 50
    for i, series in enumerate(playoff_bracket['Eastern Conference']['First Round']):
        team1, team2 = series[0]['name'], series[1]['name']
        winner = None