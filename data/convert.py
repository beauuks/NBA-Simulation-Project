import pandas as pd
import json

# Read CSV file
df = pd.read_csv("data/player_stats.csv")  # Replace with your actual CSV file path

# Convert DataFrame to dictionary with Player names as keys
players = df.set_index("Player").to_dict(orient="index")

# Convert to JSON string with special characters properly displayed
json_output = json.dumps(players, indent=4, ensure_ascii=False)

# Write to a new JSON file
with open("data/player_stats.json", "w", encoding="utf-8") as json_file:
    json_file.write(json_output)

print("JSON file has been created: player_stats.json")
