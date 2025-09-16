# Import libraries 
import requests
import pandas as pd
from datetime import datetime
import os
from dotenv import load_dotenv

# Define API details
load_dotenv()

X_API_KEY = os.getenv('X_API_KEY')
url = "https://orchestrator.pgatour.com/graphql"
headers = {
    "x-api-key": X_API_KEY
}

# Data to scrape
stats_to_fetch = {
    "120": "avg_score",
    "138": "top_10",
    "101": "drive_dist",
    "102": "drive_acc",
    "119": "putts_per_round",
    "130": "scramble",
    "103": "gir",
    "160": "bounce_back",
    "02675": "strokes_gained",
    "02567": "strokes_gained_ot",
    "02569": "strokes_gained_arg",
    "02674": "strokes_gained_ttg",
    "02568": "strokes_gained_atg",
    "02564": "strokes_gained_putt",
    "142": "par3_score",
    "143": "par4_score",
    "144": "par5_score"
}

# Function to scrape multiple stats and years
def fetch_player_stat(stat_id, year):
    payload = {
        "operationName": "StatDetails",
        "variables": {
            "tourCode": "R",
            "statId": stat_id,
            "year": year,
            "eventQuery": None
        },
        "query": """
            query StatDetails($tourCode: TourCode!, $statId: String!, $year: Int, $eventQuery: StatDetailEventQuery) {
            statDetails(
                tourCode: $tourCode
                statId: $statId
                year: $year
                eventQuery: $eventQuery
            ) {
                rows {
                ... on StatDetailsPlayer {
                    playerId
                    playerName
                    country
                    rank
                    stats {
                        statName
                        statValue
                    }
                }
                }
            }
            }
        """
    }   

    response = requests.post(url, json=payload, headers=headers)
    if response.status_code != 200:
        print(f"API error for {stat_id} ({year}): {response.status_code}")
        return []

    data = response.json()
    rows = data.get('data', {}).get('statDetails', {}).get('rows', [])
    
    player_stats = []
    for row in rows:
        if 'playerName' in row:
            # Keep raw values as-is
            for s in row['stats']:
                player_stats.append({
                    'player_id': row['playerId'],
                    'player_name': row['playerName'],
                    'country': row.get('country'),
                    'year': year,
                    'rank': row.get('rank'),
                    'stat_id': stat_id,
                    'stat_name': stats_to_fetch.get(stat_id, f"stat_{stat_id}"),
                    'stat_value': s['statValue'],  # Keep raw string
                    'scraped_at': datetime.now()
                })
    
    print(f"Fetched {len(player_stats)} rows for {stat_id} ({year})")
    return player_stats

# Define years to scrape
start_year = datetime.now().year - 20
end_year = datetime.now().year

# To store all data
all_stats = []

# Nested Loop through years and stat's
for year in range(start_year, end_year + 1):
    for stat_id in stats_to_fetch.keys():
        all_stats.extend(fetch_player_stat(stat_id, year))

print(f'All data scraped between {start_year} and {end_year}')

df = pd.DataFrame(all_stats)

# Convert to csv
df.to_csv('data/golf_data.csv', index = False)
