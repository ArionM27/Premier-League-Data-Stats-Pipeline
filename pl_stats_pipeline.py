##Arkadiuz Mercado
##Premier League Stats Pipeline
##A Batch Data Pipeline that updates Premier League player statistics on a daily basis into a CSV file to later be used in PostgreSQL

import pandas as pd
from bs4 import BeautifulSoup
from data_Cleaning import data_Clean, data_to_postgres
import requests 
import time
import psycopg2

print("Starting Data Extraction!")

all_Stats = []
html = requests.get('https://fbref.com/en/comps/9/Premier-League-Stats').text
soup = BeautifulSoup(html, 'lxml')
table = soup.find_all('table', class_ = 'stats_table')[0] 

## Gathering all the team links to eventually gather player stats for each team
## Workaround for being unable to access all league player stats directly
links = table.find_all('a')

##Finding Link of each team
links = [l.get("href") for l in links]
links = [l for l in links if '/squads/' in l]

##List comprehension to format subpage link into a complete link
team_urls = [f"https://fbref.com{l}" for l in links]

for team in team_urls: 
    ##Gathering Team Data to Connect with Each Player
    team_name = team.split("/")[-1].replace("-Stats", "")
    team_data = requests.get(team).text
    soup = BeautifulSoup(team_data, 'lxml')
    stats = soup.find_all('table', class_ = "stats_table")[0]

    if stats and stats.columns: stats.columns = stats.columns.droplevel()

    # Transforming into a Pandas DataFrame
    data = pd.read_html(str(stats))[0]
    data["Team"]= team_name

    all_Stats.append(data) 

    ##Delaying Time to Prevent FBRef from Blocking Requests
    time.sleep(5)

stat_df = pd.concat(all_Stats)

##Putting it all Together into a CSV for external use (Ex: Excel)
stat_df.to_csv("player_stats.csv") 

##Cleaning up and Transforming data for easier retrieval
print("Cleaning Player Data!")
data_Clean()

##Data to be inserted into a PostgreSQL Database
data_to_postgres()

print("Player Stats successfully Loaded!")
