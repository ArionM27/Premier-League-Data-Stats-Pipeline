import pandas as pd
import numpy as np
import psycopg2

def data_Clean():
    
    df = pd.read_csv("player_stats.csv")

    df.at[0, 'Team'] = "Team"

    ##Dropping Top Row
    df.columns = df.iloc[0]
    df = df.drop(0).reset_index()

    ##Removing any Duplicate Columns
    df = df.loc[:, ~df.columns.duplicated()]

    ##Cleaning out Empty or Unecessary Columns/Rows
    df = df[['Player', 'Team', 'Nation', 'Pos', 'Age', 'Starts', 'Min', 'Gls', 'Ast', 'G+A','PK', 'PKatt', 'xG', 'xAG', 'CrdY', 'CrdR']]

    ##Cleaning Renaming Rows to be Understood Easier
    df = df.rename(columns={"Pos": "Position", "Min": "Minutes", "Gls": "Goals", "Ast": "Assists", "G+A": "G_A", "PKatt": "PK_att", "PK": "PK_Scored", "xAG": "x_AG", "CrdY": "Yellow_Cards", "CrdR": "Red_Cards"})
    df.columns = [x.lower() for x in df]

    ##Removing any empty rows
    df = df.dropna()

    ##Cleaning up any unwanted characters
    df['team'] = df['team'].apply(lambda a: a.replace("-", " "))
    df['nation'] = df['nation'].apply(lambda a: a[-3:])
    df['age'] = df['age'].apply(lambda a: a[:2])

    ##Updating Data Types for Future Usage
    df = df.astype({'age': 'float', 'starts': 'float', 'minutes': 'float', 'goals': 'float', 'assists': 'float', 'g_a': 'float', 'pk_scored': 'float',
                    'pk_att': 'float', 'yellow_cards': 'float', 'red_cards': 'float', 'xg': 'float', 'x_ag': 'float'})
    df = df.astype({'age': 'int', 'starts': 'int', 'minutes': 'int', 'goals': 'int', 'assists': 'int', 'g_a': 'int', 'pk_scored': 'int',
                    'pk_att': 'int', 'yellow_cards': 'int', 'red_cards': 'int'})
    ##Saving CSV File
    df.to_csv("player_stats.csv")

def data_to_postgres():
    ##Opening Database Connection
    conn_string = "" ##INCLUDE YOUR OWN DATABASE CONNECTION HERE
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    print('opened database successfully')

    ##Dropping old tables to update with new
    cursor.execute("drop table if exists player_stats;")

    ##Create new table
    cursor.execute("""create table player_stats (id serial primary key, player varchar, team varchar, nation varchar, position varchar, age int, 
                   starts int, minutes int, goals int, assists int, g_a int, pk_scored int, pk_att int, xg float, x_ag float, 
                   yellow_cards int, red_cards int)""")

    ##Open csv file
    my_file = open('player_stats.csv')
    
    ##Uploading to Database
    SQL_STATEMENT = """
    COPY player_stats FROM STDIN WITH
        CSV
        HEADER
        DELIMITER AS ','
    """

    cursor.copy_expert(sql=SQL_STATEMENT, file=my_file)

    cursor.execute("grant select on table player_stats to public")
    conn.commit()

    cursor.close()

    print("player_stats table deployed to database!")

data_to_postgres()
