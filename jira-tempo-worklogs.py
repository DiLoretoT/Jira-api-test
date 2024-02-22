import requests
import json
import sqlalchemy
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text
from sqlalchemy.orm import declarative_base
from utils import read_api_credentials, get_sqlite_db_connection, close_sqlite_db_connection

# AUTHENTICATION
api_credentials = read_api_credentials("config.ini", "api_tempo")
api_token = api_credentials['token']
endpoint = 'worklogs'

headers = {
    'Authorization': f'Bearer {api_token}'
}

params = {
    'from': '2024-02-01',
    'to': '2024-02-02',
    'limit': 100,
}

# Getting and preparing dataframe
def df_worklogs(endpoint):
    api_url = f'https://api.tempo.io/4/{endpoint}'
    response = requests.get(api_url, headers=headers, params=params)
    print(f'Status code: {response.status_code}')
    
    if response.status_code == 200:
        data = response.json()
        
        # Use json_normalize to handle nested JSON
    if response.status_code == 200:
        data = response.json()
        
        if 'results' in data and len(data['results']) > 0:
            print("Original columns: ", list(data['results'][0].keys()))
            df = pd.json_normalize(data, record_path=['results'], errors='ignore')
            
            # Initialize an empty list to store horario extendido values
            horario_extendido_values = []

            # Loop through each result and extract the 'Horario Extendido' attribute if it exists
            for result in data['results']:
                horario_extendido = False  # Default value if the attribute is not present
                for attribute in result['attributes']['values']:
                    if attribute['key'] == '_HorarioExtendido_' and attribute['value'] == 'true':
                        horario_extendido = True
                        break
                horario_extendido_values.append(horario_extendido)

            # Add the 'Horario Extendido' values as a new column to the dataframe
            df['Horario Extendido'] = horario_extendido_values
            
            print("Columns After Normalization:", list(df.columns))
            
            # Define the list of selected columns
            selected_columns = [
                'tempoWorklogId', 'issue.id', 'author.accountId', 'timeSpentSeconds',
                'billableSeconds', 'startDate', 'startTime', 'description',
                'createdAt', 'updatedAt', 'Horario Extendido'
            ]

            # Select the defined columns
            df = df[selected_columns]
            print("Selected columns: ", df.columns)
            print(f"Found {len(df)} records")
            return df
            
    else:
        print(f"Failed to fetch users: {response.status_code}")
        return pd.DataFrame()  # Return an empty DataFrame if there's an error


# Call df_worklogs function with the parameter endpoint1 to get worklogs data from JIRA API.
worklogs_df = df_worklogs(endpoint)

# Convert 'timeSpentSeconds' to hours and drop the original column
worklogs_df['timeSpentSeconds'] = (worklogs_df['timeSpentSeconds'] / 3600).round(2)
worklogs_df.rename(columns={'timeSpentSeconds': 'hours'}, inplace=True)

# Convert 'billableSeconds' to hours and drop the original column
worklogs_df['billableSeconds'] = (worklogs_df['billableSeconds'] / 3600).round(2)
worklogs_df.rename(columns={'billableSeconds': 'billedHours'}, inplace=True)

print(worklogs_df)

# Connect to the SQLite database
def setup_database():
    # Connection
    engine = create_engine('sqlite:///jiradatabase.db')
    Base = declarative_base()
    
    # Define the worklogs table
    class Worklogs(Base):
        __tablename__ = 'worklogs'
        tempoWorklogId = Column(Integer, primary_key=True)
        issueId = Column(String(40))
        userAccountId = Column(String(50))
        hours = Column(Integer)
        billedHours = Column(Integer)
        startDate = Column(DateTime)
        startTime = Column(DateTime)
        description = Column(Text)
        createdAt = Column(DateTime)
        updatedAt = Column(DateTime)
        
    Base.metadata.create_all(engine)
    return engine
engine = setup_database()

table_name = "worklogs"

worklogs_df.to_sql(name='worklogs', con=engine, if_exists='replace', index=False)

# DEBUG PRINT
print(f"The DataFrame was successfully loaded into the table '{table_name}'.")