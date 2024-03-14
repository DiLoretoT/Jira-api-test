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
    # List to store all pages of worklogs
    all_data = []  # List to store all pages of worklogs
    next_url = api_url  # Start with the initial URL
    local_params = params.copy()  # Copy initial params to modify for pagination

    while next_url:
        response = requests.get(next_url, headers=headers, params=local_params)
        print(f'Status code: {response.status_code}, fetching: {next_url}')
        
        if response.status_code == 200:
            data = response.json()
            all_data.extend(data['results'])  # Add the results of the current page
            
            # Update next_url based on the presence of a 'next' link
            next_url = data['metadata'].get('next')
            local_params.clear()  # Clear params if all subsequent URLs contain full query parameters

            # If 'next' contains a relative path, construct the next full URL (optional, based on API behavior)
            # Example: next_url = f'{api_url}{next_url}' if next_url and not next_url.startswith('http') else next_url

        else:
            print(f"Failed to fetch worklogs: {response.status_code}")
            return pd.DataFrame()  # Return an empty DataFrame if there's an error
    
    # Normalize the aggregated data from all pages
    df = pd.json_normalize(all_data, errors='ignore')
    print("Columns After Normalization:", list(df.columns))
    
    # Initialize an empty list to store 'Horario Extendido' values
    horario_extendido_values = []
    for result in all_data:
        horario_extendido = False  # Default value if the attribute is not present
        for attribute in result.get('attributes', {}).get('values', []):
            if attribute.get('key') == '_HorarioExtendido_' and attribute.get('value') == 'true':
                horario_extendido = True
                break
        horario_extendido_values.append(horario_extendido)
    
    # Add the 'Horario Extendido' values as a new column to the dataframe
    df['Horario Extendido'] = horario_extendido_values
    
    # Define the list of selected columns
    selected_columns = [
        'tempoWorklogId', 'issue.id', 'author.accountId', 'timeSpentSeconds',
        'billableSeconds', 'startDate', 'startTime', 'description',
        'createdAt', 'updatedAt', 'Horario Extendido'
    ]

    # Ensure only selected columns are included, if they exist in the dataframe
    final_columns = [col for col in selected_columns if col in df.columns]
    df = df[final_columns]
    print("Selected columns: ", df.columns)
    print(f"Found {len(df)} records")
    return df

# Call df_worklogs function with the parameter endpoint1 to get worklogs data from JIRA API.
worklogs_df = df_worklogs(endpoint)
print(type(worklogs_df))

if worklogs_df is not None:
    # Handle NaN values before casting
    if worklogs_df['tempoWorklogId'].isnull().any():
        # Choose a strategy to handle NaN values. For example, you can fill them with a placeholder:
        worklogs_df['tempoWorklogId'].fillna(0, inplace=True)  # Or another value that makes sense for your context

    # Now it's safe to cast the column to int
    worklogs_df['tempoWorklogId'] = worklogs_df['tempoWorklogId'].astype(str)
    worklogs_df['issue.id'] = worklogs_df['issue.id'].astype(str)
else:
    print("df_worklogs returned None.")

# Convert 'timeSpentSeconds' to hours and drop the original column
worklogs_df['timeSpentSeconds'] = (worklogs_df['timeSpentSeconds'] / 3600).round(2)
worklogs_df.rename(columns={'timeSpentSeconds': 'hours'}, inplace=True)

# Convert 'billableSeconds' to hours and drop the original column
worklogs_df['billableSeconds'] = (worklogs_df['billableSeconds'] / 3600).round(2)
worklogs_df.rename(columns={'billableSeconds': 'billedHours'}, inplace=True)

print(worklogs_df.head(5))

# Connect to the SQLite database
def setup_database():
    # Connection
    engine = create_engine('sqlite:///jiradatabase.db')
    Base = declarative_base()
    
    # Define the worklogs table
    class Worklogs(Base):
        __tablename__ = 'worklogs'
        tempoWorklogId = Column(String, primary_key=True)
        issueId = Column(String)
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