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
endpoint1 = 'holiday-schemes/1/holidays'
endpoint3 = 'holiday-schemes/3/holidays'
endpoint4 = 'holiday-schemes/4/holidays'


headers = {
    'Authorization': f'Bearer {api_token}'
}

params = {
    'limit': 500,
}

# Getting and preparing dataframe
def df_holidaysSchemeDays1(endpoint1):
    api_url1 = f'https://api.tempo.io/4/{endpoint1}'
    
    response = requests.get(api_url1, headers=headers, params=params)
    print(f'Status code: {response.status_code}')
    
    if response.status_code == 200:
        data = response.json()
        
        if 'results' in data and len(data['results']) > 0:
            print("Original columns: ", list(data['results'][0].keys()))
            df = pd.json_normalize(data, record_path=['results'], errors='ignore')
                        
            print("Columns After Normalization:", list(df.columns))
            
            # Define the list of selected columns
            selected_columns = [
                'schemeId', 'name', 'description', 'date', 'durationSeconds'
            ]

            # Select the defined columns
            df = df[selected_columns]
            print("Selected columns: ", df.columns)
            print(f"Found {len(df)} records")
            return df
        
        else: 
            print("No content in ['results'], inside holday-schemes-days.")
            
    else:
        print(f"Failed to fetch holday-schemes-days: {response.status_code}")
        return pd.DataFrame()  # Return an empty DataFrame if there's an error

# Call df_holidaysSchemes function with the parameter endpoint1 to get holidaysSchemes data from JIRA API.
holidaysSchemeDays1_df = df_holidaysSchemeDays1(endpoint1)

# Getting and preparing dataframe
def df_holidaysSchemeDays3(endpoint3):
    api_url1 = f'https://api.tempo.io/4/{endpoint3}'
    
    response = requests.get(api_url1, headers=headers, params=params)
    print(f'Status code: {response.status_code}')
    
    if response.status_code == 200:
        data = response.json()
        
        if 'results' in data and len(data['results']) > 0:
            print("Original columns: ", list(data['results'][0].keys()))
            df = pd.json_normalize(data, record_path=['results'], errors='ignore')
                        
            print("Columns After Normalization:", list(df.columns))
            
            df['schemeId'] = 3

            # Define the list of selected columns
            selected_columns = [
                'schemeId', 'name', 'description', 'date', 'durationSeconds'
            ]

            # Select the defined columns
            df = df[selected_columns]
            print("Selected columns: ", df.columns)
            print(f"Found {len(df)} records")
            return df
        
        else: 
            print("No content in ['results'], inside holday-schemes.")
            
    else:
        print(f"Failed to fetch holday-scheme3: {response.status_code}")
        return pd.DataFrame()  # Return an empty DataFrame if there's an error

# Call df_holidaysSchemes function with the parameter endpoint1 to get holidaysSchemes data from JIRA API.
holidaysSchemeDays3_df = df_holidaysSchemeDays3(endpoint3)

# Getting and preparing dataframe
def df_holidaysSchemeDays4(endpoint4):
    api_url1 = f'https://api.tempo.io/4/{endpoint4}'
    
    response = requests.get(api_url1, headers=headers, params=params)
    print(f'Status code: {response.status_code}')
    
    if response.status_code == 200:
        data = response.json()
        
        if 'results' in data and len(data['results']) > 0:
            print("Original columns: ", list(data['results'][0].keys()))
            df = pd.json_normalize(data, record_path=['results'], errors='ignore')
                        
            print("Columns After Normalization:", list(df.columns))
            
            df['schemeId'] = 4

            # Define the list of selected columns
            selected_columns = [
                'schemeId', 'name', 'description', 'date', 'durationSeconds'
            ]

            # Select the defined columns
            df = df[selected_columns]
            print("Selected columns: ", df.columns)
            print(f"Found {len(df)} records")
            return df
        
        else: 
            print("No content in ['results'], inside holday-schemes.")
            
    else:
        print(f"Failed to fetch holday-scheme4: {response.status_code}")
        return pd.DataFrame()  # Return an empty DataFrame if there's an error

# Call df_holidaysSchemes function with the parameter endpoint1 to get holidaysSchemes data from JIRA API.
holidaysSchemeDays4_df = df_holidaysSchemeDays4(endpoint4)

# Concat all schemes and members
holidaysSchemesDays_df = pd.concat([holidaysSchemeDays1_df, holidaysSchemeDays3_df, holidaysSchemeDays4_df], ignore_index=True)

# Convert 'timeSpentSeconds' to hours and drop the original column
holidaysSchemesDays_df['durationSeconds'] = (holidaysSchemesDays_df['durationSeconds'] / 86400).round(2)
holidaysSchemesDays_df.rename(columns={'durationSeconds': 'days'}, inplace=True)


# Connect to the SQLite database
def setup_database():
    # Connection
    engine = create_engine('sqlite:///jiradatabase.db')
    Base = declarative_base()
    
    # Define the holidaysSchemes table
    class holidaysSchemesDays(Base):
        __tablename__ = 'holidaysSchemesDays'
        schemeId = Column(Integer, primary_key=True)
        name = Column(String)
        description = Column(String)
        date = Column(DateTime)
        days = Column(Integer)


    Base.metadata.create_all(engine)
    return engine

engine = setup_database()

table_name = "holidaysSchemesDays"

holidaysSchemesDays_df.to_sql(name='holidaysSchemesDays', con=engine, if_exists='replace', index=False)

# DEBUG PRINT
print(f"The DataFrame was successfully loaded into the table '{table_name}'.")