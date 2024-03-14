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
endpoint1 = 'holiday-schemes/1/members'
endpoint3 = 'holiday-schemes/3/members'
endpoint4 = 'holiday-schemes/4/members'


headers = {
    'Authorization': f'Bearer {api_token}'
}

params = {
    'limit': 500,
}

# Getting and preparing dataframe
def df_holidaysSchemesMemb1(endpoint1):
    api_url1 = f'https://api.tempo.io/4/{endpoint1}'
    
    response = requests.get(api_url1, headers=headers, params=params)
    print(f'Status code: {response.status_code}')
    
    if response.status_code == 200:
        data = response.json()
        
        if 'results' in data and len(data['results']) > 0:
            print("Original columns: ", list(data['results'][0].keys()))
            df = pd.json_normalize(data, record_path=['results'], errors='ignore')
                        
            print("Columns After Normalization:", list(df.columns))
            
            df['schemeId'] = 1

            # Define the list of selected columns
            selected_columns = [
                'schemeId', 'accountId'
            ]

            # Select the defined columns
            df = df[selected_columns]
            print("Selected columns: ", df.columns)
            print(f"Found {len(df)} records")
            return df
        
        else: 
            print("No content in ['results'], inside holday-schemes.")
            
    else:
        print(f"Failed to fetch holday-schemes: {response.status_code}")
        return pd.DataFrame()  # Return an empty DataFrame if there's an error

# Call df_holidaysSchemes function with the parameter endpoint1 to get holidaysSchemes data from JIRA API.
holidaysSchemesMemb1_df = df_holidaysSchemesMemb1(endpoint1)

# Getting and preparing dataframe
def df_holidaysSchemesMemb3(endpoint3):
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
                'schemeId', 'accountId'
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
holidaysSchemesMemb3_df = df_holidaysSchemesMemb3(endpoint3)

# Getting and preparing dataframe
def df_holidaysSchemesMemb4(endpoint4):
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
                'schemeId', 'accountId'
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
holidaysSchemesMemb4_df = df_holidaysSchemesMemb4(endpoint4)

# Concat all schemes and members
holidaysSchemes_df = pd.concat([holidaysSchemesMemb1_df, holidaysSchemesMemb3_df, holidaysSchemesMemb4_df], ignore_index=True)

# Connect to the SQLite database
def setup_database():
    # Connection
    engine = create_engine('sqlite:///jiradatabase.db')
    Base = declarative_base()
    
    # Define the holidaysSchemes table
    class holidaysSchemesMemb(Base):
        __tablename__ = 'holidaysSchemesMemb'
        schemeId = Column(Integer, primary_key=True)
        accountId = Column(String(50))


    Base.metadata.create_all(engine)
    return engine

engine = setup_database()

table_name = "holidaysSchemesMemb"

holidaysSchemes_df.to_sql(name='holidaysSchemesMemb', con=engine, if_exists='replace', index=False)

# DEBUG PRINT
print(f"The DataFrame was successfully loaded into the table '{table_name}'.")