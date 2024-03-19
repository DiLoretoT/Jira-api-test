import requests
import sqlalchemy
import pandas as pd
import base64
from pathlib import Path
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base
from utils import read_api_credentials

# AUTHENTICATION
api_credentials = read_api_credentials("config.ini", "api_jira")
api_token = api_credentials['token2']

# Trying credentials 
user_email = 'tdiloreto@algeiba.com'
credentials = f'{user_email}:{api_token}'
encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')

endpoint = 'users/search'

headers = {
    'Authorization': f'Basic {encoded_credentials}'
}

# Getting and preparing dataframe
def df_users(endpoint):
    
    api_url = f'https://algeiba.atlassian.net/rest/api/3/{endpoint}'

    all_users = []  # List to store all users across pages
    start_at = 0
    max_results = 200

    while True:
        response = requests.get(api_url, headers=headers, params={'startAt': start_at, 'maxResults': max_results})
    
        if response.status_code == 200:
            data = response.json()
            
            # Break the loop if no more users are returned
            if not data:
                break

            all_users.extend(data)
            start_at += max_results  # Or len(data) if you expect less than max_results
            
        else:
            print(f"Failed to fetch {endpoint}: {response.status_code}")
            break

    # Create a DataFrame from the combined list of all users
    df = pd.DataFrame(all_users)
    # Create a DataFrame from the values
    df = df[['accountId', 'active', 'accountType','displayName']]
    #print("Columns After Normalization:", list(df.columns))
    return df

# Call df_users function with the parameter endpoint to get users data from JIRA API.
users_df = df_users(endpoint)
print(users_df.head())

# Connect to the SQLite database
def setup_database():
    # Connection
    engine = create_engine('sqlite:///jiradatabase.db')
    Base = declarative_base()
    
    # Define the users table
    class User(Base):
        __tablename__ = 'users'
        accountId = Column(String, primary_key=True)
        active = Column(String)
        accountType = Column(String)
        displayName = Column(String)

    Base.metadata.create_all(engine)
    return engine

engine = setup_database()

table_name = "users"

users_df.to_sql(name='users', con=engine, if_exists='replace', index=False)

# DEBUG PRINT
print(f"The DataFrame was successfully loaded into the table '{table_name}'.")