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
    response = requests.get(api_url, headers=headers)
    print(f'Status code: {response.status_code}')
    
    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data)
        # Create a DataFrame from the values
        df = df[['accountId', 'active', 'displayName']]
        print("Columns After Normalization:", list(df.columns))
        return df
    
    else:
        print(f"Failed to fetch users: {response.status_code}")
        return pd.DataFrame()  # Return an empty DataFrame if there's an error

# Call df_users function with the parameter endpoint to get users data from JIRA API.
users_df = df_users(endpoint)
print(users_df)

# Connect to the SQLite database
def setup_database():
    # Connection
    engine = create_engine('sqlite:///jiradatabase.db')
    Base = declarative_base()
    
    # Define the users table
    class User(Base):
        __tablename__ = 'users'
        accountId = Column(Integer, primary_key=True)
        active = Column(String)
        displayName = Column(String)

    Base.metadata.create_all(engine)
    return engine

engine = setup_database()

table_name = "users"

users_df.to_sql(name='users', con=engine, if_exists='replace', index=False)

# DEBUG PRINT
print(f"The DataFrame was successfully loaded into the table '{table_name}'.")