import requests
import sqlalchemy
import pandas as pd
import base64
from pathlib import Path
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base
from utils import read_api_credentials, get_sqlite_db_connection, close_sqlite_db_connection

# AUTHENTICATION
api_credentials = read_api_credentials("config.ini", "api_jira")
api_token = api_credentials['token2']

# Trying credentials 
user_email = 'tdiloreto@algeiba.com'
credentials = f'{user_email}:{api_token}'
encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')

endpoint = 'project/search'

headers = {
    'Authorization': f'Basic {encoded_credentials}'
}

params = {
    'maxResults': 50
}

# Getting and preparing dataframe
def df_projects(endpoint):
    api_url = f'https://algeiba.atlassian.net/rest/api/3/{endpoint}'
    
    all_projects = []  # List to store all projects across pages
    
    start_at = 0
    max_results = 200
    total = None

    while total is None or start_at < total:
        response = requests.get(api_url, headers=headers, params={'startAt': start_at, 'maxResults': max_results})
              
    
        if response.status_code == 200:
            
            data = response.json()
            all_projects.extend(data['values'])     # Add the projects from the current page to the list
            start_at += len(data['values'])         # Increment by the number of results returned
            total = data['total'] if total is None else total

        else:
            print(f"Failed to fetch projects: {response.status_code}")
            return pd.DataFrame()  # Return an empty DataFrame if there's an error

    
    # Create a DataFrame from the combined list of all projects
    df = pd.DataFrame(all_projects)
    df = df[['id', 'key', 'name']]
    #print("Columns After Normalization:", list(df.columns))
    return df


# Call df_projects function with the parameter endpoint to get projects data from JIRA API.
projects_df = df_projects(endpoint)
print(projects_df.head())

# Connect to the SQLite database
def setup_database():
    # Connection
    engine = create_engine('sqlite:///jiradatabase.db')
    Base = declarative_base()
    
    # Define the projects table
    class Project(Base):
        __tablename__ = 'projects'
        id = Column(Integer, primary_key=True)
        key = Column(String)
        name = Column(String)
        
    Base.metadata.create_all(engine)
    return engine

engine = setup_database()

table_name = "projects"

projects_df.to_sql(name='projects', con=engine, if_exists='replace', index=False)

# DEBUG PRINT
print(f"The DataFrame was successfully loaded into the table '{table_name}'.")