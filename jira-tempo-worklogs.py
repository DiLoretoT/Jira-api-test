import requests
import json
import sqlalchemy
import logging
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Text, text
from sqlalchemy.orm import declarative_base, sessionmaker
from dateutil.relativedelta import relativedelta
from utils import read_api_credentials, get_sqlite_db_connection, close_sqlite_db_connection

# Configure logging at the start of your script
logging.basicConfig(level=logging.INFO)

# AUTHENTICATION
api_credentials = read_api_credentials("config.ini", "api_tempo")
api_token = api_credentials['token']
endpoint = 'worklogs'

headers = {
    'Authorization': f'Bearer {api_token}'
}

# Current Date
today = datetime.now().strftime('%Y-%m-%d')
# X days ago (indicate how many)
x_days_ago = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
# This Month (Starting from the first day of the current month)
this_month_start = datetime.now().replace(day=1).strftime('%Y-%m-%d')
# Last Month (First and Last Day)
last_month_start = (datetime.now().replace(day=1) - relativedelta(months=1)).strftime('%Y-%m-%d')
last_month_end = (datetime.now().replace(day=1) - relativedelta(days=1)).strftime('%Y-%m-%d')
# More Months Ago: First Day and Last Day
two_months_ago_start = (datetime.now().replace(day=1) - relativedelta(months=2)).strftime('%Y-%m-%d')
two_months_ago_end = (datetime.now().replace(day=1) - relativedelta(months=1) - relativedelta(days=1)).strftime('%Y-%m-%d')

three_months_ago_start = (datetime.now().replace(day=1) - relativedelta(months=3)).strftime('%Y-%m-%d')
three_months_ago_end = (datetime.now().replace(day=1) - relativedelta(months=2) - relativedelta(days=1)).strftime('%Y-%m-%d')

four_months_ago_start = (datetime.now().replace(day=1) - relativedelta(months=4)).strftime('%Y-%m-%d')
four_months_ago_end = (datetime.now().replace(day=1) - relativedelta(months=3) - relativedelta(days=1)).strftime('%Y-%m-%d')

five_months_ago_start = (datetime.now().replace(day=1) - relativedelta(months=5)).strftime('%Y-%m-%d')
five_months_ago_end = (datetime.now().replace(day=1) - relativedelta(months=4) - relativedelta(days=1)).strftime('%Y-%m-%d')

six_months_ago_start = (datetime.now().replace(day=1) - relativedelta(months=6)).strftime('%Y-%m-%d')
six_months_ago_end = (datetime.now().replace(day=1) - relativedelta(months=5) - relativedelta(days=1)).strftime('%Y-%m-%d')

print(f"Getting worklogs from {six_months_ago_start} to {today}")


params = {
    'from': f"{six_months_ago_start}",
    'to': f"{today}",
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
    #print("Selected columns: ", df.columns)
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

# Changin date format
worklogs_df['createdAt'] = pd.to_datetime(worklogs_df['createdAt'])
worklogs_df['updatedAt'] = pd.to_datetime(worklogs_df['updatedAt'])

# Renaming
worklogs_df.rename(columns={'issue.id': 'issueId'}, inplace=True)
worklogs_df.rename(columns={'author.accountId': 'userAccountId'}, inplace=True)

print(worklogs_df.head(5))

print("Connecting to the database...")

Base = declarative_base()

# Define the worklogs table
class stg_worklogs(Base):
    __tablename__ = 'stg_worklogs'
    tempoWorklogId = Column(Integer, primary_key=True)
    issueId = Column(Integer)
    userAccountId = Column(String(50))
    hours = Column(Integer)
    billedHours = Column(Integer)
    startDate = Column(DateTime)
    startTime = Column(DateTime)
    description = Column(Text)
    createdAt = Column(DateTime)
    updatedAt = Column(DateTime)

# Set up the database and create tables
def setup_database():
    engine = create_engine('sqlite:///jiradatabase.db')
    Base.metadata.create_all(engine)
    return engine

engine = create_engine('sqlite:///jiradatabase.db')
Session = sessionmaker(bind=engine)
session = Session()

# Load the data into the staging table
print("Loading Dataframe into 'stg_worklogs'...")
try:
    worklogs_df.to_sql(name='stg_worklogs', con=engine, if_exists='replace', index=False)
    print("Dataframe successfully loaded into 'stg_worklogs'...")

except Exception as e:
    logging.info("Dataframe failed to be loaded into 'stg_worklogs: {e}'")

# Execute the upsert operation using raw SQL
with engine.begin() as connection:
    # Query to INSERT non-existing records from 'stg_worklogs' into 'worklogs'

    # Execute the SQL statements to create the indexes
    print("Creating indexes...")
    connection.execute(text("""
    CREATE INDEX IF NOT EXISTS idx_worklogs_tempoWorklogId_updatedAt ON worklogs(tempoWorklogId, updatedAt);
    """))
    connection.execute(text("""
    CREATE INDEX IF NOT EXISTS idx_stg_worklogs_tempoWorklogId_updatedAt ON stg_worklogs(tempoWorklogId, updatedAt);
    """))
    print("Indexes created successfully.")

    print("Starting query to INSERT non-existing records from 'stg_worklogs' into 'worklogs'")
    connection.execute(text("""
    INSERT INTO worklogs (tempoWorklogId, issueId, userAccountId, hours, billedHours, startDate, startTime, description, createdAt)
    SELECT tempoWorklogId, issueId, userAccountId, hours, billedHours, startDate, startTime, description, createdAt
    FROM stg_worklogs
    WHERE tempoWorklogId NOT IN (SELECT tempoWorklogId FROM worklogs)
    """))
    print("The query run successfully.")

    # Identify records that require updates
    print("Starting query to UPDATE existing records from 'stg_worklogs' into 'worklogs'")
    records_to_update = connection.execute(text("""
        SELECT stg.tempoWorklogId, stg.issueId, stg.userAccountId, stg.hours, stg.billedHours, stg.startDate, stg.startTime, stg.description, stg.createdAt
        FROM stg_worklogs stg
        JOIN worklogs w ON stg.tempoWorklogId = w.tempoWorklogId
        WHERE stg.updatedAt != w.updatedAt
    """)).fetchall()

    # Update identified records
    for record in records_to_update:
        connection.execute(text("""
            UPDATE worklogs
            SET issueId = ?, userAccountId = ?, hours = ?, billedHours = ?, startDate = ?, startTime = ?, description = ?, createdAt = ?
            WHERE tempoWorklogId = ?
        """), 
        (record['issueId'], record['userAccountId'], record['hours'], record['billedHours'], record['startDate'], record['startTime'], record['description'], record['createdAt'], record['tempoWorklogId']))
    print("The query run successfully.")

    print("Starting query to delete 'stg_worklogs' table.")
    connection.execute(text("DROP TABLE IF EXISTS stg_worklogs"))
    print("The query run successfully.")

print("The worklogs table has been updated.")