import requests
import logging
import sqlalchemy
import pandas as pd
import base64
from datetime import datetime, timedelta
from pathlib import Path
from sqlalchemy import create_engine, Column, Integer, String, DateTime, text
from sqlalchemy.orm import declarative_base
from utils import read_api_credentials
from sqlalchemy.orm import sessionmaker
from dateutil.relativedelta import relativedelta

# Configure logging at the start of your script
logging.basicConfig(level=logging.INFO)

# AUTHENTICATION
api_credentials = read_api_credentials("config.ini", "api_jira")
api_token = api_credentials['token2']
user_email = api_credentials['user_email']

# Trying credentials 
#user_email = 'tdiloreto@algeiba.com'
credentials = f'{user_email}:{api_token}'
encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')

# Construct the JQL query to retrieve issues created this month
#current_month = datetime.now().month
#current_year = datetime.now().year

# Adjust for year wrapping if current month is December
#if current_month == 12:
#    next_month = 1
#    next_year = current_year + 1
#else:
#    next_month = current_month + 1
#    next_year = current_year

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

# Construct the JQL query to retrieve issues created in the last 90 days
jql_query = f"created >= '{six_months_ago_start}' ORDER BY created ASC"
endpoint = 'search'

headers = {
    'Authorization': f'Basic {encoded_credentials}'
}
    

params = {
    'jql': jql_query,
    'fields': ','.join([
        'key', 'summary', 'status', 'project', 'assignee', 'created', 'reporter', 'issuetype', 'updated', 'timeestimate'
        'customfield_10101', 'customfield_10100', 'customfield_10099', 'customfield_10217','customfield_10167', 'resolutiondate',
        'customfield_10105', 'customfield_10104', 'customfield_10056', 'customfield_10055', 'customfield_10051', 'customfield_10215'
        ]),
    'maxResults': 200
}

# Getting and preparing dataframe
def df_issues(endpoint):
    api_url = f'https://algeiba.atlassian.net/rest/api/3/{endpoint}'
    
    all_issues = []

    start_at = 0
    max_results = 100
    total = None

    print("Making API request...")
    while total is None or start_at < total:
        params['startAt'] = start_at
        response = requests.get(api_url, headers=headers, params=params)
        print(f'Status code: {response.status_code}, start at line: {start_at}')

        if response.status_code == 200:
            # Parse the JSON response
            data = response.json()
            all_issues.extend(data['issues'])
            
            start_at += max_results
            total = data['total']

        else:
            print(f"Failed to fetch issues: {response.status_code}")
            return pd.DataFrame()  # Return an empty DataFrame if there's an error
            break

        # Normalize the JSON response to convert to a dataframe
        #print("The request to /search endpoint was successfull.")
        df_issues = pd.json_normalize(all_issues, errors='ignore')
        #print("Columns BEFORE renaming: ", df_issues.columns)

        #print("Normalizing custom fields and getting None for issues without them...")
        for custom_field in ['fields.customfield_10051.id', 'fields.customfield_10099.value', 'fields.customfield_10100.value', 'fields.customfield_10167.value','fields.customfield_10217',
                            'fields.customfield_10105','fields.customfield_10104', 'fields.timeestimate', 'fields.customfield_10056', 'fields.customfield_10055', 'fields.customfield_10215']:
            if custom_field not in df_issues.columns:
                df_issues[custom_field] = None # Create the column with None values

        #print("Renaming columns...")      
        # Rename the columns for better readability
        df_issues.rename(columns={
            'fields.assignee.accountId': 'assigneeId',
            'fields.status.description': 'statusDescription',
            'fields.customfield_10051.id': 'accountId',
            'fields.creator.accountId': 'creatorId',
            'fields.reporter.accountId': 'reporterId',
            'fields.issuetype.name': 'issueType',
            'fields.project.id': 'projectId',
            'fields.resolutiondate': 'resolutionDate',
            'fields.created': 'createdDate',
            'fields.updated': 'updatedDate',
            'fields.summary': 'summary',
            'fields.customfield_10167.value': 'Scania Activity Type',
            'fields.customfield_10217.value': 'RZBT Activity Type',
            'fields.customfield_10100.value': 'GHZ Organization',
            'fields.customfield_10099.value': 'GP Organization',
            'fields.customfield_10104': '% Invoiced',
            'fields.customfield_10105': '% Advance',
            'fields.timeestimate': 'TimeEstimate',
            'fields.customfield_10055': 'StartDate',
            'fields.customfield_10056': 'EndDate',
            'fields.customfield_10215': 'FinalDate'
            
        }, inplace=True)

        df_issues['TimeEstimate'] = (df_issues['TimeEstimate'] / 3600).round(2)
                
        #print("Columns after process and rename: ", df_issues.columns)
        #print("Selecting columns...")
        selected_columns = ['id', 'key', 'summary', 'issueType', 'createdDate', 'updatedDate', 'resolutionDate', 'projectId','accountId', 'reporterId', 'assigneeId', 'statusDescription',
                            'GHZ Organization','GP Organization', 'Scania Activity Type', 'RZBT Activity Type','% Invoiced','% Advance', 'TimeEstimate', 'StartDate', 'EndDate', 'FinalDate']

        df_selected = df_issues[selected_columns]
        
        #print("Columns after filter: ", df_selected.columns)

        #return df_selected
    
    #else:
    #    print(f"Failed to fetch issues: {response.status_code}")
    #    return pd.DataFrame()  # Return an empty DataFrame if there's an error

    print(f"Found {len(df_selected)} records")

    if all_issues:
        df_issues = pd.json_normalize(all_issues, errors='ignore')
        # The rest of your DataFrame processing code here
    
        return df_selected
    else:
        print("No issues fetched.")
        return pd.DataFrame()
    

# Call df_issues function with the parameter endpoint to get issues data from JIRA API.
#print("Getting function up to work...")
issues_df = df_issues(endpoint)

print("Dataframe right now: ")
print(issues_df.head())


print("Connecting to the database...")
# Connect to the SQLite database

Base = declarative_base()

# Define the stg_issues table
class stg_issues(Base):
    __tablename__ = 'stg_issues'
    id = Column(Integer, primary_key=True)        
    key = Column(String)
    summary = Column(String)
    issueType = Column(String)
    createdDate = Column(DateTime)
    updatedDate = Column(DateTime)
    resolutionDate = Column(DateTime)
    projectId = Column(Integer)
    accountId = Column(Integer)
    reporterId = Column(String)
    assigneeId = Column(String)
    statusDescription = Column(String)
    GHZOrganization = Column(String)
    GPOrganization = Column(String)
    ScaniaActivityType = Column(String)
    RZBTActivityType = Column(String)
    InvoicedPercent = Column(Integer)
    AdvancePercent = Column(Integer)
    TimeEstimate = Column(Integer)
    StartDate = Column(DateTime)
    EndDate = Column(DateTime)
    FinalDate = Column(DateTime)

# Set up the database and create tables
def setup_database():
    engine = create_engine('sqlite:///jiradatabase.db')
    Base.metadata.create_all(engine)
    return engine

engine = create_engine('sqlite:///jiradatabase.db')
Session = sessionmaker(bind=engine)
session = Session()

# Load the data into the staging table
print("Loading Dataframe into 'stg_issues'...")
try:
    issues_df.to_sql('stg_issues', engine, if_exists='replace', index=False)
    print("Dataframe successfully loaded into 'stg_issues'...")
    
except Exception as e: 
    logging.info("Dataframe failed to be loaded into 'stg_issues: {e}'")

# Execute the upsert operation using raw SQL
with engine.begin() as connection:
        
    # Execute the SQL statements to create the indexes
    print("Creating indexes...")
    connection.execute(text("""
    CREATE INDEX IF NOT EXISTS idx_issues_id_updatedDate ON issues(id, updatedDate);
    """))
    connection.execute(text("""
    CREATE INDEX IF NOT EXISTS idx_stg_issues_id_updatedDate ON stg_issues(id, updatedDate);
    """))
    print("Indexes created successfully.")

    # Query to INSERT non-existing records from 'stg_issues' into 'issues'
    print("Starting query to INSERT non-existing records from 'stg_issues' into 'issues'")
    connection.execute(text("""
    INSERT INTO issues (id, key, summary, issueType, createdDate, updatedDate, resolutionDate, projectId, accountId, reporterId, assigneeId, statusDescription, "GHZ Organization", "GP Organization", "Scania Activity Type", "RZBT Activity Type", "% Invoiced", "% Advance", TimeEstimate, StartDate, EndDate, FinalDate)
    SELECT id, key, summary, issueType, createdDate, updatedDate, resolutionDate, projectId, accountId, reporterId, assigneeId, statusDescription, "GHZ Organization", "GP Organization", "Scania Activity Type", "RZBT Activity Type", "% Invoiced", "% Advance", TimeEstimate, StartDate, EndDate, FinalDate
    FROM stg_issues
    WHERE id NOT IN (SELECT id FROM issues)
    """))
    print("The query run successfully.")

    # Query to UPDATE non-existing records from 'stg_issues' into 'issues'
    print("Starting query to UPDATE non-existing records from 'stg_issues' into 'issues'")
    # Identify records that require updates
    records_to_update = connection.execute(text("""
        SELECT stg.id AS "id", stg.key AS "key", stg.summary AS "summary", stg.issueType AS "issueType", 
            stg.createdDate AS "createdDate", stg.updatedDate AS "updatedDate", stg.resolutionDate AS "resolutionDate", 
            stg.projectId AS "projectId", stg.accountId AS "accountId", stg.reporterId AS "reporterId", 
            stg.assigneeId AS "assigneeId", stg.statusDescription AS "statusDescription", 
            stg."GHZ Organization" AS "GHZOrganization", stg."GP Organization" AS "GPOrganization", 
            stg."Scania Activity Type" AS "ScaniaActivityType", stg."RZBT Activity Type" AS "RZBTActivityType", 
            stg."% Invoiced" AS "InvoicedPercent", stg."% Advance" AS "AdvancePercent", 
            stg.TimeEstimate AS "TimeEstimate", stg.StartDate AS "StartDate", stg.EndDate AS "EndDate", stg.FinalDate AS "FinalDate"
        FROM stg_issues stg
        JOIN issues i ON stg.id = i.id
        WHERE stg.updatedDate != i.updatedDate
    """)).mappings().fetchall()

    # Update identified records
    for record in records_to_update:
        connection.execute(text("""
            UPDATE issues
            SET key = :key, summary = :summary, issueType = :issueType, createdDate = :createdDate, updatedDate = :updatedDate, 
                resolutionDate = :resolutionDate, projectId = :projectId, accountId = :accountId, reporterId = :reporterId, 
                assigneeId = :assigneeId, statusDescription = :statusDescription, "GHZ Organization" = :GHZOrganization, 
                "GP Organization" = :GPOrganization, "Scania Activity Type" = :ScaniaActivityType, "RZBT Activity Type" = :RZBTActivityType, 
                "% Invoiced" = :InvoicedPercent, "% Advance" = :AdvancePercent, TimeEstimate = :TimeEstimate, 
                StartDate = :StartDate, EndDate = :EndDate, FinalDate = :FinalDate
            WHERE id = :id
        """), record)
    print("The query run successfully.")

    print("Starting query to delete 'stg_issues' table.")
    connection.execute(text("DROP TABLE IF EXISTS stg_issues"))
    print("The query run successfully.")

print("The issues table has been updated.")