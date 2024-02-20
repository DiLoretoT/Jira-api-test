import requests
import sqlalchemy
import pandas as pd
import base64
from datetime import datetime, timedelta
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

# Calculate the date 90 days ago from the current date
ninety_days_ago = datetime.now() - timedelta(days=180)
# Format the date in the format Jira expects (yyyy/mm/dd)
ninety_days_ago_formatted = ninety_days_ago.strftime('%Y-%m-%d')

# Construct the JQL query to retrieve issues created in the last 90 days
jql_query = f"created >= '{ninety_days_ago_formatted}'"
endpoint = 'search'

jql_query = f"created >= '{ninety_days_ago_formatted}'"

headers = {
    'Authorization': f'Basic {encoded_credentials}'
}

params = {
    'jql': jql_query,
    'fields': ','.join([
        'key', 'summary', 'status', 'project', 'assignee', 'created', 'reporter', 'issuetype',
        'customfield_10101', 'customfield_10100', 'customfield_10099', 'customfield_10217',
        'customfield_10167', 'resolutiondate',
        # Add more custom fields as needed
    
    ]),
    'maxResults': 1000
}


# Getting and preparing dataframe
def df_issues(endpoint):
    api_url = f'https://algeiba.atlassian.net/rest/api/3/{endpoint}'
    response = requests.get(api_url, headers=headers, params=params)
    print(f'Status code: {response.status_code}')
    
    # Check for a successful response
    if response.status_code == 200:
        # Parse the JSON response
        data = response.json()
        df = pd.DataFrame(data)
        print(df.columns)
        
        # Normalize the JSON response to convert to a dataframe
        df_issues = pd.json_normalize(
            data['issues'],
            record_path=None,
            meta=[
                'id', 'key', 'summary', ['fields', 'assignee', 'accountId'], ['fields', 'status', 'description'],
            ['fields', 'customfield_10051', 'id'], ['fields', 'creator', 'accountId'],
            ['fields', 'reporter', 'accountId'], ['fields', 'issuetype', 'name'],
            ['fields', 'project', 'id'], ['fields', 'resolutiondate'],
            ['fields', 'created'],
            # Add paths to custom fields here, for example ['fields', 'customfield_10101']
            ],       
        )
        

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
            'fields.summary': 'summary',
            'fields.customfield_10167.value': 'Scania Activity Type',
            'fields.customfield_10217.value': 'RZBT Activity Type',
                        
            # Add more renames for custom fields as needed
        }, inplace=True)
                
        print("Columns after process and rename: ", df_issues.columns)

        selected_columns = ['id', 'key', 'summary', 'issueType', 'createdDate', 'resolutionDate', 'projectId', 'reporterId', 'assigneeId', 'statusDescription', 'Scania Activity Type', 'RZBT Activity Type']

        df_issues = df_issues[selected_columns]

        print("Columns after filter: ", df_issues.columns)

        return df_issues
    
    else:
        print(f"Failed to fetch issues: {response.status_code}")
        return pd.DataFrame()  # Return an empty DataFrame if there's an error

# Call df_issues function with the parameter endpoint to get issues data from JIRA API.
issues_df = df_issues(endpoint)
print(issues_df)

# Connect to the SQLite database
#def setup_database():
#    # Connection
#    engine = create_engine('sqlite:///jiradatabase.db')
#    Base = declarative_base()
#    
#    # Define the issues table
#    class User(Base):
#        __tablename__ = 'issues'
#        accountId = Column(Integer, primary_key=True)
#        active = Column(String)
#        displayName = Column(String)

#    Base.metadata.create_all(engine)
#    return engine

#engine = setup_database()

#table_name = "issues"

#issues_df.to_sql(name='issues', con=engine, if_exists='replace', index=False)

# DEBUG PRINT
#print(f"The DataFrame was successfully loaded into the table '{table_name}'.")