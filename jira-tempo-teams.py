import requests
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base
from utils import read_api_credentials, get_sqlite_db_connection, close_sqlite_db_connection

# AUTHENTICATION
api_credentials = read_api_credentials("config.ini", "api_tempo")
api_token = api_credentials['token']
endpoint1 = 'teams'

headers = {
    'Authorization': f'Bearer {api_token}'
}

# Getting and preparing dataframe
def df_teams(endpoint):
    api_url = f'https://api.tempo.io/4/{endpoint}'
    response = requests.get(api_url, headers=headers)
    print(f'Status code: {response.status_code}')
    
    if response.status_code == 200:
        data = response.json()
        df = pd.json_normalize(data['results'])
        #print("Columns After Normalization:", list(df.columns))
        
        # Define columns of interest, including the original 'links.self'
        selected_columns = ['id', 'name', 'lead.accountId', 'members.self']
        df = df[selected_columns]
        
        # Rename columns for clarity
        rename_columns = {
            'lead.accountId': 'leadId',
            'members.self': 'teamLink'
        }

        df = df.rename(columns=rename_columns)
        print("Columns after Filter and Renaming: ", list(df.columns))
        print(df.head())
        
        return df

    else:
        print(f"Failed to fetch accounts: {response.status_code}")
        return pd.DataFrame()  # Return an empty DataFrame if there's an error

# Call df_accounts function with the parameter endpoint1 to get accounts data from JIRA API.
teams_df = df_teams(endpoint1)
print(teams_df)


# Initialize an empty list for teams and their members
teams_with_members = []

print("Initializing loop to get team members...")
# Loop through each team
for index, team in teams_df.iterrows():
    team_id = team['id']
    team_name = team['name']
    lead_id = team['leadId']
    team_members_link = team['teamLink']
    
    # Get team members using the 'team_members_link'
    members_response = requests.get(team_members_link, headers=headers)
    
    if members_response.status_code == 200:
        members_data = members_response.json()
        # Now you need to extract the member IDs from members_data.
        # This will depend on the structure of members_data.
        # Assuming it's a list of member details:
        for member in members_data['results']:
            # Extract the member ID from each member detail
            member_id = member['member']['accountId']
            # Append a dict with team and member info to the list

            active_membership = member['memberships'].get('active') if member['memberships'] else None
            member_from = active_membership['from'] if active_membership else None
            member_to = active_membership['to'] if active_membership else None

            teams_with_members.append({
                'teamId': team_id,
                'teamName': team_name,
                'leadId': lead_id,
                'memberId': member_id,
                'from': member_from,
                'to': member_to
            })
            
    else:
        print(f"Failed to fetch members for team {team_name}: {members_response.status_code}")
        
# Create a DataFrame from the list of dictionaries
teams_members_df = pd.DataFrame(teams_with_members)

print("Final Dataframe with Members (head):")
print(teams_members_df.head())


print("Initializing database creation...")
# Connect to the SQLite database
def setup_database():

    # Connection
    engine = create_engine('sqlite:///jiradatabase.db')
    Base = declarative_base()

    # Define the accounts table
    class Account(Base):
        __tablename__ = 'teams'
        id = Column(Integer, primary_key=True)
        name = Column(String)
        leadId = Column(String)
        teams_ids = Column(String)
                
    Base.metadata.create_all(engine)
    return engine

engine = setup_database()

table_name = 'teams'

print("Loading info into the database...")
teams_members_df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

# DEBUG PRINT
print(f"The DataFrame was successfully loaded into the table '{table_name}'.")

    