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
            teams_with_members.append({
                'teamId': team_id,
                'teamName': team_name,
                'leadId': lead_id,
                'memberId': member_id
            })
    else:
        print(f"Failed to fetch members for team {team_name}: {members_response.status_code}")
        
# Create a DataFrame from the list of dictionaries
teams_members_df = pd.DataFrame(teams_with_members)

print("Final Dataframe with Members (head):")
print(teams_members_df.head())



# # Initialize an empty list for project IDs
# members_id = []
# request_count = 0
# print("Initializing loop to get project ID from endpoint /accounts/{ACCOUNT ID}/links...")
# # Loop through each open account's link to fetch the project ID
# for teamLink in teams_df['teamLink']:
#     link_response = requests.get(teamLink, headers=headers)
#     request_count += 1
#     print(f"Iteration {request_count}: Status code {link_response.status_code}")
#     link_data = link_response.json()
    
#     # Check if 'results' is not empty and contains 'scope' with type "PROJECT"
#     if link_data['results'] and link_data['results'][0]['member']['accountId']:
#         teams_id = link_data['results'][0]['member']['accountId']
#         members_id.append(teams_id)
#     else:
#         members_id.append(None)  # Append None if no project ID is found or if the structure is unexpected

# # Assuming accounts_df is a copy or you're okay modifying it directly
# teams_df['teams_ids'] = members_id
# # Optionally, drop the 'accountLink' column if it's no longer needed
# print("Replacing columns...")
# teams_df.drop(columns=['teamLink'], inplace=True)

# print("Final Dataframe (head):")
# print(teams_df.head())

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

    