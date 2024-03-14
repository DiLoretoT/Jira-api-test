import requests
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base
from utils import read_api_credentials, get_sqlite_db_connection, close_sqlite_db_connection

# AUTHENTICATION
api_credentials = read_api_credentials("config.ini", "api_tempo")
api_token = api_credentials['token']
endpoint1 = 'accounts'

headers = {
    'Authorization': f'Bearer {api_token}'
}

# Getting and preparing dataframe
def df_accounts(endpoint):
    api_url = f'https://api.tempo.io/core/3/{endpoint}'
    response = requests.get(api_url, headers=headers)
    print(f'Status code: {response.status_code}')
    
    if response.status_code == 200:
        data = response.json()
        df = pd.json_normalize(data['results'])
        #print("Columns After Normalization:", list(df.columns))
        
        # Define columns of interest, including the original 'links.self'
        selected_columns = ['id', 'key', 'name', 'status', 'lead.accountId', 'category.id', 'customer.id', 'links.self']
        df = df[selected_columns]
        
        # Rename columns for clarity
        rename_columns = {
            'lead.accountId': 'leadId',
            'category.id': 'categoryId',
            'links.self': 'accountLink'  # Use 'accountLink' for clarity
        }

        df = df.rename(columns=rename_columns)
        #print("Columns after Filter and Renaming: ", list(df.columns))
        #print(df.head())
        
        return df

    else:
        print(f"Failed to fetch accounts: {response.status_code}")
        return pd.DataFrame()  # Return an empty DataFrame if there's an error

# Call df_accounts function with the parameter endpoint1 to get accounts data from JIRA API.
accounts_df = df_accounts(endpoint1)
print(accounts_df.head())

#accounts_df = accounts_df[accounts_df['status'] == "OPEN"].head(5)
#accounts_df = accounts_df[(accounts_df['status'] == "OPEN")]

# Initialize an empty list for project IDs
project_ids = []
request_count = 0

print("Initializing loop to get project ID from endpoint /accounts/{ACCOUNT ID}/links...")
# Loop through each open account's link to fetch the project ID
for account_link in accounts_df['accountLink']:
    link_response = requests.get(account_link, headers=headers)
    request_count += 1
    print(f"Iteration {request_count}: Status code {link_response.status_code}")
    link_data = link_response.json()
    
    # Check if 'results' is not empty and contains 'scope' with type "PROJECT"
    if link_data['results'] and link_data['results'][0]['scope']['type'] == "PROJECT":
        project_id = link_data['results'][0]['scope']['id']
        project_ids.append(project_id)
    else:
        project_ids.append(None)  # Append None if no project ID is found or if the structure is unexpected

# Assuming accounts_df is a copy or you're okay modifying it directly
accounts_df['projectId'] = project_ids
# Optionally, drop the 'accountLink' column if it's no longer needed
print("Replacing columns...")
accounts_df.drop(columns=['accountLink'], inplace=True)

print("Final Dataframe (head):")
print(accounts_df.head())

print("Initializing database creation...")
# Connect to the SQLite database
def setup_database():
    # Connection
    engine = create_engine('sqlite:///jiradatabase.db')
    Base = declarative_base()
    
    # Define the accounts table
    class Account(Base):
        __tablename__ = 'accounts'
        id = Column(Integer, primary_key=True)
        key = Column(String)
        name = Column(String)
        status = Column(String)
        leadId = Column(String)
        categoryId = Column(Integer)
        customerId = Column(String)
        projectId = Column(Integer)
                
    Base.metadata.create_all(engine)
    return engine

engine = setup_database()

table_name = 'accounts'

print("Loading info into the database...")
accounts_df.to_sql(name='accounts', con=engine, if_exists='replace', index=False)
# DEBUG PRINT
print(f"The DataFrame was successfully loaded into the table '{table_name}'.")

    