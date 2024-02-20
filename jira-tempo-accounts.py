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
        
        # Use json_normalize to handle nested JSON
        if 'results' in data and len(data['results']) >0:
            
            # Prtin column names
            print("Original colums: ", list(data['results'][0].keys()))
            df = pd.json_normalize(data, record_path=['results'])
            print("Columns After Normalization:", list(df.columns))
            
            try:
                # Selecting columns
                selected_columns = ['id', 'key',  'name', 'status', 'lead.displayName', 'category.id', 'customer.name']
                df = df[selected_columns]
                rename_columns = {
                    'lead.displayName': 'lead',
                    'category.id': 'categoryId',
                    'customer.name': 'customer'
                }
                # Rename columns 
                df = df.rename(columns=rename_columns)
                
                # Exclude self column if present
                df = df[df.columns[~df.columns.isin(['self'])]]
                print("Columns after Filter and renaming: ", list(df.columns))
                
            except KeyError as e:
                print(f"Column not found in the data: {e}")
           
            print(f"Found {len(df)} records")
            return df
            
        else:
            df = pd.DataFrame()  # or pd.DataFrame(data) if the response is already a flat dictionary
            print("No accounts found.")
        return df
    else:
        print(f"Failed to fetch accounts: {response.status_code}")
        return pd.DataFrame()  # Return an empty DataFrame if there's an error

# Call df_accounts function with the parameter endpoint1 to get accounts data from JIRA API.
accounts_df = df_accounts(endpoint1)
accounts_df = accounts_df[accounts_df.columns[~accounts_df.columns.isin(['self'])]]

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
        categoryId = Column(Integer)
        customer = Column(String)
        lead = Column(String)
       
        
    Base.metadata.create_all(engine)
    return engine

engine = setup_database()

table_name = 'accounts'

accounts_df.to_sql(name='accounts', con=engine, if_exists='replace', index=False)

# DEBUG PRINT
print(f"The DataFrame was successfully loaded into the table '{table_name}'.")

    