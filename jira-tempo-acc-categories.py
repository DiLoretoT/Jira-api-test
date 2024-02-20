import requests
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base
from utils import read_api_credentials, get_sqlite_db_connection, close_sqlite_db_connection

# AUTHENTICATION
api_credentials = read_api_credentials("config.ini", "api_tempo")
api_token = api_credentials['token']
endpoint1 = 'account-categories'

headers = {
    'Authorization': f'Bearer {api_token}'
}

# Getting and preparing dataframe
def df_accountcategories(endpoint):
    api_url = f'https://api.tempo.io/core/3/{endpoint}'
    response = requests.get(api_url, headers=headers)
    print(f'Status code: {response.status_code}')
    
    if response.status_code == 200:
        data = response.json()
        
        print("Original colums: ", list(data['results'][0].keys()))
        df = pd.json_normalize(data, record_path=['results'])
        print("Columns After Normalization:", list(df.columns))

        try:
            # Selecting columns
            selected_columns = ['id', 'name', 'type.name']
            df = df[selected_columns]
            
            print("Columns after Filter and renaming: ", list(df.columns))
            
        except KeyError as e:
            print(f"Column not found in the data: {e}")

        print(f"Found {len(df)} records")
        return df
            
    else:
        print(f"Failed to fetch account-categories: {response.status_code}")
        return pd.DataFrame()  # Return an empty DataFrame if there's an error

# Call df_account-categories function with the parameter endpoint1 to get account-categories data from JIRA API.
accountCategories_df = df_accountcategories(endpoint1)

print(accountCategories_df)

# Connect to the SQLite database
def setup_database():
    # Connection
    engine = create_engine('sqlite:///jiradatabase.db')
    Base = declarative_base()
    
    # Define the account-categories table
    class AccountCategories(Base):
        __tablename__ = 'accountCategories'
        id = Column(Integer, primary_key=True)
        key = Column(String)
        name = Column(String)
        status = Column(String)
        category = Column(String)
        customer = Column(String)
        lead = Column(String)
       
        
    Base.metadata.create_all(engine)
    return engine

engine = setup_database()

table_name = 'accountCategories'

accountCategories_df.to_sql(name='accountCategories', con=engine, if_exists='replace', index=False)

# DEBUG PRINT
print(f"The DataFrame was successfully loaded into the table '{table_name}'.")

    