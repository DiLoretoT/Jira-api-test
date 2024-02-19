import requests
import sqlalchemy
import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import declarative_base
from utils import read_api_credentials, get_sqlite_db_connection, close_sqlite_db_connection

# AUTHENTICATION
api_credentials = read_api_credentials("config.ini", "api_tempo")
api_token = api_credentials['token']
endpoint = 'customers'

headers = {
    'Authorization': f'Bearer {api_token}'
}

# Getting and preparing dataframe
def df_customers(endpoint):
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
                selected_columns = ['id', 'key',  'name']
                df = df[selected_columns]
                                
                # Exclude self column if present
                df = df[df.columns[~df.columns.isin(['self'])]]
                print("Columns after Filter: ", list(df.columns))
                
            except KeyError as e:
                print(f"Column not found in the data: {e}")
           
            print(f"Found {len(df)} records")
            return df
            
        else:
            df = pd.DataFrame()  # or pd.DataFrame(data) if the response is already a flat dictionary
            print("No customers found.")
        return df
    else:
        print(f"Failed to fetch customers: {response.status_code}")
        return pd.DataFrame()  # Return an empty DataFrame if there's an error

# Call df_customers function with the parameter endpoint1 to get customers data from JIRA API.
customers_df = df_customers(endpoint)
customers_df = customers_df[customers_df.columns[~customers_df.columns.isin(['self'])]]

# Connect to the SQLite database
def setup_database():
    # Connection
    engine = create_engine('sqlite:///jiradatabase.db')
    Base = declarative_base()
    
    # Define the customers table
    class Customer(Base):
        __tablename__ = 'customers'
        id = Column(Integer, primary_key=True)
        key = Column(String)
        name = Column(String)
        
    Base.metadata.create_all(engine)
    return engine

engine = setup_database()

table_name = "customers"

customers_df.to_sql(name='customers', con=engine, if_exists='replace', index=False)

# DEBUG PRINT
print(f"The DataFrame was successfully loaded into the table '{table_name}'.")