import requests
import pandas as pd
from datetime import datetime
from utils import read_api_credentials
from pathlib import Path

# Adjust these imports as necessary
import pytz

# AUTHENTICATION
api_credentials = read_api_credentials("config.ini", "api_jira")
api_token = api_credentials['token']
endpoint = 'worklogs'
start_date = '2024-02-01'
end_date = '2024-02-03'

params = {
    'from': start_date,
    'to': end_date
}

headers = {
    'Authorization': f'Bearer {api_token}'
}


#Getting and preparing dataframe
def df_worklogs(endpoint):
    api_url = f'https://api.tempo.io/core/3/{endpoint}'
    response = requests.get(api_url, headers=headers, params=params)
    print(f'Status code: {response.status_code}')
    
    if response.status_code == 200:
        data = response.json()
        # Use json_normalize to handle nested JSON
        if 'results' in data:
            df = pd.json_normalize(data['results'])
            print(f"Found {len(df)} worklogs")
        else:
            df = pd.DataFrame()  # or pd.DataFrame(data) if the response is already a flat dictionary
            print("No worklogs found.")
        return df
    else:
        print(f"Failed to fetch worklogs: {response.status_code}")
        return pd.DataFrame()  # Return an empty DataFrame if there's an error

worklogs_df = df_worklogs('worklogs')
print(worklogs_df)

