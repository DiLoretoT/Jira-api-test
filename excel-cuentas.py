import pandas as pd
import sqlite3

# Dictionary to map Excel sheet names to the desired table names
sheet_to_table = {
    'Sheet1': 'dias_habiles',
    'Sheet2': 'subteams',
    'Sheet3': 'Cotización_USD'
}

# Define the path to your Excel file
excel_file_path = r'C:\Users\tdiloreto\Algeiba\InO Mgmt - Documents\Power BI Data input\ALG_Cuentas.xlsx'

# Connect to SQLite Database
conn = sqlite3.connect('jiradatabase.db') 

# Read the Excel file
xls = pd.ExcelFile(excel_file_path)

# Iterate through the sheet names in the Excel file
for sheet_name in xls.sheet_names:

    # Skip the 'Horas Facturadas'
    if sheet_name == 'Horas Facturadas':
        continue

    # Read the sheet into a DataFrame
    df = pd.read_excel(xls, sheet_name=sheet_name)

    # Replace blank cells with None
    #df.fillna(value=None, inplace=True)

    # Rename the sheet to the corresponding table name and load it into SQLite
    table_name = sheet_to_table.get(sheet_name, sheet_name)  # Fallback to the original sheet name if not found in the dictionary
    df.to_sql(table_name, conn, if_exists='replace', index=False)

# Close the connection
conn.close()