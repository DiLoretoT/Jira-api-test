import pandas as pd
import sqlite3
import requests
from datetime import datetime
from pathlib import Path
from configparser import ConfigParser


def get_sqlite_db_connection(database_path='jiradatabase.db'):
    """
    Create the SQLite database connection.
    """
    conn = sqlite3.connect(database_path)
    return conn

def close_sqlite_db_connection(conn):
    """
    Close the SQLite database connection.
    """
    conn.close()

def read_api_credentials(file_path: Path, section: str) -> dict:
    """
    Lee las credenciales de la api desde el archivo "config.ini"
    
    args: 
        file_path: ruta del archivo de configuración
        section: sección del archivo con la información requerida
        
    Return:
        token de la API para construir el connection string
    """
    config = ConfigParser()
    config.read(file_path)
    print(f"Sections available: {config.sections()}")
    if section in config:
        api_credentials = dict(config[section])
        return api_credentials
    else: 
        raise ValueError(f"Section {section} not found in config file.")
    


