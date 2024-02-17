import pandas as pd
import requests
from datetime import datetime
from pathlib import Path
from configparser import ConfigParser


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
    print(config.sections())
    api_credentials = dict(config[section])
    return api_credentials


