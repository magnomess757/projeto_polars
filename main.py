import polars as pl
import os
from dotenv import load_dotenv
import urllib.parse
from loader import insert_data

load_dotenv()

db_user = os.getenv("DB_DW_USER")
db_pass = os.getenv("DB_DW_PASS")
db_host = os.getenv("DB_DW_HOST") 
db_name = os.getenv("DB_STG_NAME")


safe_pass = urllib.parse.quote_plus(db_pass)

connection_uri = f"mssql://{db_user}:{safe_pass}@{db_host}/{db_name}?encrypt=false"

query = """
    SELECT *
    FROM stg_minerva_bonds
"""

print(f"Tentando conectar em: {db_host}...")

try:
    
    df = pl.read_database_uri(query=query, uri=connection_uri)

    insert_data(
        df,
        "bonds_test",
        "replace"      
    )

    
except Exception as e:
    print(f"Erro: {e}")