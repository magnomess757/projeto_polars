import polars as pl
import os
from dotenv import load_dotenv
import urllib.parse
from sqlalchemy import create_engine, event

load_dotenv()

# Busca as variaveis
db_user = os.getenv("DB_DW_USER")
db_pass = os.getenv("DB_DW_PASS")
db_host = os.getenv("DB_DW_HOST") 
db_name = os.getenv("DB_DW_NAME")

safe_pass = urllib.parse.quote_plus(db_pass)

# 1. Configuração da Engine SQL Server (MSSQL)
connection_uri = (
    f"mssql+pyodbc://{db_user}:{safe_pass}@{db_host}/{db_name}"
    "?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
)

engine = create_engine(connection_uri)

# 2. O Segredo da Performance (Fast Executemany)
# Sem isso, o write_database no SQL Server será lento para 300k linhas
@event.listens_for(engine, "before_cursor_execute")
def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
    if executemany:
        cursor.fast_executemany = True

def insert_data(df: pl.DataFrame, table: str, mode: str, schema: str = "raw"):

    
    # Validações
    if not isinstance(df, pl.DataFrame):
        raise ValueError("Isso dai nao é um dataFrame, Pae")
    
    if df.is_empty():
        raise ValueError("O dataFrame ta VAZIO.")

    validated_modes = ['append', 'replace'] 
    if mode not in validated_modes:
        raise ValueError(f"Modo inválido. Use: {validated_modes}")

    full_table_name = f"{schema}.{table}"
    print(f"Iniciando carga de {df.height} linhas em '{full_table_name}' no SQL Server...")

    try:
        # 3. Polars write_database usando a Engine configurada
        # Note que passamos o objeto 'engine' criado acima, não a string.
        df.write_database(
            table_name=full_table_name,        # SQL Server via SQLAlchemy lida melhor passando só a tabela
            connection=engine,       # Passamos a engine com o Turbo ativado
            if_table_exists=mode,     
            engine="sqlalchemy"      # Fallback necessário para MSSQL
        )
        
        print("Carga FINALIZADA com sucesso!")

    except Exception as e:
        print(f"Deu ruim na carga: {e}")
        raise e