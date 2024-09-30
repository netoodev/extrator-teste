import os
import fdb
import psycopg2
import pymysql
from supabase import create_client, Client
from dotenv import load_dotenv

# Carregando as variáveis de ambiente do arquivo .env
load_dotenv()

# Configuração de Logs
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Conexão com MySQL
def connect_mysql(host, user, password, database):
    try:
        connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        logging.info("Conectado ao MySQL/MariaDB com sucesso.")
        return connection
    except pymysql.MySQLError as e:
        logging.error(f"Erro na conexão com MySQL/MariaDB: {e}")
        return None

# Conexão com PostgreSQL
def connect_postgresql(host, user, password, database):
    try:
        connection = psycopg2.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        logging.info("Conectado ao PostgreSQL com sucesso.")
        return connection
    except psycopg2.Error as e:
        logging.error(f"Erro na conexão com PostgreSQL: {e}")
        return None

# Conexão com Firebird
def connect_firebird(host, user, password, database):
    try:
        connection = fdb.connect(
            host=host,
            database=database,
            user=user,
            password=password
        )
        logging.info("Conectado ao Firebird com sucesso.")
        return connection
    except fdb.Error as e:
        logging.error(f"Erro na conexão com Firebird: {e}")
        return None

# Seleção do banco de dados
def choose_database():
    print("Escolha o banco de dados:")
    print("1. MySQL/MariaDB")
    print("2. PostgreSQL")
    print("3. Firebird")
    try:
        choice = int(input("Digite o número da escolha: "))
    except ValueError:
        logging.error("Entrada inválida. Por favor, digite um número entre 1 e 3.")
        return None

    if choice == 1:
        return connect_mysql(
            os.getenv("MYSQL_HOST", "localhost"),
            os.getenv("MYSQL_USER", "root"),
            os.getenv("MYSQL_PASSWORD", "password"),
            os.getenv("MYSQL_DATABASE", "banco_farmacia")
        )
    elif choice == 2:
        return connect_postgresql(
            os.getenv("POSTGRES_HOST", "localhost"),
            os.getenv("POSTGRES_USER", "postgres"),
            os.getenv("POSTGRES_PASSWORD", "password"),
            os.getenv("POSTGRES_DATABASE", "banco_farmacia_pg")
        )
    elif choice == 3:
        return connect_firebird(
            os.getenv("FIREBIRD_HOST", "localhost"),
            os.getenv("FIREBIRD_USER", "SYSDBA"),
            os.getenv("FIREBIRD_PASSWORD", "masterkey"),
            os.getenv("FIREBIRD_DATABASE", "banco_farmacia.fdb")
        )
    else:
        logging.error("Escolha inválida.")
        return None

# Extração dos dados
def extract_data(connection, query):
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()
        logging.info(f"Consulta executada com sucesso. {len(data)} registros extraídos.")
        return data
    except Exception as e:
        logging.error(f"Erro ao executar a consulta: {e}")
        return []

# Consulta de total vendas por produto
def extract_sales_data(conn):
    query = """
        SELECT p.nome AS Produto, COUNT(v.cod_venda) AS Quantidade_Vendida, SUM(v.valor_total) AS Total_Vendas 
        FROM vendas v 
        JOIN produtos p ON v.cod_produto = p.cod 
        GROUP BY p.nome 
        ORDER BY Total_Vendas DESC;
    """
    return extract_data(conn, query)

# Conectando ao Supabase
def connect_supabase(url, key):
    try:
        supabase = create_client(url, key)
        logging.info("Conectado ao Supabase com sucesso.")
        return supabase
    except Exception as e:
        logging.error(f"Erro ao conectar ao Supabase: {e}")
        return None

# Função para upsert dados no Supabase
def upsert_to_supabase(supabase, table_name, data):
    if not data:
        logging.info("Nenhum dado para upsertar no Supabase.")
        return
    try:
        response = supabase.table(table_name).upsert(data, on_conflict="Produto").execute()
        error = getattr(response, 'error', None)
        if error is None:
            logging.info("Dados upsertados com sucesso no Supabase.")
        else:
            logging.error(f"Erro ao upsertar dados: {error}")
    except Exception as e:
        logging.error(f"Erro ao enviar dados para o Supabase: {e}")

# Converter dados para dicionários
def convert_to_dict(raw_data):
    data_dict = []
    for row in raw_data:
        try:
            data_dict.append({
                "Produto": row[0],
                "Quantidade_Vendida": int(row[1]),
                "Total_Vendas": float(row[2])
            })
        except (ValueError, TypeError) as e:
            logging.error(f"Erro ao converter linha {row}: {e}")
    logging.info(f"Dados convertidos para o formato correto. Total de registros: {len(data_dict)}.")
    return data_dict

def main():
    try:
        # Conectar ao banco de dados escolhido
        conn = choose_database()
        if conn is None:
            logging.error("Falha na conexão com o banco de dados.")
            return

        # Extrair e tratar os dados
        raw_data = extract_sales_data(conn)
        if not raw_data:
            logging.warning("Nenhum dado foi extraído.")
            return

        # Converter dados para o formato adequado
        data_to_upload = convert_to_dict(raw_data)
        if not data_to_upload:
            logging.warning("Nenhum dado válido para enviar ao Supabase.")
            return

        # Conectar ao Supabase
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        if not url or not key:
            logging.error("URL ou chave do Supabase não estão definidas nas variáveis de ambiente.")
            return

        supabase = connect_supabase(url, key)
        if supabase is None:
            logging.error("Falha ao conectar ao Supabase.")
            return

        # Upsert dados no Supabase
        upsert_to_supabase(supabase, "sales_summary", data_to_upload)

    except Exception as e:
        logging.error(f"Ocorreu um erro inesperado: {e}")

if __name__ == "__main__":
    main()
