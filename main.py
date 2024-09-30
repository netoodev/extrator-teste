import os
import fdb
import psycopg2
import pymysql
import threading
from supabase import create_client, Client
from dotenv import load_dotenv
import tkinter as tk
from tkinter import ttk, scrolledtext, messagebox
import logging
from datetime import datetime

# Carregando as variáveis de ambiente do arquivo .env (se necessário)
load_dotenv()

# Configuração de Logs
class GUIHandler(logging.Handler):
    def __init__(self, log_widget):
        super().__init__()
        self.log_widget = log_widget

    def emit(self, record):
        msg = self.format(record)
        def append():
            self.log_widget.configure(state='normal')
            self.log_widget.insert(tk.END, msg + '\n')
            self.log_widget.configure(state='disabled')
            self.log_widget.yview(tk.END)
        self.log_widget.after(0, append)

# Funções de Conexão com Bancos de Dados
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

# Extração dos Dados
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

def extract_sales_data(conn):
    query = """
        SELECT p.nome AS Produto, COUNT(v.cod_venda) AS Quantidade_Vendida, SUM(v.valor_total) AS Total_Vendas 
        FROM vendas v 
        JOIN produtos p ON v.cod_produto = p.cod 
        GROUP BY p.nome 
        ORDER BY Total_Vendas DESC;
    """
    return extract_data(conn, query)

# Conexão com Supabase
def connect_supabase(url, key):
    try:
        supabase = create_client(url, key)
        logging.info("Conectado ao Supabase com sucesso.")
        return supabase
    except Exception as e:
        logging.error(f"Erro ao conectar ao Supabase: {e}")
        return None

# Função para Upsert no Supabase
def upsert_to_supabase(supabase, table_name, data, on_conflict_column):
    if not data:
        logging.info("Nenhum dado para upsertar no Supabase.")
        return

    try:
        response = supabase.table(table_name).upsert(data, on_conflict=on_conflict_column).execute()
        # Verificar se há erros
        if hasattr(response, 'error') and response.error:
            logging.error(f"Erro ao upsertar dados: {response.error}")
        elif hasattr(response, 'status_code') and response.status_code not in [200, 201]:
            logging.error(f"Erro ao upsertar dados: Status Code {response.status_code}")
        else:
            logging.info("Dados upsertados com sucesso no Supabase.")
    except Exception as e:
        logging.error(f"Erro ao enviar dados para o Supabase: {e}")

# Converter Dados para Dicionários
def convert_to_dict(raw_data):
    data_dict = []
    for row in raw_data:
        try:
            data_dict.append({
                "Produto": row[0].strip(),  # Remover espaços em branco
                "Quantidade_Vendida": int(row[1]),
                "Total_Vendas": float(row[2])
            })
        except (ValueError, TypeError) as e:
            logging.error(f"Erro ao converter linha {row}: {e}")
    logging.info(f"Dados convertidos para o formato correto. Total de registros: {len(data_dict)}.")
    return data_dict

# Interface Gráfica com Tkinter
class App:
    def __init__(self, root):
        self.root = root
        self.root.title("Extrator e Upsert de Dados para Supabase")
        self.create_widgets()
        self.setup_logging()

    def create_widgets(self):
        # Tornar o tab_control uma variável de instância
        self.tab_control = ttk.Notebook(self.root)

        # Tab para Configuração do Banco de Dados
        self.db_tab = ttk.Frame(self.tab_control)
        self.tab_control.add(self.db_tab, text='Banco de Dados')
        self.create_db_tab()

        # Tab para Configuração do Supabase
        self.supabase_tab = ttk.Frame(self.tab_control)
        self.tab_control.add(self.supabase_tab, text='Supabase')
        self.create_supabase_tab()

        # Tab para Logs
        self.log_tab = ttk.Frame(self.tab_control)
        self.tab_control.add(self.log_tab, text='Logs')
        self.create_log_tab()

        self.tab_control.pack(expand=1, fill='both')

        # Botão de Execução
        execute_button = ttk.Button(self.root, text="Executar", command=self.run_process)
        execute_button.pack(pady=10)

    def create_db_tab(self):
        # Tipo de Banco de Dados
        ttk.Label(self.db_tab, text="Tipo de Banco de Dados:").grid(column=0, row=0, padx=10, pady=10, sticky='W')
        self.db_type = tk.StringVar()
        db_options = ["MySQL/MariaDB", "PostgreSQL", "Firebird"]
        self.db_dropdown = ttk.Combobox(self.db_tab, textvariable=self.db_type, values=db_options, state='readonly')
        self.db_dropdown.grid(column=1, row=0, padx=10, pady=10)
        self.db_dropdown.current(0)

        # Host
        ttk.Label(self.db_tab, text="Host:").grid(column=0, row=1, padx=10, pady=5, sticky='W')
        self.db_host = tk.StringVar(value=os.getenv("MYSQL_HOST", ""))
        self.host_entry = ttk.Entry(self.db_tab, textvariable=self.db_host, width=30)
        self.host_entry.grid(column=1, row=1, padx=10, pady=5)

        # Usuário
        ttk.Label(self.db_tab, text="Usuário:").grid(column=0, row=2, padx=10, pady=5, sticky='W')
        self.db_user = tk.StringVar(value=os.getenv("MYSQL_USER", ""))
        self.user_entry = ttk.Entry(self.db_tab, textvariable=self.db_user, width=30)
        self.user_entry.grid(column=1, row=2, padx=10, pady=5)

        # Senha
        ttk.Label(self.db_tab, text="Senha:").grid(column=0, row=3, padx=10, pady=5, sticky='W')
        self.db_password = tk.StringVar(value=os.getenv("MYSQL_PASSWORD", ""))
        self.password_entry = ttk.Entry(self.db_tab, textvariable=self.db_password, width=30, show='*')
        self.password_entry.grid(column=1, row=3, padx=10, pady=5)

        # Nome do Banco de Dados
        ttk.Label(self.db_tab, text="Nome do Banco de Dados:").grid(column=0, row=4, padx=10, pady=5, sticky='W')
        self.db_name = tk.StringVar(value=os.getenv("MYSQL_DATABASE", ""))
        self.dbname_entry = ttk.Entry(self.db_tab, textvariable=self.db_name, width=30)
        self.dbname_entry.grid(column=1, row=4, padx=10, pady=5)

    def create_supabase_tab(self):
        # URL do Supabase
        ttk.Label(self.supabase_tab, text="URL do Supabase:").grid(column=0, row=0, padx=10, pady=10, sticky='W')
        self.supabase_url = tk.StringVar(value=os.getenv("SUPABASE_URL", ""))
        self.url_entry = ttk.Entry(self.supabase_tab, textvariable=self.supabase_url, width=50)
        self.url_entry.grid(column=1, row=0, padx=10, pady=10)

        # Chave do Supabase
        ttk.Label(self.supabase_tab, text="Chave do Supabase:").grid(column=0, row=1, padx=10, pady=10, sticky='W')
        self.supabase_key = tk.StringVar(value=os.getenv("SUPABASE_KEY", ""))
        self.key_entry = ttk.Entry(self.supabase_tab, textvariable=self.supabase_key, width=50, show='*')
        self.key_entry.grid(column=1, row=1, padx=10, pady=10)

    def create_log_tab(self):
        self.log_text = scrolledtext.ScrolledText(self.log_tab, state='disabled', width=100, height=30)
        self.log_text.pack(padx=10, pady=10)

    def setup_logging(self):
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        gui_handler = GUIHandler(self.log_text)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        gui_handler.setFormatter(formatter)
        logger.addHandler(gui_handler)

    def run_process(self):
        # Desabilitar o botão de execução para evitar múltiplas execuções
        for widget in self.root.winfo_children():
            if isinstance(widget, ttk.Button) and widget['text'] == "Executar":
                widget.config(state='disabled')
        # Selecionar a aba de Logs
        self.tab_control.select(self.log_tab)
        # Executar o processo em uma thread separada
        thread = threading.Thread(target=self.process)
        thread.start()

    def process(self):
        try:
            # Obter os dados do GUI
            db_type = self.db_type.get()
            host = self.db_host.get()
            user = self.db_user.get()
            password = self.db_password.get()
            database = self.db_name.get()
            supabase_url = self.supabase_url.get()
            supabase_key = self.supabase_key.get()

            # Conectar ao Banco de Dados Selecionado
            if db_type == "MySQL/MariaDB":
                conn = connect_mysql(host, user, password, database)
            elif db_type == "PostgreSQL":
                conn = connect_postgresql(host, user, password, database)
            elif db_type == "Firebird":
                conn = connect_firebird(host, user, password, database)
            else:
                logging.error("Tipo de banco de dados inválido.")
                self.enable_execute_button()
                return

            if conn is None:
                logging.error("Falha na conexão com o banco de dados.")
                self.enable_execute_button()
                return

            # Extrair Dados
            raw_data = extract_sales_data(conn)
            if not raw_data:
                logging.warning("Nenhum dado foi extraído.")
                self.enable_execute_button()
                return

            # Converter Dados
            data_to_upload = convert_to_dict(raw_data)
            if not data_to_upload:
                logging.warning("Nenhum dado válido para enviar ao Supabase.")
                self.enable_execute_button()
                return

            # Conectar ao Supabase
            supabase = connect_supabase(supabase_url, supabase_key)
            if supabase is None:
                logging.error("Falha ao conectar ao Supabase.")
                self.enable_execute_button()
                return

            # Definir a coluna para conflito
            on_conflict_column = "Produto"  

            # Upsert no Supabase
            upsert_to_supabase(supabase, "sales_summary", data_to_upload, on_conflict_column)

            # Fechar conexão com o banco de dados
            conn.close()
            logging.info("Processo concluído com sucesso.")

        except Exception as e:
            logging.error(f"Ocorreu um erro inesperado: {e}")
        finally:
            self.enable_execute_button()

    def enable_execute_button(self):
        # Reabilitar o botão de execução
        for widget in self.root.winfo_children():
            if isinstance(widget, ttk.Button) and widget['text'] == "Executar":
                widget.config(state='normal')

# Função Principal
def main():
    root = tk.Tk()
    app = App(root)
    root.mainloop()

if __name__ == "__main__":
    main()
