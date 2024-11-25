import pandas as pd
import psycopg2
from psycopg2 import sql

# Carregar dados do CSV
data = pd.read_csv('dados_despesa.csv')  # Substitua pelo caminho real do seu arquivo CSV

# Configurações para a conexão com o banco de dados PostgreSQL
conn = psycopg2.connect(
    dbname="despesas",  # Nome do seu banco de dados
    user="postgres",      # Usuário do banco de dados
    password="1234",    # Senha do banco de dados
    host="localhost",        # Host do banco, altere se necessário
    port="5432"              # Porta padrão do PostgreSQL
)
cursor = conn.cursor()

# Criar tabelas para armazenar resultados das análises
# 1. Total de despesas por categoria (Ex: Materiais, Internet, etc.)
create_table_query_category = '''
CREATE TABLE IF NOT EXISTS total_por_categoria (
    despesa TEXT PRIMARY KEY,
    total_valor REAL
)
'''
cursor.execute(create_table_query_category)

# 2. Total de despesas por mês
create_table_query_month = '''
CREATE TABLE IF NOT EXISTS total_por_mes (
    ano INTEGER,
    mes TEXT,
    total_valor REAL,
    PRIMARY KEY (ano, mes)
)
'''
cursor.execute(create_table_query_month)

# 3. Total de despesas anual
create_table_query_year = '''
CREATE TABLE IF NOT EXISTS total_por_ano (
    ano INTEGER PRIMARY KEY,
    total_valor REAL
)
'''
cursor.execute(create_table_query_year)

# Commit das tabelas criadas
conn.commit()

# Funções de Análise e Inserção no Banco de Dados
def analyze_and_save(dataframe):
    # Análise 1: Total de despesas por categoria
    total_por_categoria = dataframe.groupby('despesa')['valor_despesa'].sum().reset_index()
    for _, row in total_por_categoria.iterrows():
        insert_query = '''
        INSERT INTO total_por_categoria (despesa, total_valor)
        VALUES (%s, %s)
        ON CONFLICT (despesa) DO UPDATE SET total_valor = EXCLUDED.total_valor
        '''
        cursor.execute(insert_query, (row['despesa'], row['valor_despesa']))

    # Análise 2: Total de despesas por mês e ano
    total_por_mes = dataframe.groupby(['ano', 'mes'])['valor_despesa'].sum().reset_index()
    for _, row in total_por_mes.iterrows():
        insert_query = '''
        INSERT INTO total_por_mes (ano, mes, total_valor)
        VALUES (%s, %s, %s)
        ON CONFLICT (ano, mes) DO UPDATE SET total_valor = EXCLUDED.total_valor
        '''
        cursor.execute(insert_query, (row['ano'], row['mes'], row['valor_despesa']))

    # Análise 3: Total de despesas por ano
    total_por_ano = dataframe.groupby('ano')['valor_despesa'].sum().reset_index()
    for _, row in total_por_ano.iterrows():
        insert_query = '''
        INSERT INTO total_por_ano (ano, total_valor)
        VALUES (%s, %s)
        ON CONFLICT (ano) DO UPDATE SET total_valor = EXCLUDED.total_valor
        '''
        cursor.execute(insert_query, (row['ano'], row['valor_despesa']))

    # Commit das inserções
    conn.commit()

# Executar as análises e salvar no banco de dados
analyze_and_save(data)

# Fechar a conexão
cursor.close()
conn.close()
