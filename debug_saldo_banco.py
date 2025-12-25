import sqlite3
import pandas as pd
from datetime import date
import os

# Conectar ao banco
ARQUIVO_DB = os.path.join(os.path.dirname(__file__), 'dados_sistema.db')
conn = sqlite3.connect(ARQUIVO_DB)

# Verificar linhas TOTAL no banco
query_total = """
    SELECT COUNT(*) as total_linhas, COUNT(DISTINCT sku) as skus_unicos
    FROM plenus_historico
    WHERE tipo_movimento IN ('TOTAL', 'TOTAL:', 'TOTAL ')
"""
df_stats = pd.read_sql(query_total, conn)
print(f"=== ESTATÍSTICAS DE LINHAS TOTAL NO BANCO ===")
print(f"Total de linhas TOTAL: {df_stats.iloc[0]['total_linhas']}")
print(f"Total de SKUs únicos: {df_stats.iloc[0]['skus_unicos']}")

# Verificar se há duplicatas por SKU
query_dup = """
    SELECT sku, COUNT(*) as qtd
    FROM plenus_historico
    WHERE tipo_movimento IN ('TOTAL', 'TOTAL:', 'TOTAL ')
    GROUP BY sku
    HAVING COUNT(*) > 1
    ORDER BY qtd DESC
"""
df_duplicatas = pd.read_sql(query_dup, conn)
print(f"\n=== SKUs COM MÚLTIPLAS LINHAS TOTAL ===")
print(f"Total de SKUs com duplicatas: {len(df_duplicatas)}")
if not df_duplicatas.empty:
    print("\nPrimeiros 20 SKUs com duplicatas:")
    print(df_duplicatas.head(20).to_string())

# Calcular soma de saldos (com e sem remoção de duplicatas)
query_soma_com = """
    SELECT SUM(saldo_apos) as soma_total
    FROM plenus_historico
    WHERE tipo_movimento IN ('TOTAL', 'TOTAL:', 'TOTAL ')
"""
df_soma_com = pd.read_sql(query_soma_com, conn)
soma_com_dup = df_soma_com.iloc[0]['soma_total'] if df_soma_com.iloc[0]['soma_total'] else 0.0
print(f"\n=== SOMA DE SALDOS ===")
print(f"Soma de TODAS as linhas TOTAL (com duplicatas): {soma_com_dup:.4f}")

# Soma sem duplicatas (uma linha TOTAL por SKU, a mais recente)
query_soma_sem = """
    SELECT SUM(saldo_apos) as soma_total
    FROM (
        SELECT sku, saldo_apos, ROW_NUMBER() OVER (PARTITION BY sku ORDER BY id DESC) as rn
        FROM plenus_historico
        WHERE tipo_movimento IN ('TOTAL', 'TOTAL:', 'TOTAL ')
    )
    WHERE rn = 1
"""
df_soma_sem = pd.read_sql(query_soma_sem, conn)
soma_sem_dup = df_soma_sem.iloc[0]['soma_total'] if df_soma_sem.iloc[0]['soma_total'] else 0.0
print(f"Soma de linhas TOTAL sem duplicatas (uma por SKU): {soma_sem_dup:.4f}")
print(f"Esperado: 18.200,8888")
print(f"Diferença: {abs(soma_sem_dup - 18200.8888):.4f}")

# Verificar linhas TOTAL por data
query_por_data = """
    SELECT
        substr(data_movimento, 1, 10) as data,
        COUNT(*) as qtd_linhas,
        COUNT(DISTINCT sku) as qtd_skus,
        SUM(saldo_apos) as soma_saldo
    FROM plenus_historico
    WHERE tipo_movimento IN ('TOTAL', 'TOTAL:', 'TOTAL ')
    GROUP BY substr(data_movimento, 1, 10)
    ORDER BY data DESC
    LIMIT 20
"""
df_por_data = pd.read_sql(query_por_data, conn)
print(f"\n=== LINHAS TOTAL POR DATA (últimas 20 datas) ===")
if not df_por_data.empty:
    print(df_por_data.to_string())
else:
    print("Nenhuma linha encontrada")

# Verificar se há linhas TOTAL sem data
query_sem_data = """
    SELECT COUNT(*) as qtd
    FROM plenus_historico
    WHERE tipo_movimento IN ('TOTAL', 'TOTAL:', 'TOTAL ')
    AND (data_movimento IS NULL OR data_movimento = '')
"""
df_sem_data = pd.read_sql(query_sem_data, conn)
qtd_sem_data = df_sem_data.iloc[0]['qtd']
print(f"\n=== LINHAS TOTAL SEM DATA ===")
print(f"Total de linhas TOTAL sem data: {qtd_sem_data}")

# Verificar período de dados no banco
query_periodo = """
    SELECT
        MIN(substr(data_movimento, 1, 10)) as data_min,
        MAX(substr(data_movimento, 1, 10)) as data_max
    FROM plenus_historico
    WHERE tipo_movimento IN ('TOTAL', 'TOTAL:', 'TOTAL ')
    AND data_movimento IS NOT NULL
    AND data_movimento != ''
"""
df_periodo = pd.read_sql(query_periodo, conn)
if not df_periodo.empty and df_periodo.iloc[0]['data_min']:
    print(f"\n=== PERÍODO DOS DADOS ===")
    print(f"Data mínima: {df_periodo.iloc[0]['data_min']}")
    print(f"Data máxima: {df_periodo.iloc[0]['data_max']}")

# Simular o que acontece ao carregar do histórico (usando a função do database.py)
print(f"\n=== SIMULANDO CARREGAMENTO DO HISTÓRICO ===")
from database import carregar_plenus_movimento_db
from datetime import datetime

# Pegar data mínima e máxima do banco
query_datas = """
    SELECT
        MIN(substr(data_movimento, 1, 10)) as data_min,
        MAX(substr(data_movimento, 1, 10)) as data_max
    FROM plenus_historico
    WHERE data_movimento IS NOT NULL
    AND data_movimento != ''
"""
df_datas = pd.read_sql(query_datas, conn)
if not df_datas.empty and df_datas.iloc[0]['data_min']:
    dt_min = datetime.strptime(df_datas.iloc[0]['data_min'], '%Y-%m-%d').date()
    dt_max = datetime.strptime(df_datas.iloc[0]['data_max'], '%Y-%m-%d').date()

    print(f"Carregando dados de {dt_min} até {dt_max}")
    df_carregado = carregar_plenus_movimento_db(dt_min, dt_max)

    if not df_carregado.empty:
        # Verificar linhas TOTAL no DataFrame carregado
        if 'tipo_movimento' in df_carregado.columns:
            mask_total = df_carregado['tipo_movimento'].astype(str).str.strip().str.upper().isin(['TOTAL', 'TOTAL:', 'TOTAL '])
            df_totais_carregado = df_carregado[mask_total].copy()

            print(f"\nLinhas TOTAL no DataFrame carregado: {len(df_totais_carregado)}")
            print(f"SKUs únicos com linha TOTAL: {df_totais_carregado['sku'].nunique() if 'sku' in df_totais_carregado.columns else 0}")

            # Remover duplicatas por SKU (como faz a função calcular_total_saldo)
            if 'sku' in df_totais_carregado.columns:
                df_totais_sem_dup = df_totais_carregado.drop_duplicates(subset=['sku'], keep='last')
                print(f"Linhas TOTAL após remover duplicatas por SKU: {len(df_totais_sem_dup)}")

                # Calcular soma
                if 'saldo_apos' in df_totais_sem_dup.columns:
                    df_totais_sem_dup['saldo_apos'] = pd.to_numeric(df_totais_sem_dup['saldo_apos'], errors='coerce').fillna(0.0)
                    soma_carregado = float(df_totais_sem_dup['saldo_apos'].sum())
                    print(f"Soma de saldos após carregar e remover duplicatas: {soma_carregado:.4f}")
                    print(f"Esperado: 18.200,8888")
                    print(f"Diferença: {abs(soma_carregado - 18200.8888):.4f}")

conn.close()
