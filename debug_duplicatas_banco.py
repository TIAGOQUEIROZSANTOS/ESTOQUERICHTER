import sqlite3
import pandas as pd
from datetime import date
import os

# Conectar ao banco (mesmo caminho usado no database.py)
ARQUIVO_DB = os.path.join(os.path.dirname(__file__), 'dados_sistema.db')
conn = sqlite3.connect(ARQUIVO_DB)

# Verificar linhas TOTAL no banco
query = """
    SELECT sku, tipo_movimento, data_movimento, saldo_apos, id, COUNT(*) as qtd
    FROM plenus_historico
    WHERE tipo_movimento IN ('TOTAL', 'TOTAL:', 'TOTAL ')
    GROUP BY sku, tipo_movimento, data_movimento
    HAVING COUNT(*) > 1
    ORDER BY sku, id DESC
"""
df_duplicatas = pd.read_sql(query, conn)

print(f"=== LINHAS TOTAL DUPLICADAS NO BANCO ===")
print(f"Total de grupos duplicados: {len(df_duplicatas)}")
if not df_duplicatas.empty:
    print("\nPrimeiras 20 duplicatas:")
    print(df_duplicatas.head(20).to_string())

# Verificar linhas TOTAL por SKU (mesmo SKU com múltiplas linhas TOTAL)
query_sku = """
    SELECT sku, COUNT(*) as qtd_total
    FROM plenus_historico
    WHERE tipo_movimento IN ('TOTAL', 'TOTAL:', 'TOTAL ')
    GROUP BY sku
    HAVING COUNT(*) > 1
    ORDER BY qtd_total DESC
"""
df_sku_duplicatas = pd.read_sql(query_sku, conn)

print(f"\n=== SKUs COM MÚLTIPLAS LINHAS TOTAL ===")
print(f"Total de SKUs com múltiplas linhas TOTAL: {len(df_sku_duplicatas)}")
if not df_sku_duplicatas.empty:
    print("\nPrimeiros 30 SKUs:")
    print(df_sku_duplicatas.head(30).to_string())

    # Mostrar detalhes de alguns SKUs
    print("\n=== DETALHES DE ALGUNS SKUs COM MÚLTIPLAS LINHAS TOTAL ===")
    for idx, row in df_sku_duplicatas.head(5).iterrows():
        sku = row['sku']
        query_detalhes = """
            SELECT id, sku, tipo_movimento, data_movimento, saldo_apos, arquivo_origem
            FROM plenus_historico
            WHERE sku = ? AND tipo_movimento IN ('TOTAL', 'TOTAL:', 'TOTAL ')
            ORDER BY id DESC
        """
        df_detalhes = pd.read_sql(query_detalhes, conn, params=(sku,))
        print(f"\nSKU {sku} - {row['qtd_total']} linhas TOTAL:")
        print(df_detalhes.to_string())

# Verificar total de linhas TOTAL no banco
query_total = """
    SELECT COUNT(*) as total, COUNT(DISTINCT sku) as skus_unicos
    FROM plenus_historico
    WHERE tipo_movimento IN ('TOTAL', 'TOTAL:', 'TOTAL ')
"""
df_stats = pd.read_sql(query_total, conn)
print(f"\n=== ESTATÍSTICAS GERAIS ===")
print(f"Total de linhas TOTAL no banco: {df_stats.iloc[0]['total']}")
print(f"Total de SKUs únicos com linha TOTAL: {df_stats.iloc[0]['skus_unicos']}")
print(f"Diferença (duplicatas): {df_stats.iloc[0]['total'] - df_stats.iloc[0]['skus_unicos']}")

# Calcular soma de saldos (com e sem remoção de duplicatas)
query_soma_com_dup = """
    SELECT SUM(saldo_apos) as soma_total
    FROM plenus_historico
    WHERE tipo_movimento IN ('TOTAL', 'TOTAL:', 'TOTAL ')
"""
df_soma_com = pd.read_sql(query_soma_com_dup, conn)
print(f"\nSoma de TODAS as linhas TOTAL (com duplicatas): {df_soma_com.iloc[0]['soma_total']:.4f}")

# Soma sem duplicatas (uma linha TOTAL por SKU, a mais recente)
query_soma_sem_dup = """
    SELECT SUM(saldo_apos) as soma_total
    FROM (
        SELECT sku, saldo_apos, ROW_NUMBER() OVER (PARTITION BY sku ORDER BY id DESC) as rn
        FROM plenus_historico
        WHERE tipo_movimento IN ('TOTAL', 'TOTAL:', 'TOTAL ')
    )
    WHERE rn = 1
"""
df_soma_sem = pd.read_sql(query_soma_sem_dup, conn)
print(f"Soma de linhas TOTAL sem duplicatas (uma por SKU): {df_soma_sem.iloc[0]['soma_total']:.4f}")
print(f"Esperado: 18.200,8888")
print(f"Diferença: {abs(df_soma_sem.iloc[0]['soma_total'] - 18200.8888):.4f}")

conn.close()
