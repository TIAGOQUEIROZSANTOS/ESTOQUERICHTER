"""
Script para debugar produtos sem movimentação
"""
import sqlite3
import pandas as pd
from datetime import datetime, date

ARQUIVO_DB = "dados_sistema.db"

def get_db_connection():
    return sqlite3.connect(ARQUIVO_DB)

# Verificar AMAPA especificamente
conn = get_db_connection()

print("=" * 80)
print("DEBUG - PRODUTOS SEM MOVIMENTACAO")
print("=" * 80)

# 1. Verificar se AMAPA existe no banco
print("\n[1] Verificando AMAPA (SKU 001002) no banco:")
query_amapa = """
    SELECT sku, produto, categoria, tipo_movimento, data_movimento, saldo_apos, entrada, saida
    FROM plenus_historico
    WHERE sku = '001002'
    ORDER BY data_movimento DESC, id DESC
    LIMIT 10
"""
df_amapa = pd.read_sql(query_amapa, conn)
print(f"Total de registros do AMAPA no banco: {len(df_amapa)}")
if not df_amapa.empty:
    print("\nRegistros do AMAPA:")
    for idx, row in df_amapa.iterrows():
        print(f"  - Tipo: {row['tipo_movimento']} | Data: {row['data_movimento']} | Saldo: {row['saldo_apos']}")
else:
    print("  [ERRO] AMAPA nao encontrado no banco!")

# 2. Verificar linhas TOTAL/ANTERIOR de todos os produtos
print("\n[2] Verificando linhas TOTAL/ANTERIOR no banco:")
query_totais = """
    SELECT sku, produto, categoria, tipo_movimento, data_movimento, saldo_apos
    FROM plenus_historico
    WHERE tipo_movimento IN ('TOTAL', 'TOTAL:', 'ANTERIOR', 'ANTERIOR:')
    ORDER BY sku, tipo_movimento
"""
df_totais = pd.read_sql(query_totais, conn)
print(f"Total de linhas TOTAL/ANTERIOR no banco: {len(df_totais)}")
print(f"SKUs unicos com TOTAL/ANTERIOR: {len(df_totais['sku'].unique())}")

# Verificar AMAPA especificamente
amapa_totais = df_totais[df_totais['sku'] == '001002']
if not amapa_totais.empty:
    print("\nLinhas TOTAL/ANTERIOR do AMAPA:")
    for idx, row in amapa_totais.iterrows():
        print(f"  - Tipo: {row['tipo_movimento']} | Data: {row['data_movimento']} | Saldo: {row['saldo_apos']}")
else:
    print("\n  [ERRO] AMAPA nao tem linhas TOTAL/ANTERIOR no banco!")

# 3. Testar a query de carregamento (periodo 01/01/2024 a 31/01/2025)
print("\n[3] Testando carregamento para periodo 01/01/2024 a 31/01/2025:")
dt_ini = date(2024, 1, 1)
dt_fim = date(2025, 1, 31)

# Movimentacoes no periodo
query_mov = """
    SELECT * FROM plenus_historico
    WHERE substr(data_movimento,1,10) BETWEEN ? AND ?
"""
params_mov = [dt_ini.strftime("%Y-%m-%d"), dt_fim.strftime("%Y-%m-%d")]
df_mov = pd.read_sql(query_mov, conn, params=params_mov)
print(f"Movimentacoes no periodo: {len(df_mov)}")
print(f"SKUs com movimentacao: {len(df_mov['sku'].unique()) if not df_mov.empty else 0}")

# Verificar se AMAPA esta nas movimentacoes
amapa_mov = df_mov[df_mov['sku'] == '001002'] if not df_mov.empty else pd.DataFrame()
if not amapa_mov.empty:
    print(f"\nAMAPA tem {len(amapa_mov)} movimentacoes no periodo")
else:
    print("\nAMAPA NAO tem movimentacoes no periodo")

# Linhas TOTAL/ANTERIOR (todas)
query_totais_all = """
    SELECT * FROM plenus_historico
    WHERE tipo_movimento IN ('TOTAL', 'TOTAL:', 'ANTERIOR', 'ANTERIOR:')
"""
df_totais_all = pd.read_sql(query_totais_all, conn)
print(f"\nLinhas TOTAL/ANTERIOR (todas): {len(df_totais_all)}")
print(f"SKUs com TOTAL/ANTERIOR: {len(df_totais_all['sku'].unique()) if not df_totais_all.empty else 0}")

# Verificar se AMAPA esta nas linhas TOTAL/ANTERIOR
amapa_totais_all = df_totais_all[df_totais_all['sku'] == '001002'] if not df_totais_all.empty else pd.DataFrame()
if not amapa_totais_all.empty:
    print(f"\nAMAPA tem {len(amapa_totais_all)} linhas TOTAL/ANTERIOR")
    for idx, row in amapa_totais_all.iterrows():
        print(f"  - Tipo: {row['tipo_movimento']} | Data: {row['data_movimento']} | Saldo: {row['saldo_apos']}")
else:
    print("\n[ERRO] AMAPA NAO tem linhas TOTAL/ANTERIOR no banco!")

# Combinar (como faz a funcao)
if df_mov.empty:
    df_combined = df_totais_all.copy()
elif df_totais_all.empty:
    df_combined = df_mov.copy()
else:
    df_combined = pd.concat([df_mov, df_totais_all], ignore_index=True)
    df_combined = df_combined.drop_duplicates(subset=['sku', 'tipo_movimento', 'data_movimento'], keep='first')

print(f"\nResultado combinado: {len(df_combined)} linhas")
print(f"SKUs unicos no resultado: {len(df_combined['sku'].unique()) if not df_combined.empty else 0}")

# Verificar se AMAPA esta no resultado combinado
amapa_combined = df_combined[df_combined['sku'] == '001002'] if not df_combined.empty else pd.DataFrame()
if not amapa_combined.empty:
    print(f"\n[OK] AMAPA esta no resultado combinado com {len(amapa_combined)} linhas")
    for idx, row in amapa_combined.iterrows():
        print(f"  - Tipo: {row['tipo_movimento']} | Data: {row['data_movimento']} | Saldo: {row['saldo_apos']}")
else:
    print("\n[ERRO] AMAPA NAO esta no resultado combinado!")

# 4. Verificar todos os SKUs
print("\n[4] Verificando todos os SKUs no banco:")
query_all_skus = """
    SELECT DISTINCT sku, produto, categoria
    FROM plenus_historico
    WHERE sku IS NOT NULL
    ORDER BY sku
"""
df_all_skus = pd.read_sql(query_all_skus, conn)
print(f"Total de SKUs unicos no banco: {len(df_all_skus)}")

# Verificar quantos SKUs tem linhas TOTAL/ANTERIOR
skus_com_totais = set(df_totais_all['sku'].unique()) if not df_totais_all.empty else set()
print(f"SKUs com linhas TOTAL/ANTERIOR: {len(skus_com_totais)}")

# Verificar quantos SKUs tem movimentacao no periodo
skus_com_mov = set(df_mov['sku'].unique()) if not df_mov.empty else set()
print(f"SKUs com movimentacao no periodo: {len(skus_com_mov)}")

# SKUs sem movimentacao mas com TOTAL/ANTERIOR
skus_sem_mov_com_total = skus_com_totais - skus_com_mov
print(f"SKUs sem movimentacao mas com TOTAL/ANTERIOR: {len(skus_sem_mov_com_total)}")

if '001002' in skus_sem_mov_com_total:
    print("\n[OK] AMAPA esta na lista de SKUs sem movimentacao mas com TOTAL/ANTERIOR")
else:
    print("\n[AVISO] AMAPA NAO esta na lista de SKUs sem movimentacao mas com TOTAL/ANTERIOR")

conn.close()
