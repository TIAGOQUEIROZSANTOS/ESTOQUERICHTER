"""
Debug completo do AMAPA - verificar cada etapa do processo
"""
import sqlite3
import pandas as pd
from datetime import datetime, date

ARQUIVO_DB = "dados_sistema.db"

def get_db_connection():
    return sqlite3.connect(ARQUIVO_DB)

def normalizar_sku(sku):
    """Remove zeros à esquerda do SKU"""
    if pd.isna(sku) or sku is None:
        return ""
    return str(sku).lstrip('0') or "0"

def detectar_categoria_plenus(item_completo):
    """Detecta categoria baseado no nome do produto"""
    if not item_completo or pd.isna(item_completo):
        return None
    item_str = str(item_completo).upper()

    # TORAS
    if any(termo in item_str for termo in ["TORAS", "TOROS", "TORA"]):
        return "TORAS"
    # SERRADAS
    if any(termo in item_str for termo in ["SERRADA", "SERRADO", "PRANCHA", "PRANCHAS", "TABUA", "TABUAS"]):
        return "SERRADAS"
    # BENEFICIADAS
    if any(termo in item_str for termo in ["BENEFICIADA", "BENEFICIADO", "ALISAR", "BRUTO"]):
        return "BENEFICIADAS"

    return None

conn = get_db_connection()

print("=" * 80)
print("DEBUG COMPLETO - AMAPA (SKU 001002)")
print("=" * 80)

# 1. Verificar no banco
print("\n[1] VERIFICANDO NO BANCO:")
query_amapa = """
    SELECT * FROM plenus_historico
    WHERE sku = '001002'
    ORDER BY data_movimento DESC, id DESC
"""
df_amapa_db = pd.read_sql(query_amapa, conn)
print(f"  Total de registros no banco: {len(df_amapa_db)}")
if not df_amapa_db.empty:
    for idx, row in df_amapa_db.iterrows():
        print(f"    - ID: {row['id']} | Tipo: {row['tipo_movimento']} | Data: {row['data_movimento']} | Saldo: {row['saldo_apos']} | Cat: {row['categoria']}")

# 2. Simular carregamento do histórico (período 01/01/2024 a 31/01/2025)
print("\n[2] SIMULANDO CARREGAMENTO DO HISTORICO (01/01/2024 a 31/01/2025):")
dt_ini = date(2024, 1, 1)
dt_fim = date(2025, 1, 31)

# Movimentações no período
query_mov = """
    SELECT * FROM plenus_historico
    WHERE substr(data_movimento,1,10) BETWEEN ? AND ?
"""
params_mov = [dt_ini.strftime("%Y-%m-%d"), dt_fim.strftime("%Y-%m-%d")]
df_mov = pd.read_sql(query_mov, conn, params=params_mov)
print(f"  Movimentações no período: {len(df_mov)}")

# Verificar se AMAPA está nas movimentações
amapa_mov = df_mov[df_mov['sku'] == '001002'] if not df_mov.empty else pd.DataFrame()
print(f"  AMAPA nas movimentações: {len(amapa_mov)} registros")

# Linhas TOTAL/ANTERIOR (todas)
query_totais = """
    SELECT * FROM plenus_historico
    WHERE tipo_movimento IN ('TOTAL', 'TOTAL:', 'ANTERIOR', 'ANTERIOR:')
"""
df_totais = pd.read_sql(query_totais, conn)
print(f"  Linhas TOTAL/ANTERIOR (todas): {len(df_totais)}")

# Verificar se AMAPA está nas linhas TOTAL/ANTERIOR
amapa_totais = df_totais[df_totais['sku'] == '001002'] if not df_totais.empty else pd.DataFrame()
print(f"  AMAPA nas linhas TOTAL/ANTERIOR: {len(amapa_totais)} registros")

# Combinar (como faz a função carregar_plenus_movimento_db)
if df_mov.empty:
    df_combined = df_totais.copy()
elif df_totais.empty:
    df_combined = df_mov.copy()
else:
    df_combined = pd.concat([df_mov, df_totais], ignore_index=True)
    df_combined = df_combined.drop_duplicates(subset=['sku', 'tipo_movimento', 'data_movimento'], keep='first')

print(f"  Resultado combinado: {len(df_combined)} linhas")
amapa_combined = df_combined[df_combined['sku'] == '001002'] if not df_combined.empty else pd.DataFrame()
print(f"  AMAPA no resultado combinado: {len(amapa_combined)} registros")

# 3. Simular processamento (renomear colunas, criar Item_Completo, Cat_Auto)
print("\n[3] SIMULANDO PROCESSAMENTO:")
if not df_combined.empty:
    # Renomear
    if 'tipo_movimento' in df_combined.columns:
        df_combined.rename(columns={'tipo_movimento': 'tipo', 'saldo_apos': 'saldo'}, inplace=True)

    # Criar Item_Completo e Cat_Auto
    if 'categoria' not in df_combined.columns:
        df_combined['categoria'] = ""
    df_combined["Item_Completo"] = df_combined["produto"].astype(str) + " (" + df_combined["categoria"].fillna("").astype(str) + ")"
    df_combined["Cat_Auto"] = df_combined["Item_Completo"].apply(detectar_categoria_plenus)

    # Normalizar TOROS para TORAS
    if 'categoria' in df_combined.columns:
        df_combined['categoria'] = df_combined['categoria'].replace('TOROS', 'TORAS')
    if 'Cat_Auto' in df_combined.columns:
        df_combined['Cat_Auto'] = df_combined['Cat_Auto'].replace('TOROS', 'TORAS')

    # Verificar AMAPA após processamento
    amapa_processado = df_combined[df_combined['sku'] == '001002'] if not df_combined.empty else pd.DataFrame()
    print(f"  AMAPA após processamento: {len(amapa_processado)} registros")
    if not amapa_processado.empty:
        for idx, row in amapa_processado.iterrows():
            print(f"    - Tipo: {row['tipo']} | Cat: {row['categoria']} | Cat_Auto: {row['Cat_Auto']} | Saldo: {row['saldo']}")

# 4. Simular filtro de categoria TORAS
print("\n[4] SIMULANDO FILTRO DE CATEGORIA TORAS:")
if not df_combined.empty:
    cat_para_filtrar = ['TORAS']
    if 'Cat_Auto' in df_combined.columns:
        df_filtrado_cat = df_combined[df_combined['Cat_Auto'].isin(cat_para_filtrar)].copy()
        print(f"  Linhas após filtrar por TORAS: {len(df_filtrado_cat)}")

        amapa_filtrado_cat = df_filtrado_cat[df_filtrado_cat['sku'] == '001002'] if not df_filtrado_cat.empty else pd.DataFrame()
        print(f"  AMAPA após filtro de categoria: {len(amapa_filtrado_cat)} registros")
        if amapa_filtrado_cat.empty:
            print("  [ERRO] AMAPA foi filtrado pelo filtro de categoria!")
            # Verificar por que
            amapa_antes = df_combined[df_combined['sku'] == '001002']
            if not amapa_antes.empty:
                print(f"    Cat_Auto do AMAPA antes do filtro: {amapa_antes['Cat_Auto'].unique()}")

# 5. Simular filtro de data (período padrão)
print("\n[5] SIMULANDO FILTRO DE DATA (01/01/2025 a 31/01/2025):")
if not df_combined.empty and 'data_movimento' in df_combined.columns:
    d_ini_f = date(2025, 1, 1)
    d_fim_f = date(2025, 1, 31)

    df_combined['dt_temp'] = pd.to_datetime(df_combined['data_movimento'], errors='coerce').dt.date

    # Manter linhas TOTAL/ANTERIOR independente da data
    if 'tipo' in df_combined.columns:
        mask_tipo_especial = df_combined['tipo'].astype(str).str.strip().str.upper().isin(['TOTAL', 'TOTAL:', 'ANTERIOR', 'ANTERIOR:'])
        mask = mask_tipo_especial | (df_combined['dt_temp'].isna()) | ((df_combined['dt_temp'] >= d_ini_f) & (df_combined['dt_temp'] <= d_fim_f))
    else:
        mask = (df_combined['dt_temp'].isna()) | ((df_combined['dt_temp'] >= d_ini_f) & (df_combined['dt_temp'] <= d_fim_f))

    df_filtrado_data = df_combined[mask].copy()
    df_filtrado_data = df_filtrado_data.drop(columns=['dt_temp'])

    print(f"  Linhas após filtro de data: {len(df_filtrado_data)}")

    amapa_filtrado_data = df_filtrado_data[df_filtrado_data['sku'] == '001002'] if not df_filtrado_data.empty else pd.DataFrame()
    print(f"  AMAPA após filtro de data: {len(amapa_filtrado_data)} registros")
    if amapa_filtrado_data.empty:
        print("  [ERRO] AMAPA foi filtrado pelo filtro de data!")
        # Verificar por que
        amapa_antes = df_combined[df_combined['sku'] == '001002']
        if not amapa_antes.empty:
            for idx, row in amapa_antes.iterrows():
                print(f"    Tipo: {row['tipo']} | Data: {row['data_movimento']} | dt_temp: {row.get('dt_temp', 'N/A')}")

# 6. Verificar se há linhas TOTAL/ANTERIOR sendo preservadas
print("\n[6] VERIFICANDO PRESERVACAO DE LINHAS TOTAL/ANTERIOR:")
if not df_combined.empty and 'tipo' in df_combined.columns:
    mask_total_anterior = df_combined['tipo'].astype(str).str.strip().str.upper().isin(['TOTAL', 'TOTAL:', 'ANTERIOR', 'ANTERIOR:'])
    df_total_anterior = df_combined[mask_total_anterior].copy()
    print(f"  Total de linhas TOTAL/ANTERIOR: {len(df_total_anterior)}")

    amapa_total_anterior = df_total_anterior[df_total_anterior['sku'] == '001002'] if not df_total_anterior.empty else pd.DataFrame()
    print(f"  AMAPA nas linhas TOTAL/ANTERIOR: {len(amapa_total_anterior)} registros")
    if amapa_total_anterior.empty:
        print("  [ERRO] AMAPA nao tem linhas TOTAL/ANTERIOR!")

conn.close()
