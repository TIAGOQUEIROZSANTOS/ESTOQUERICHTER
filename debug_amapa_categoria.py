"""
Script para debugar categoria do AMAPA
"""
import sqlite3
import pandas as pd
import re

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

ARQUIVO_DB = "dados_sistema.db"

def get_db_connection():
    return sqlite3.connect(ARQUIVO_DB)

conn = get_db_connection()

print("=" * 80)
print("DEBUG - CATEGORIA DO AMAPA")
print("=" * 80)

# Carregar AMAPA do banco
query_amapa = """
    SELECT sku, produto, categoria, tipo_movimento, data_movimento, saldo_apos
    FROM plenus_historico
    WHERE sku = '001002'
    ORDER BY data_movimento DESC, id DESC
"""
df_amapa = pd.read_sql(query_amapa, conn)

if not df_amapa.empty:
    print(f"\n[1] AMAPA encontrado no banco: {len(df_amapa)} registros")

    # Renomear colunas como no app.py
    if 'tipo_movimento' in df_amapa.columns:
        df_amapa.rename(columns={'tipo_movimento': 'tipo', 'saldo_apos': 'saldo'}, inplace=True)

    # Criar Item_Completo e Cat_Auto como no app.py
    if 'categoria' not in df_amapa.columns:
        df_amapa['categoria'] = ""

    df_amapa["Item_Completo"] = df_amapa["produto"].astype(str) + " (" + df_amapa["categoria"].fillna("").astype(str) + ")"
    df_amapa["Cat_Auto"] = df_amapa["Item_Completo"].apply(detectar_categoria_plenus)

    # Normalizar TOROS para TORAS
    if 'categoria' in df_amapa.columns:
        df_amapa['categoria'] = df_amapa['categoria'].replace('TOROS', 'TORAS')
    if 'Cat_Auto' in df_amapa.columns:
        df_amapa['Cat_Auto'] = df_amapa['Cat_Auto'].replace('TOROS', 'TORAS')

    print("\n[2] Dados do AMAPA após processamento:")
    for idx, row in df_amapa.iterrows():
        print(f"  SKU: {row['sku']}")
        print(f"  Produto: {row['produto']}")
        print(f"  Categoria (banco): {row.get('categoria', 'N/A')}")
        print(f"  Item_Completo: {row['Item_Completo']}")
        print(f"  Cat_Auto: {row['Cat_Auto']}")
        print(f"  Tipo: {row['tipo']}")
        print(f"  Saldo: {row['saldo']}")
        print()

    # Verificar se passaria no filtro de TORAS
    print("[3] Testando filtro de categoria TORAS:")
    cat_para_filtrar = ['TORAS']

    if 'Cat_Auto' in df_amapa.columns:
        df_filtrado = df_amapa[df_amapa['Cat_Auto'].isin(cat_para_filtrar)].copy()
        print(f"  Linhas após filtrar por TORAS: {len(df_filtrado)}")
        if len(df_filtrado) == len(df_amapa):
            print("  [OK] AMAPA passaria no filtro de TORAS")
        else:
            print("  [ERRO] AMAPA NAO passaria no filtro de TORAS!")
            print(f"  Cat_Auto do AMAPA: {df_amapa['Cat_Auto'].unique()}")
    else:
        print("  [ERRO] Coluna Cat_Auto nao existe!")

    # Verificar valores únicos
    print("\n[4] Valores únicos:")
    print(f"  Categoria: {df_amapa['categoria'].unique()}")
    print(f"  Cat_Auto: {df_amapa['Cat_Auto'].unique()}")
    print(f"  Item_Completo: {df_amapa['Item_Completo'].unique()}")

else:
    print("\n[ERRO] AMAPA nao encontrado no banco!")

conn.close()
