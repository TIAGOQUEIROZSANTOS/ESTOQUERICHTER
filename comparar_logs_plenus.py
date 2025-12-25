"""
Script para comparar logs de Plenus e identificar discrepâncias
Compara: HTML Importado vs Histórico Carregado vs Exibido para Usuário
"""
import json
import os
import glob
from datetime import datetime

def encontrar_log_mais_recente(pattern):
    """Encontra o log mais recente que corresponde ao padrão"""
    files = glob.glob(f"logs/{pattern}")
    if not files:
        return None
    return max(files, key=os.path.getmtime)

def carregar_log(caminho):
    """Carrega um log JSON"""
    if not caminho or not os.path.exists(caminho):
        return None
    with open(caminho, 'r', encoding='utf-8') as f:
        return json.load(f)

def comparar_logs():
    """Compara os três tipos de logs"""
    print("=" * 80)
    print("COMPARAÇÃO DE LOGS PLENUS - IDENTIFICAR DISCREPÂNCIAS")
    print("=" * 80)

    # Encontrar logs mais recentes
    log_import = encontrar_log_mais_recente("plenus_import_html_*.json")
    log_history = encontrar_log_mais_recente("plenus_load_history_*.json")
    log_debug = encontrar_log_mais_recente("plenus_debug_*.json")

    print(f"\n[IMPORT] Log Importacao HTML: {log_import}")
    print(f"[HISTORY] Log Historico Carregado: {log_history}")
    print(f"[DEBUG] Log Exibido Usuario: {log_debug}")

    # Carregar logs
    dados_import = carregar_log(log_import)
    dados_history = carregar_log(log_history)
    dados_debug = carregar_log(log_debug)

    if not dados_import and not dados_history and not dados_debug:
        print("\n❌ Nenhum log encontrado!")
        return

    # Extrair SKUs de cada fonte
    skus_import = {}
    skus_history = {}
    skus_exibidos = {}
    skus_calculo = {}

    if dados_import and "skus_detalhado" in dados_import:
        skus_import = dados_import["skus_detalhado"]

    if dados_history and "skus_detalhado" in dados_history:
        skus_history = dados_history["skus_detalhado"]

    if dados_debug:
        if "skus_exibidos_usuario" in dados_debug:
            skus_exibidos = dados_debug["skus_exibidos_usuario"]
        if "saldos_por_sku_detalhado" in dados_debug:
            skus_calculo = dados_debug["saldos_por_sku_detalhado"]

    print(f"\n[ESTATISTICAS]")
    print(f"  - SKUs Importados do HTML: {len(skus_import)}")
    print(f"  - SKUs Carregados do Historico: {len(skus_history)}")
    print(f"  - SKUs Exibidos para Usuario: {len(skus_exibidos)}")
    print(f"  - SKUs no Calculo do Total: {len(skus_calculo)}")

    # Comparar SKUs
    print(f"\n[COMPARACAO DE SKUs]")

    # SKUs que estão no HTML mas não no histórico
    skus_apenas_import = set(skus_import.keys()) - set(skus_history.keys())
    if skus_apenas_import:
        print(f"\n[AVISO] SKUs apenas no HTML (nao no historico): {len(skus_apenas_import)}")
        for sku in sorted(skus_apenas_import)[:10]:
            print(f"    - {sku}: {skus_import[sku].get('produto', '')} | Saldo: {skus_import[sku].get('saldo_m3', 0)}")

    # SKUs que estão no histórico mas não exibidos
    skus_apenas_history = set(skus_history.keys()) - set(skus_exibidos.keys())
    if skus_apenas_history:
        print(f"\n[AVISO] SKUs no historico mas NAO exibidos: {len(skus_apenas_history)}")
        for sku in sorted(skus_apenas_history)[:10]:
            print(f"    - {sku}: {skus_history[sku].get('produto', '')} | Saldo: {skus_history[sku].get('saldo_m3', 0)} | Cat: {skus_history[sku].get('cat_auto', '')}")

    # SKUs que estão exibidos mas não no cálculo
    skus_apenas_exibidos = set(skus_exibidos.keys()) - set(skus_calculo.keys())
    if skus_apenas_exibidos:
        print(f"\n[AVISO] SKUs exibidos mas NAO no calculo do total: {len(skus_apenas_exibidos)}")
        for sku in sorted(skus_apenas_exibidos)[:10]:
            print(f"    - {sku}: {skus_exibidos[sku].get('produto', '')} | Saldo: {skus_exibidos[sku].get('saldo_m3', 0)}")

    # Verificar AMAPA especificamente
    print(f"\n[VERIFICACAO ESPECIFICA - AMAPA (SKU 001002)]")
    amapa_sku = "001002"

    if amapa_sku in skus_import:
        print(f"  [OK] No HTML Importado: Saldo = {skus_import[amapa_sku].get('saldo_m3', 0)}")
    else:
        print(f"  [ERRO] NAO esta no HTML Importado")

    if amapa_sku in skus_history:
        print(f"  [OK] No Historico Carregado: Saldo = {skus_history[amapa_sku].get('saldo_m3', 0)}")
    else:
        print(f"  [ERRO] NAO esta no Historico Carregado")

    if amapa_sku in skus_exibidos:
        print(f"  [OK] Exibido para Usuario: Saldo = {skus_exibidos[amapa_sku].get('saldo_m3', 0)}")
        print(f"     No calculo: {skus_exibidos[amapa_sku].get('no_calculo_total', False)}")
    else:
        print(f"  [ERRO] NAO esta exibido para Usuario")

    if amapa_sku in skus_calculo:
        print(f"  [OK] No Calculo do Total: Saldo = {skus_calculo[amapa_sku].get('saldo', 0)}")
    else:
        print(f"  [ERRO] NAO esta no Calculo do Total")

    # Comparar saldos (M3) entre fontes
    print(f"\n[COMPARACAO DE SALDOS M3 - Primeiros 10 SKUs comuns]")
    skus_comuns = set(skus_import.keys()) & set(skus_history.keys()) & set(skus_exibidos.keys())
    count = 0
    for sku in sorted(skus_comuns):
        if count >= 10:
            break
        saldo_import = skus_import[sku].get('saldo_m3', 0)
        saldo_history = skus_history[sku].get('saldo_m3', 0)
        saldo_exibido = skus_exibidos[sku].get('saldo_m3', 0)
        saldo_calculo = skus_calculo.get(sku, {}).get('saldo', 0) if sku in skus_calculo else 0

        if abs(saldo_import - saldo_history) > 0.0001 or abs(saldo_history - saldo_exibido) > 0.0001:
            print(f"  [DIFERENCA] {sku}: HTML={saldo_import:.4f} | Hist={saldo_history:.4f} | Exib={saldo_exibido:.4f} | Calc={saldo_calculo:.4f}")
            count += 1

    # Total calculado
    if dados_debug:
        total_calc = dados_debug.get("estatisticas_gerais", {}).get("total_saldo_calculado", 0)
        print(f"\n[TOTAL CALCULADO NO SISTEMA] {total_calc:.4f}")
        print(f"   Total esperado (HTML): 13669.2718")
        print(f"   Diferenca: {total_calc - 13669.2718:.4f}")

if __name__ == "__main__":
    comparar_logs()
