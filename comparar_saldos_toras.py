import json
import pandas as pd

# Valores que o usuário enviou (soma correta: 13.669,2718)
# Formato: vírgula como separador decimal
valores_usuario_str = [
    "0,0000", "0,0000", "0,0000", "798,7034", "0,0000", "1.131,4260", "0,0000", "0,0000", "0,6980", "0,0001",
    "312,2899", "0,0000", "0,0010", "1,9900", "0,0000", "13,4800", "0,0050", "0,0000", "0,0000", "0,0000",
    "46,1263", "435,9491", "0,0010", "0,0000", "0,0000", "22,3970", "18,7975", "0,0000", "30,7560", "0,0000",
    "0,0060", "486,1874", "0,0000", "0,0000", "804,9300", "2.190,8063", "26,6640", "28,0800", "3,7570",
    "716,3152", "0,0000", "59,6380", "0,0000", "466,4855", "366,5310", "216,1120", "1.217,6030", "0,0000",
    "0,0000", "0,0000", "10,5630", "0,0000", "0,0000", "303,2890", "1.371,6649", "489,8490", "0,0110",
    "0,0000", "0,0000", "0,0000", "1.159,4082", "0,0100", "0,0002", "0,0000", "0,0000", "0,0000", "0,0054",
    "0,0000", "519,8740", "0,0000", "0,0000", "0,0000", "216,5422", "0,0000", "0,0000", "185,1123", "17,2069",
    "0,0000", "0,0000"
]

# Corrigir valores com vírgula (formato brasileiro: ponto como milhar, vírgula como decimal)
valores_usuario_corrigidos = []
for v in valores_usuario_str:
    # Remover ponto de milhar e substituir vírgula por ponto
    v_clean = v.replace('.', '').replace(',', '.')
    valores_usuario_corrigidos.append(float(v_clean))

soma_usuario = sum(valores_usuario_corrigidos)
print(f"Soma dos valores do usuário: {soma_usuario:,.4f}")

# Ler log
with open('logs/debug_categoria_20251225_134706.json', 'r', encoding='utf-8') as f:
    log_data = json.load(f)

saldos_log = [item['saldo'] for item in log_data['detalhes_linhas_total']]
soma_log = sum(saldos_log)
print(f"Soma dos valores do log: {soma_log:,.4f}")
print(f"Diferença: {soma_log - soma_usuario:,.4f}")

# Encontrar valores que estão no log mas não na lista do usuário (ou vice-versa)
print("\n=== Valores no log que podem estar causando diferença ===")
for item in log_data['detalhes_linhas_total']:
    saldo = item['saldo']
    # Verificar se o valor está próximo de algum valor do usuário (com tolerância)
    encontrado = False
    for v_user in valores_usuario_corrigidos:
        if abs(saldo - v_user) < 0.0001:
            encontrado = True
            break
    if not encontrado and saldo > 0.0001:  # Ignorar zeros
        print(f"SKU {item['sku']}: {item['produto']} - Saldo: {saldo:,.4f}")

# Ordenar valores do log para comparação
saldos_log_sorted = sorted(saldos_log)
valores_usuario_sorted = sorted(valores_usuario_corrigidos)

print(f"\nTotal de valores no log: {len(saldos_log_sorted)}")
print(f"Total de valores do usuário: {len(valores_usuario_sorted)}")

# Verificar se há valores duplicados no log
from collections import Counter
contagem_log = Counter(saldos_log)
duplicados = {k: v for k, v in contagem_log.items() if v > 1}
if duplicados:
    print(f"\nATENCAO: Valores duplicados no log: {duplicados}")
