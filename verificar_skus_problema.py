import json

# Ler log
with open('logs/debug_categoria_20251225_134706.json', 'r', encoding='utf-8') as f:
    log_data = json.load(f)

# SKUs problemáticos
skus_problema = ['001027', '001029', '001035', '002548']

print("=== SKUs Problematicos ===")
for item in log_data['detalhes_linhas_total']:
    if item['sku'] in skus_problema:
        print(f"SKU {item['sku']}: {item['produto']}")
        print(f"  Categoria: {item['categoria']}")
        print(f"  Saldo: {item['saldo']:,.4f}")
        print(f"  Tipo: {item['tipo']}")
        print()

# Verificar se há duplicatas desses SKUs
print("\n=== Verificando duplicatas ===")
for sku in skus_problema:
    ocorrencias = [item for item in log_data['detalhes_linhas_total'] if item['sku'] == sku]
    if len(ocorrencias) > 1:
        print(f"SKU {sku} aparece {len(ocorrencias)} vezes:")
        for occ in ocorrencias:
            print(f"  - {occ['produto']}: {occ['saldo']:,.4f}")
