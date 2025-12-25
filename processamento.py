import pdfplumber
import pandas as pd
import re
import io
import streamlit as st
from bs4 import BeautifulSoup
from datetime import datetime, date
from difflib import SequenceMatcher

# --- CONSTANTES ---
MAPA_CORRECAO_PRODUTOS = {
    "10": "10 - Toras de Madeira Nativa",
    "20": "20 - Madeira Serrada em Bruto",
    "3030": "3030 - Madeira Serrada Aproveitamento",
    "50": "50 - Madeira Beneficiada"
}
CODIGOS_ACEITOS = ["10", "20", "3030", "50"]
TERMOS_GENERICOS = [
    "TORAS DE MADEIRA NATIVA", "MADEIRA SERRADA EM BRUTO",
    "MADEIRA SERRADA APROVEITAMENTO", "MADEIRA BENEFICIADA",
    "MADEIRA", "TORAS", "SERRADA", "BENEFICIADA"
]
COLS_SISTRANSF_EXCEL = [
    "Número", "Data Realização", "Situação",
    "Produto Origem", "Essência Origem", "Volume Origem", "Unidade Origem",
    "Produto Gerado", "Essência Gerada", "Volume Gerado", "Unidade Gerada"
]

# --- FUNÇÕES AUXILIARES GERAIS ---
def parse_float_inteligente(valor):
    try:
        if isinstance(valor, (float, int)):
            return float(valor)
        val_str = str(valor).strip()
        if not val_str:
            return 0.0
        # Check Brazilian format (dot as thousand, comma as decimal)
        # but also handle cases where only dot or only comma is used
        if ',' in val_str and '.' in val_str:
            # Assume 1.000,00 format
            clean = val_str.replace('.', '').replace(',', '.')
            return float(clean)
        elif ',' in val_str:
            return float(val_str.replace(',', '.'))
        return float(val_str)
    except:
        return 0.0

def formatar_br(valor):
    if not isinstance(valor, (float, int)):
        return str(valor)
    return f"{valor:,.4f}".replace(',', 'X').replace('.', ',').replace('X', '.')

def parse_data_br(valor):
    if not valor or not isinstance(valor, str):
        return None
    try:
        return datetime.strptime(valor.strip(), "%d/%m/%Y").date()
    except:
        return None

def detecting_category(item, origin):
    cat = "OUTROS"
    if origin == "SISFLORA":
        if str(item).startswith("10"):
            cat = "TORAS"
        elif str(item).startswith("20") or str(item).startswith("3030"):
            cat = "SERRADAS"
        elif str(item).startswith("50"):
            cat = "BENEFICIADAS"
    else:
        # PLENUS Logic
        txt = str(item).upper()
        if "BENEF" in txt or "DECK" in txt or "FORRO" in txt or "ASSOALHO" in txt:
            cat = "BENEFICIADAS"
        elif "TORA" in txt or "TORO" in txt:
            cat = "TORAS"
        elif "SERRAD" in txt or "CAIBRO" in txt or "VIGA" in txt or "PRANCH" in txt or "RIPA" in txt:
            cat = "SERRADAS"
    return cat

def detectar_categoria_plenus(item_completo):
    # Alias for compatibility with old extraction logic
    return detecting_category(item_completo, "PLENUS")

def sort_key_nomes(item):
    parts = str(item).split(' - ', 1)
    if len(parts) > 1:
        return parts[1].strip()
    return str(item)

# --- FUNÇÕES AUXILIARES SISTRANSF ---
def transform_data_sistransf(df, filename="Upload"):
    rows = []
    for idx, row in df.iterrows():
        essencia_origem = str(row.get("Essência Origem", ""))
        popular = ""
        if "-" in essencia_origem:
            parts = essencia_origem.split("-", 1)
            if len(parts) > 1:
                popular = parts[1].strip()

        dt_real = row.get("Data Realização", "")
        if isinstance(dt_real, (pd.Timestamp, datetime, date)):
            dt_real = dt_real.strftime("%Y-%m-%d")
        else:
            try:
                dt_obj = datetime.strptime(str(dt_real).strip(), "%d/%m/%Y")
                dt_real = dt_obj.strftime("%Y-%m-%d")
            except:
                pass

        origem = {
            "numero": str(row.get("Número", "")),
            "data_realizacao": str(dt_real),
            "situacao": str(row.get("Situação", "")),
            "tipo_produto": "PRODUTO DE ORIGEM",
            "produto": str(row.get("Produto Origem", "")),
            "popular": popular,
            "essencia": str(row.get("Essência Origem", "")),
            "volume": float(row.get("Volume Origem", 0) if pd.notnull(row.get("Volume Origem")) else 0),
            "unidade": str(row.get("Unidade Origem", "")),
            "arquivo_origem": filename
        }
        rows.append(origem)

        produto_gerado = row.get("Produto Gerado", "")
        if pd.notnull(produto_gerado) and str(produto_gerado).strip() != "":
            popular_gerado = ""
            if "-" in str(row.get("Essência Origem", "")):
                parts = str(row.get("Essência Origem", "")).split("-", 1)
                if len(parts) > 1:
                    popular_gerado = parts[1].strip()

            gerado = {
                "numero": str(row.get("Número", "")),
                "data_realizacao": str(dt_real),
                "situacao": str(row.get("Situação", "")),
                "tipo_produto": "PRODUTO GERADO",
                "produto": str(produto_gerado),
                "popular": popular_gerado,
                "essencia": str(row.get("Essência Origem", "")),
                "volume": float(row.get("Volume Gerado", 0) if pd.notnull(row.get("Volume Gerado")) else 0),
                "unidade": str(row.get("Unidade Gerada", "")),
                "arquivo_origem": filename
            }
            rows.append(gerado)

    final_df = pd.DataFrame(rows)
    mask_origem = final_df["tipo_produto"] == "PRODUTO DE ORIGEM"
    df_origem = final_df[mask_origem].drop_duplicates(subset=["numero", "essencia", "volume"], keep="first")
    df_outros = final_df[~mask_origem]
    final_df = pd.concat([df_origem, df_outros], ignore_index=True)
    return final_df

def to_excel_autoajustado(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Relatorio')
        workbook = writer.book
        worksheet = writer.sheets['Relatorio']
        for i, col in enumerate(df.columns):
            max_len = max(df[col].astype(str).map(len).max() if not df[col].empty else 0, len(str(col)))
            worksheet.set_column(i, i, max_len + 3)
    return output.getvalue()

# --- ALGORITMO DE VÍNCULO ---
def limpar_para_comparacao(texto):
    palavras_lixo = {
        "MADEIRA", "NATIVA", "SERRADA", "SERRADOS", "SERRADO", "BENEFICIADA", "BENEFICIADO", "BENEFICIADOS",
        "TORAS", "TOROS", "TORA", "TORO", "EM", "DE", "DO", "DA", "BRUTO", "APROVEITAMENTO",
        "TABUA", "VIGA", "CAIBRO", "PRANCHA", "RIPA", "SARRAFO", "DECK", "ASSOALHO", "FORRO", "-", ".", ",", "(", ")"
    }
    texto_limpo = re.sub(r'[^\w\s]', ' ', texto.upper())
    parts = texto_limpo.split()
    clean_parts = [p for p in parts if p not in palavras_lixo and not p.isdigit()]
    if not clean_parts:
        return texto.upper()
    return " ".join(clean_parts)

def calcular_similaridade_avancada(nome_plenus, nome_sisflora):
    essencia_p = limpar_para_comparacao(nome_plenus)
    essencia_s = limpar_para_comparacao(nome_sisflora)
    ratio_essencia = SequenceMatcher(None, essencia_p, essencia_s).ratio()
    bonus = 0
    if len(essencia_p) > 3 and len(essencia_s) > 3:
        if essencia_p in essencia_s or essencia_s in essencia_p:
            bonus = 0.15
    return min(ratio_essencia + bonus, 1.0)

def gerar_sugestao_nome_primeiro(itens_selecionados, categoria_filtro, origem="SISFLORA"):
    if not itens_selecionados:
        return ""
    primeiro_item = itens_selecionados[0]
    nome_bruto = ""
    cat_detectada = ""
    s_upper = primeiro_item.upper()

    if origem == "SISFLORA":
        parts = primeiro_item.split(' - ')
        candidatos = [p.strip() for p in parts if not re.match(r'^\d+$', p.strip()) and p.strip().upper() not in TERMOS_GENERICOS]
        if candidatos:
            nome_bruto = candidatos[-1]
        else:
            nome_bruto = parts[-1]
        if primeiro_item.startswith("10"):
            cat_detectada = "TORAS"
        elif primeiro_item.startswith("20") or primeiro_item.startswith("3030"):
            cat_detectada = "SERRADAS"
        elif primeiro_item.startswith("50"):
            cat_detectada = "BENEFICIADAS"
    else:
        s_clean = primeiro_item.split(' (')[0]
        if ' - ' in s_clean:
            nome_bruto = s_clean.split(' - ', 1)[1].strip()
        else:
            nome_bruto = s_clean.strip()
        if "BENEF" in s_upper or "DECK" in s_upper:
            cat_detectada = "BENEFICIADAS"
        elif "TORA" in s_upper or "TORO" in s_upper:
            cat_detectada = "TORAS"
        elif "SERRAD" in s_upper or "CAIBRO" in s_upper or "VIGA" in s_upper or "PRANCH" in s_upper or "RIPA" in s_upper:
            cat_detectada = "SERRADAS"

    TERMOS_LIMPEZA = [
        "SERRADA", "SERRADOS", "SERRADO", "BENEFICIADA", "BENEFICIADO", "BENEFICIADOS",
        "TORAS", "TOROS", "TORA", "TORO", "EM", "DE", "DO", "BRUTO", "APROVEITAMENTO",
        "TABUA", "TABUAS", "VIGA", "VIGAS", "CAIBRO", "CAIBROS", "PRANCHA", "PRANCHAO",
        "RIPA", "RIPAS", "SARRAFO", "DECK", "ASSOALHO", "FORRO", "LAMBRI", "RODAPE", "ALISAR", "BATENTE"
    ]
    palavras = nome_bruto.upper().replace(".", " ").replace("-", " ").split()
    essencia_parts = [p for p in palavras if p not in TERMOS_LIMPEZA and not p.isdigit()]
    essencia_final = " ".join(essencia_parts)
    cat_final = ""
    if categoria_filtro:
        cat_final = categoria_filtro
    elif cat_detectada:
        cat_final = cat_detectada
    if cat_final == "OUTROS":
        cat_final = ""
    if cat_final.endswith('S') and cat_final != "TORAS":
        cat_final = cat_final[:-1]
    if cat_final:
        raiz_cat = cat_final[:-1] if len(cat_final) > 3 else cat_final
        if raiz_cat in essencia_final:
            return essencia_final
    if cat_final and cat_final not in essencia_final:
        return f"{essencia_final} {cat_final}"
    return essencia_final

# --- LEITURA SISFLORA ---
@st.cache_data(show_spinner=False)
def extrair_dados_sisflora(arquivo):
    dados_brutos = []
    with pdfplumber.open(arquivo) as pdf:
        for page in pdf.pages:
            tabela = page.extract_table()
            if tabela:
                for linha in tabela:
                    txt = "".join([str(c) for c in linha if c]).lower()
                    if "governo" in txt or "sisflora" in txt or "página" in txt:
                        continue
                    dados_brutos.append(linha)
            page.flush_cache()

    linhas_corrigidas = []
    linha_anterior = None
    for row in dados_brutos:
        row_str = [str(x) if x is not None else "" for x in row]
        eh_novo_registro = re.match(r'^\d+\s*-', row_str[0])
        if eh_novo_registro:
            if linha_anterior:
                linhas_corrigidas.append(linha_anterior)
            linha_anterior = row_str
        else:
            if linha_anterior and len(row_str) > 1:
                linha_anterior[1] = (linha_anterior[1] + " " + row_str[1]).strip()
    if linha_anterior:
        linhas_corrigidas.append(linha_anterior)
    dados_finais = linhas_corrigidas if linhas_corrigidas else dados_brutos

    colunas_padrao = ["Produto", "Essencia", "Volume Disponivel", "Item_Completo", "Cat_Auto"]
    if not dados_finais:
        return pd.DataFrame(columns=colunas_padrao)

    df = pd.DataFrame(dados_finais)
    idx_dados = -1
    for i in range(min(len(df), 50)):
        c0 = str(df.iloc[i, 0]).strip()
        if re.match(r'^\d+\s*-', c0):
            idx_dados = i
            break
    if idx_dados == -1:
        return pd.DataFrame(columns=colunas_padrao)
    df = df[idx_dados:].reset_index(drop=True)

    idx_vol = len(df.columns) - 1
    for c in range(len(df.columns)-1, -1, -1):
        if df.iloc[:20, c].astype(str).str.contains(r'\d+,\d+').any():
            idx_vol = c
            break

    mapa_cols = {0: "Produto"}
    if idx_vol > 1:
        mapa_cols[1] = "Essencia"
    if idx_vol >= 2:
        mapa_cols[idx_vol-1] = "Unidade"

    mapa_cols[idx_vol] = "Volume Disponivel"
    df.rename(columns=mapa_cols, inplace=True)

    if "Produto" not in df.columns:
        df["Produto"] = ""
    if "Essencia" not in df.columns:
        df["Essencia"] = ""
    if "Unidade" not in df.columns:
        df["Unidade"] = ""
    if "Volume Disponivel" not in df.columns:
        df["Volume Disponivel"] = "0"

    def extrair_codigo(val):
        match = re.match(r'^(\d+)', str(val).strip())
        return match.group(1) if match else None

    df['Codigo'] = df['Produto'].apply(extrair_codigo)
    df = df[df['Codigo'].isin(CODIGOS_ACEITOS)].copy()

    def limpa_prod(texto):
        match = re.match(r'^(\d+)', str(texto).strip())
        if match and match.group(1) in MAPA_CORRECAO_PRODUTOS:
            return MAPA_CORRECAO_PRODUTOS[match.group(1)]
        return str(texto).strip()

    def limpa_ess(texto):
        t = str(texto).replace('\n', ' ').strip()
        t = re.sub(r'(CCSEMA\s*[-–]?\s*\d+|PMFS|AUTEX|PEF|\d{3,}/\d{4}|GERAL\s*ST\s*[\d,.-]+)', '', t, flags=re.IGNORECASE)
        return re.sub(r'^[-–\s]+|[-–\s]+$', '', t).strip()

    df["Volume Disponivel"] = df["Volume Disponivel"].apply(parse_float_inteligente)
    df["Produto"] = df["Produto"].apply(limpa_prod)
    df["Essencia"] = df["Essencia"].apply(limpa_ess)
    df["Item_Completo"] = df.apply(lambda x: f"{x['Produto']} - {x['Essencia']}" if x['Essencia'] else x['Produto'], axis=1)
    df["Cat_Auto"] = df["Item_Completo"].apply(lambda x: detecting_category(x, "SISFLORA"))
    return df

# --- LEITURA SISCONSUMO ---
@st.cache_data(show_spinner=False)
def load_data_consumo_excel(file):
    try:
        df = pd.read_excel(file, header=1)
    except Exception:
        file.seek(0)
        df = pd.read_csv(file, header=1, encoding='utf-8', sep=',')

    if 'Quantidade' in df.columns:
        df['Quantidade'] = pd.to_numeric(df['Quantidade'], errors='coerce').fillna(0)

    if 'Data' in df.columns:
        df['Data'] = pd.to_datetime(df['Data'], errors='coerce')

    return df

# --- LEITURA PLENUS (v189: Validação TOTAL + Saldo Anterior) ---
@st.cache_data(show_spinner=False)
def extrair_dados_plenus_html(arquivo_html, nome_arquivo="Upload"):
    soup = BeautifulSoup(arquivo_html, 'html.parser')
    dados_extraidos = []

    skus_vistos = set()
    skus_com_total = set()
    saldos_anteriores = {}  # {sku: saldo_anterior}

    state = {'categoria': None, 'sku': None, 'produto': None}

    def safe_txt(c):
        return c.get_text(strip=True) if c else ''

    rows = soup.find_all('tr')
    for tr in rows:
        cols = tr.find_all('td')
        if not cols:
            continue
        cat_cell = tr.find('td', class_='s29')
        if cat_cell and safe_txt(cat_cell).startswith('Categoria:'):
            state['categoria'] = safe_txt(cat_cell).replace('Categoria: ', '')
            continue
        prod_cell = tr.find('td', class_='s12')
        if prod_cell:
            prod_txt = safe_txt(prod_cell)
            match = re.match(r'(\S+)\s*-\s*(.*)', prod_txt)
            if match:
                new_sku = match.group(1).strip()
                new_prod = match.group(2).strip()
                if new_sku != state['sku']:
                    state['sku'] = new_sku
                    state['produto'] = new_prod
                    if state['sku']:
                        skus_vistos.add(state['sku'])
            continue
        if state['sku']:
            tipo_cell = tr.find('td', class_=['s14', 's25'])
            if not tipo_cell:
                continue
            tipo = safe_txt(tipo_cell)

            vlr_unit = parse_float_inteligente(safe_txt(tr.find('td', class_='s18')))
            vlr_total = parse_float_inteligente(safe_txt(tr.find('td', class_='s19')))
            ent = parse_float_inteligente(safe_txt(tr.find('td', class_='s15') or tr.find('td', class_='s21')))
            sai = parse_float_inteligente(safe_txt(tr.find('td', class_='s16') or tr.find('td', class_='s22')))
            sal = parse_float_inteligente(safe_txt(tr.find('td', class_='s17') or tr.find('td', class_='s23')))
            # V189: Extrair NOTA e SÉRIE - tentar múltiplas classes CSS possíveis
            nota = ""
            serie = ""
            # Tenta classes específicas primeiro para NOTA
            nota_cell = tr.find('td', class_='s20') or tr.find('td', class_='s11') or tr.find('td', class_='s24')
            if nota_cell:
                nota = safe_txt(nota_cell)

            # V189: Para SÉRIE, tentar várias classes CSS possíveis
            # Busca mais abrangente tentando todas as classes comuns
            for class_name in ['s10', 's11', 's24', 's26', 's27', 's28', 's30']:
                serie_cell = tr.find('td', class_=class_name)
                if serie_cell:
                    serie_txt = safe_txt(serie_cell)
                    # Verifica se não é a mesma célula da nota e tem conteúdo válido
                    if serie_txt and serie_txt.strip() and serie_txt != nota:
                        serie = serie_txt
                        break

            # Se ainda não encontrou, busca por todas as células procurando padrão de série
            if not serie or not serie.strip():
                all_cells = tr.find_all('td')
                nota_idx = -1
                # Encontra o índice da célula da nota
                for idx, cell in enumerate(all_cells):
                    if safe_txt(cell) == nota and nota:
                        nota_idx = idx
                        break

                # Busca células adjacentes à nota (série geralmente vem antes ou depois)
                if nota_idx >= 0:
                    for offset in [1, -1, 2, -2]:
                        check_idx = nota_idx + offset
                        if 0 <= check_idx < len(all_cells):
                            cell_txt = safe_txt(all_cells[check_idx])
                            if cell_txt and cell_txt.strip() and cell_txt != nota:
                                # Verifica se parece ser uma série (curto, alfanumérico)
                                if len(cell_txt.strip()) <= 15:
                                    serie = cell_txt.strip()
                                    break

            data_raw = safe_txt(tr.find('td', class_='s13'))

            if tipo.upper() in ['TOTAL', 'TOTAL:']:
                skus_com_total.add(state['sku'])
                data_raw = "Total"
            elif tipo.upper() in ['ANTERIOR', 'ANTERIOR:']:
                # Guarda o saldo anterior deste SKU
                saldos_anteriores[state['sku']] = sal
                data_raw = "Anterior"

            data_db = None
            if data_raw and data_raw not in ["Total", "Anterior"]:
                try:
                    data_db = datetime.strptime(data_raw, "%d/%m/%Y").strftime("%Y-%m-%d")
                except:
                    pass

            # V189: Não inclui linhas "Anterior" ou "Total" na coluna data (só movimentações com data real)
            # Mas ainda extrai o saldo_anterior quando tipo é "ANTERIOR"
            dados_extraidos.append({
                "sku": state['sku'],
                "produto": state['produto'],
                "categoria": state['categoria'] if state['categoria'] else "",  # Garantir string vazia se None
                "data": data_raw if data_raw not in ["Anterior", "Total"] else "",  # Limpa "Anterior"/"Total" da coluna data
                "data_movimento": data_db,
                "tipo": tipo,
                "entrada": ent,
                "saida": sai,
                "saldo": sal,
                "nota": nota,
                "serie": serie,
                "vlr_unit": vlr_unit,
                "vlr_total": vlr_total,
                "arquivo_origem": nome_arquivo
            })

    # VALIDAÇÃO v189: Todos os SKUs devem ter TOTAL
    lista_erros_skus = list(skus_vistos - skus_com_total)
    lista_erros_detalhada = []

    # Criar DataFrame temporario para buscar nomes dos SKUs com erro
    df_temp = pd.DataFrame(dados_extraidos)
    if not df_temp.empty:
        # V189: Garantir que categoria não seja None
        df_temp["categoria"] = df_temp["categoria"].fillna("")
        df_temp["Item_Completo"] = df_temp["produto"] + " (" + df_temp["categoria"] + ")"
        df_temp["Cat_Auto"] = df_temp["Item_Completo"].apply(detectar_categoria_plenus)

        # V189: Adiciona coluna saldo_anterior propagada para TODAS as linhas do mesmo SKU
        # Isso permite ver o saldo anterior em todas as linhas de movimentação do produto
        # IMPORTANTE: Sempre mostra saldo_anterior, mesmo que seja 0 (zero é um valor válido)
        df_temp['saldo_anterior'] = df_temp['sku'].map(saldos_anteriores).fillna(0.0)
        # Preenche com 0.0 se não tiver saldo_anterior mapeado (sempre mostra, nunca None)

        for sku_erro in lista_erros_skus:
            prod_nome = "Desconhecido"
            matches = df_temp[df_temp['sku'] == sku_erro]
            if not matches.empty:
                prod_nome = matches.iloc[0]['produto']
            lista_erros_detalhada.append({"SKU": sku_erro, "Produto": prod_nome, "Erro": "Sem Total"})
    else:
        df_temp = pd.DataFrame(columns=["sku", "produto", "categoria", "saldo", "saldo_anterior", "tipo", "Item_Completo", "Cat_Auto", "data", "data_movimento", "entrada", "saida", "nota", "serie", "vlr_unit", "vlr_total", "arquivo_origem"])

    # LOG: Salvar log de importação do HTML com SKUs e saldos (M3)
    import json
    import os
    if not df_temp.empty and 'sku' in df_temp.columns and 'saldo' in df_temp.columns:
        log_importacao = {
            "timestamp": datetime.now().isoformat(),
            "fonte": "HTML_IMPORTADO",
            "arquivo": nome_arquivo,
            "total_skus": len(df_temp['sku'].dropna().unique()),
            "skus_detalhado": {}
        }

        # Agrupar por SKU e pegar o saldo da linha TOTAL ou último saldo
        for sku in df_temp['sku'].dropna().unique():
            df_sku = df_temp[df_temp['sku'] == sku].copy()
            if not df_sku.empty:
                # Tentar pegar saldo da linha TOTAL
                mask_total = df_sku['tipo'].astype(str).str.strip().str.upper().isin(['TOTAL', 'TOTAL:'])
                if mask_total.any():
                    saldo_valor = float(df_sku[mask_total].iloc[0]['saldo'])
                else:
                    # Pegar último saldo
                    if 'data_movimento' in df_sku.columns:
                        df_sku['data_dt'] = pd.to_datetime(df_sku['data_movimento'], errors='coerce')
                        df_sku = df_sku.sort_values(by='data_dt', ascending=False)
                    saldo_valor = float(df_sku.iloc[0]['saldo'])

                produto = df_sku.iloc[0].get('produto', '')
                categoria = df_sku.iloc[0].get('categoria', '')
                cat_auto = df_sku.iloc[0].get('Cat_Auto', '')

                log_importacao["skus_detalhado"][str(sku)] = {
                    "sku": str(sku),
                    "produto": str(produto),
                    "categoria": str(categoria),
                    "cat_auto": str(cat_auto),
                    "saldo_m3": float(saldo_valor),
                    "saldo_m3_formatado": formatar_br(saldo_valor)
                }

        # Salvar log
        os.makedirs("logs", exist_ok=True)
        log_file = os.path.join("logs", f"plenus_import_html_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        with open(log_file, 'w', encoding='utf-8') as f:
            json.dump(log_importacao, f, indent=2, ensure_ascii=False)

    return df_temp, lista_erros_detalhada
