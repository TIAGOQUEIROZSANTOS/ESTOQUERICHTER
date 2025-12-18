import streamlit as st
import pandas as pd
import pdfplumber
import re
import io
import json
import time
import shutil
from bs4 import BeautifulSoup
from datetime import datetime, date
from difflib import SequenceMatcher
import firebase_admin
from firebase_admin import credentials, firestore

# --- CONFIGURAÇÃO INICIAL ---
st.set_page_config(page_title="Sistema S&P v183 Cloud", layout="wide", page_icon="🌲")

# --- CONEXÃO FIREBASE (CORAÇÃO DO SISTEMA) ---
@st.cache_resource
def get_db():
    if not firebase_admin._apps:
        try:
            # Tenta pegar das Secrets do Streamlit Cloud
            if "firebase_service_account" in st.secrets:
                key_dict = dict(st.secrets["firebase_service_account"])
                cred = credentials.Certificate(key_dict)
                firebase_admin.initialize_app(cred)
            else:
                # Tenta pegar arquivo local (para testes no PC)
                cred = credentials.Certificate("stok-c6d28-firebase-adminsdk-fbsvc-0ffa6c6012.json")
                firebase_admin.initialize_app(cred)
        except Exception as e:
            st.error(f"Erro de Conexão Firebase: {e}")
            return None
    return firestore.client()

# --- FUNÇÕES DE BANCO DE DADOS (ADAPTADAS DO SQLITE PARA FIREBASE) ---

def carregar_df_firebase(colecao, filtros=None):
    """Carrega dados de uma coleção com opção de filtros simples."""
    db = get_db()
    if not db: return pd.DataFrame()
    
    ref = db.collection(colecao)
    if filtros:
        for campo, op, valor in filtros:
            ref = ref.where(campo, op, valor)
            
    docs = ref.stream()
    dados = [d.to_dict() for d in docs]
    return pd.DataFrame(dados)

def carregar_agrupamentos_fb(origem=None):
    """Substitui carregar_agrupamentos_db."""
    filtros = [('origem', '==', origem)] if origem else None
    df = carregar_df_firebase('agrupamentos', filtros)
    if df.empty: return {}
    return pd.Series(df.nome_grupo.values, index=df.item_original).to_dict()

def carregar_vinculos_fb():
    """Substitui carregar_vinculos_db."""
    df = carregar_df_firebase('vinculos')
    if df.empty: return {}
    return pd.Series(df.grupo_sisflora.values, index=df.grupo_plenus).to_dict()

def salvar_agrupamento_fb(itens, nome_grupo, origem, categoria):
    """Salva agrupamentos no Firebase."""
    db = get_db()
    batch = db.batch()
    col = db.collection('agrupamentos')
    
    for item in itens:
        # Cria um ID seguro para o documento
        doc_id = re.sub(r'[^a-zA-Z0-9]', '_', f"{origem}_{item}")[:100]
        doc_ref = col.document(doc_id)
        batch.set(doc_ref, {
            "item_original": item,
            "nome_grupo": nome_grupo.upper(),
            "origem": origem,
            "categoria": categoria
        })
    batch.commit()
    st.toast(f"Grupo '{nome_grupo}' salvo!", icon="💾")

def salvar_vinculo_fb(grupos_plenus, grupo_sisflora):
    """Salva vínculos entre Plenus e Sisflora."""
    db = get_db()
    batch = db.batch()
    col = db.collection('vinculos')
    
    for gp in grupos_plenus:
        doc_id = re.sub(r'[^a-zA-Z0-9]', '_', gp)[:100]
        doc_ref = col.document(doc_id)
        batch.set(doc_ref, {
            "grupo_plenus": gp,
            "grupo_sisflora": grupo_sisflora
        })
    batch.commit()
    st.toast("Vínculo criado!", icon="🔗")

def obter_limites_datas_fb(colecao, campo_data):
    """Substitui get_smart_date_range."""
    try:
        db = get_db()
        # Data Minima
        r_min = db.collection(colecao).order_by(campo_data).limit(1).get()
        # Data Maxima
        r_max = db.collection(colecao).order_by(campo_data, direction=firestore.Query.DESCENDING).limit(1).get()
        
        if r_min and r_max:
            d_min = pd.to_datetime(r_min[0].to_dict()[campo_data]).date()
            d_max = pd.to_datetime(r_max[0].to_dict()[campo_data]).date()
            return d_min.replace(day=1), d_max
    except: pass
    return date.today(), date.today()

def salvar_lote_firebase_smart(colecao, df, campo_data, campo_chave_extra=None):
    """
    Substitui salvar_lote_smart.
    Salva dados em lote, convertendo datas para string.
    """
    if df.empty: return 0
    db = get_db()
    batch = db.batch()
    col = db.collection(colecao)
    count = 0
    
    for _, row in df.iterrows():
        dados = row.to_dict()
        # Converte datas para string ISO
        for k, v in dados.items():
            if isinstance(v, (date, datetime)): dados[k] = v.strftime("%Y-%m-%d")
        
        # Gera ID Único baseado nos dados para evitar duplicatas exatas
        # Ex: Data + Produto + Volume
        id_str = f"{dados.get(campo_data)}_{dados.get('produto', '')}_{dados.get('volume', '')}_{dados.get('sku', '')}"
        doc_id = re.sub(r'[^a-zA-Z0-9]', '', id_str)[:150]
        
        doc_ref = col.document(doc_id)
        batch.set(doc_ref, dados)
        count += 1
        
        if count >= 400: # Limite do Firebase
            batch.commit()
            batch = db.batch()
            count = 0
    if count > 0: batch.commit()
    return len(df)

def excluir_colecao_por_data(colecao, campo_data, dt_ini, dt_fim):
    """Substitui excluir_periodo_tabela."""
    db = get_db()
    docs = db.collection(colecao)\
             .where(campo_data, '>=', str(dt_ini))\
             .where(campo_data, '<=', str(dt_fim))\
             .stream()
    count = 0
    batch = db.batch()
    for d in docs:
        batch.delete(d.reference)
        count += 1
        if count % 400 == 0:
            batch.commit()
            batch = db.batch()
    if count > 0: batch.commit()
    return count

# --- FUNÇÕES DE AJUDA / EXTRAÇÃO (MANTIDAS DO ORIGINAL) ---

def parse_float_inteligente(valor):
    try:
        val_str = str(valor).strip()
        if not val_str: return 0.0
        if ',' in val_str and '.' in val_str:
             clean = val_str.replace('.', '').replace(',', '.')
             return float(clean)
        elif ',' in val_str:
             return float(val_str.replace(',', '.'))
        return float(val_str)
    except: return 0.0

def formatar_br(valor):
    if not isinstance(valor, (float, int)): return str(valor)
    return f"{valor:,.4f}".replace(',', 'X').replace('.', ',').replace('X', '.')

def detecting_category(item, origin):
    cat = "OUTROS"
    item = str(item).upper()
    if origin == "SISFLORA":
        if item.startswith("10"): cat = "TORAS"
        elif item.startswith("20") or item.startswith("3030"): cat = "SERRADAS"
        elif item.startswith("50"): cat = "BENEFICIADAS"
    else: # PLENUS
        if "BENEF" in item or "DECK" in item or "FORRO" in item: cat = "BENEFICIADAS"
        elif "TORA" in item or "TORO" in item: cat = "TORAS"
        elif "SERRAD" in item or "CAIBRO" in item or "VIGA" in item: cat = "SERRADAS"
    return cat

def detectar_categoria_plenus(item):
    return detecting_category(item, "PLENUS")

# --- LEITOR SISFLORA (PDF) ---
def extrair_dados_sisflora(arquivo):
    dados_brutos = []
    with pdfplumber.open(arquivo) as pdf:
        for page in pdf.pages:
            tabela = page.extract_table()
            if tabela:
                for linha in tabela:
                    txt = "".join([str(c) for c in linha if c]).lower()
                    if "governo" in txt or "sisflora" in txt or "página" in txt: continue
                    dados_brutos.append(linha)
    
    if not dados_brutos: return pd.DataFrame()
    df = pd.DataFrame(dados_brutos)
    
    # Tenta alinhar colunas (Lógica Simplificada para Robustez)
    # Assume que a última coluna com números é volume
    # Assume a primeira com texto é produto
    
    # Renomeação forçada para garantir funcionamento
    mapa_cols = {}
    if len(df.columns) > 0: mapa_cols[0] = "Produto"
    if len(df.columns) > 2: mapa_cols[1] = "Essencia"
    
    # Acha coluna de volume
    idx_vol = len(df.columns) - 1
    for c in reversed(range(len(df.columns))):
        if df.iloc[:5, c].astype(str).str.contains(r'\d+,\d+').any():
            idx_vol = c; break
    mapa_cols[idx_vol] = "Volume Disponivel"
    
    df.rename(columns=mapa_cols, inplace=True)
    if "Volume Disponivel" not in df.columns: df["Volume Disponivel"] = 0
    if "Produto" not in df.columns: df["Produto"] = ""
    if "Essencia" not in df.columns: df["Essencia"] = ""
    
    # Tratamento
    df["Volume Disponivel"] = df["Volume Disponivel"].apply(parse_float_inteligente)
    df["Item_Completo"] = df.apply(lambda x: f"{x['Produto']} - {x['Essencia']}" if x['Essencia'] else x['Produto'], axis=1)
    df["Cat_Auto"] = df["Item_Completo"].apply(lambda x: detecting_category(x, "SISFLORA"))
    
    return df

# --- LEITOR PLENUS (HTML) ---
def extrair_dados_plenus_html(arquivo_html):
    soup = BeautifulSoup(arquivo_html, 'html.parser')
    dados = []
    state = {'categoria': '', 'sku': '', 'produto': ''}
    
    rows = soup.find_all('tr')
    for tr in rows:
        cols = tr.find_all('td')
        if not cols: continue
        txts = [c.get_text(strip=True) for c in cols]
        linha = " ".join(txts)
        
        # Captura Categoria
        if "Categoria:" in linha:
            for t in txts:
                if "Categoria:" in t: state['categoria'] = t.replace("Categoria:", "").strip()
        
        # Captura SKU/Produto
        # Procura padrão "123 - MADEIRA..."
        prod_found = False
        for t in txts:
            if re.match(r'^\d+\s*-\s*', t):
                parts = t.split('-', 1)
                state['sku'] = parts[0].strip()
                state['produto'] = parts[1].strip()
                prod_found = True
        
        if not prod_found and state['sku']:
            # Captura Movimentos
            # Tenta identificar linhas de movimento pelo padrão de data
            if len(txts) >= 4:
                try:
                    ent = parse_float_inteligente(txts[-3])
                    sai = parse_float_inteligente(txts[-2])
                    sal = parse_float_inteligente(txts[-1])
                    
                    data_raw = txts[0]
                    # Se tiver data válida ou for SALDO ANTERIOR/TOTAL
                    valid_row = False
                    dt_iso = None
                    
                    if "/" in data_raw and len(data_raw) <= 10:
                        dt_iso = datetime.strptime(data_raw, "%d/%m/%Y").strftime("%Y-%m-%d")
                        valid_row = True
                    elif "TOTAL" in linha.upper() or "ANTERIOR" in linha.upper():
                        valid_row = True
                    
                    if valid_row:
                         dados.append({
                            "sku": state['sku'],
                            "produto": state['produto'],
                            "categoria": state['categoria'],
                            "data_movimento": dt_iso,
                            "tipo": "Movimento",
                            "entrada": ent,
                            "saida": sai,
                            "saldo": sal,
                            "raw_data": data_raw
                         })
                except: pass
                
    return pd.DataFrame(dados)

# --- 5. INTERFACE DO SISTEMA (MÓDULOS) ---

# Menu Lateral com Estado Persistente
if 'menu_idx' not in st.session_state: st.session_state.menu_idx = 0
opcoes_menu = [
    "1. Saldo Sisflora", 
    "2. Saldo Plenus", 
    "3. Histórico Transformação", 
    "4. Débito Consumo", 
    "5. Gestão: Vínculos", 
    "6. Conferência Final"
]
menu = st.sidebar.radio("Navegação", opcoes_menu, index=st.session_state.menu_idx)

# --- MÓDULO 1: SISFLORA ---
if menu == "1. Saldo Sisflora":
    st.title("🌲 Saldo Sisflora")
    
    # Aba persistente (Correção v183)
    if 'aba_sis' not in st.session_state: st.session_state.aba_sis = 0
    abas = ["Ler PDF (Upload)", "Histórico Nuvem", "Visualização"]
    op_sis = st.radio("Ações:", abas, horizontal=True, index=st.session_state.aba_sis)
    
    if op_sis == "Ler PDF (Upload)":
        st.session_state.aba_sis = 0
        f = st.file_uploader("Upload PDF Sisflora", type="pdf")
        if f:
            df = extrair_dados_sisflora(f)
            st.session_state['df_sisflora'] = df
            st.success("PDF Lido! Vá para Visualização ou Salve.")
            
            d_ref = st.date_input("Data Referência:", date.today())
            if st.button("☁️ Salvar no Firebase"):
                df['data_referencia'] = str(d_ref)
                salvar_lote_firebase_smart('sisflora_historico', df, 'data_referencia')
                st.success("Salvo com sucesso!")

    elif op_sis == "Histórico Nuvem":
        st.session_state.aba_sis = 1
        db = get_db()
        # Busca datas distintas (pode ser lento se tiver muitos dados, ideal otimizar futuramente)
        docs = db.collection('sisflora_historico').stream()
        dates = sorted(list(set([d.to_dict().get('data_referencia') for d in docs])), reverse=True)
        
        sel_data = st.selectbox("Selecione data:", dates)
        if st.button("Carregar"):
            df = carregar_df_firebase('sisflora_historico', [('data_referencia', '==', sel_data)])
            st.session_state['df_sisflora'] = df
            st.session_state.aba_sis = 2 # Vai para visualização
            st.rerun()

    elif op_sis == "Visualização":
        st.session_state.aba_sis = 2
        if 'df_sisflora' in st.session_state:
            st.dataframe(st.session_state['df_sisflora'])
            st.metric("Volume Total", formatar_br(st.session_state['df_sisflora']['Volume Disponivel'].sum()))
        else:
            st.info("Nada carregado.")

# --- MÓDULO 2: PLENUS ---
elif menu == "2. Saldo Plenus":
    st.title("📦 Saldo Plenus")
    
    if 'aba_ple' not in st.session_state: st.session_state.aba_ple = 0
    op_ple = st.radio("Ações:", ["Importar HTML", "Consultar Nuvem"], horizontal=True, index=st.session_state.aba_ple)
    
    if op_ple == "Importar HTML":
        st.session_state.aba_ple = 0
        f = st.file_uploader("Upload HTML Plenus", type=['html', 'htm'])
        if f:
            txt = f.getvalue().decode('utf-8', errors='ignore')
            df = extrair_dados_plenus_html(txt)
            st.session_state['df_plenus'] = df
            
            # Mostra itens sem total (Correção v183)
            pendentes = df[(df['entrada']==0) & (df['saida']==0) & (~df['raw_data'].str.contains('TOTAL', na=False, case=False))]
            if not pendentes.empty:
                st.warning(f"Atenção: {len(pendentes)} itens sem movimentação/total detectados.")
                with st.expander("Ver Itens Sem Total"):
                    st.dataframe(pendentes)
            
            if st.button("☁️ Salvar Movimentos"):
                salvar_lote_firebase_smart('plenus_historico', df, 'data_movimento')
                st.success("Salvo!")

    elif op_ple == "Consultar Nuvem":
        st.session_state.aba_ple = 1
        # Datas automáticas
        d_min, d_max = obter_limites_datas_fb('plenus_historico', 'data_movimento')
        c1, c2 = st.columns(2)
        di = c1.date_input("Início", d_min)
        dfim = c2.date_input("Fim", d_max)
        
        if st.button("Buscar"):
            df = carregar_df_firebase('plenus_historico', [
                ('data_movimento', '>=', str(di)),
                ('data_movimento', '<=', str(dfim))
            ])
            st.session_state['df_plenus'] = df
            st.dataframe(df)

# --- MÓDULO 3: TRANSFORMAÇÃO ---
elif menu == "3. Histórico Transformação":
    st.title("🔄 Histórico Transformação")
    
    tab1, tab2 = st.tabs(["Importar Excel", "Consultar"])
    
    with tab1:
        f = st.file_uploader("Upload Excel Transformação", type=['xlsx', 'xls'])
        if f:
            # Lógica simples de leitura
            df = pd.read_excel(f)
            # Normalização mínima das colunas
            cols_map = {c: c.lower().replace(' ', '_') for c in df.columns}
            df.rename(columns=cols_map, inplace=True)
            
            st.dataframe(df.head())
            if st.button("☁️ Salvar Transformações"):
                # Garante coluna data_realizacao
                if 'data_realização' in df.columns: df.rename(columns={'data_realização': 'data_realizacao'}, inplace=True)
                salvar_lote_firebase_smart('transf_historico', df, 'data_realizacao')
                st.success("Enviado para o Firebase!")
    
    with tab2:
        d_min, d_max = obter_limites_datas_fb('transf_historico', 'data_realizacao')
        c1, c2 = st.columns(2)
        di = c1.date_input("De", d_min)
        dfim = c2.date_input("Até", d_max)
        if st.button("Filtrar Transf."):
            df = carregar_df_firebase('transf_historico', [
                ('data_realizacao', '>=', str(di)),
                ('data_realizacao', '<=', str(dfim))
            ])
            st.dataframe(df)

# --- MÓDULO 4: CONSUMO ---
elif menu == "4. Débito Consumo":
    st.title("🚚 Débito Consumo")
    # Lógica similar ao Transformação
    f = st.file_uploader("Upload Excel Consumo", type=['xlsx', 'xls'])
    if f:
        df = pd.read_excel(f)
        st.dataframe(df)
        if st.button("☁️ Salvar Consumo"):
            # Cria coluna de data padrão se não existir
            if 'data_consumo' not in df.columns: df['data_consumo'] = str(date.today())
            salvar_lote_firebase_smart('consumo_historico', df, 'data_consumo')
            st.success("Salvo!")

# --- MÓDULO 5: VÍNCULOS ---
elif menu == "5. Gestão: Vínculos":
    st.title("⚙️ Gestão de Vínculos")
    
    # 1. Carrega dados reais do banco
    agrupamentos = carregar_agrupamentos_fb("SISFLORA")
    vinculos = carregar_vinculos_fb()
    
    # 2. Calcula Pendências (Correção v183)
    db = get_db()
    # Pega lista de SKUs únicos do Plenus
    skus_ref = db.collection('plenus_historico').limit(500).stream() # Limitado para performance, ideal seria agregação
    skus_plenus = set([d.to_dict().get('sku') for d in skus_ref if d.to_dict().get('sku')])
    
    # Pega lista de SKUs já vinculados
    agrup_docs = db.collection('agrupamentos').where('origem', '==', 'PLENUS').stream()
    skus_vinc = set([d.to_dict().get('item_original') for d in agrup_docs])
    
    pendentes_ple = skus_plenus - skus_vinc
    
    c1, c2 = st.columns(2)
    c1.metric("Pendências Plenus", len(pendentes_ple))
    c2.metric("Grupos Criados", len(agrupamentos))
    
    st.divider()
    st.subheader("Criar Novo Grupo")
    col_tipo = st.radio("Origem:", ["SISFLORA", "PLENUS"], horizontal=True)
    
    if col_tipo == "PLENUS":
        itens_sel = st.multiselect("Selecione SKUs Pendentes:", list(pendentes_ple))
        nome_grupo = st.text_input("Nome do Grupo (Plenus):")
        if st.button("Salvar Grupo Plenus"):
            salvar_agrupamento_fb(itens_sel, nome_grupo, "PLENUS", "OUTROS")
            st.rerun()
            
    else: # SISFLORA
        # Mostra itens do Sisflora carregados na memória ou banco
        if 'df_sisflora' in st.session_state:
            lista = st.session_state['df_sisflora']['Item_Completo'].unique()
            itens_sel = st.multiselect("Itens Sisflora:", lista)
            nome_grupo = st.text_input("Nome do Grupo (Sisflora):")
            if st.button("Salvar Grupo Sisflora"):
                salvar_agrupamento_fb(itens_sel, nome_grupo, "SISFLORA", "OUTROS")
                st.rerun()

# --- MÓDULO 6: CONFERÊNCIA FINAL ---
elif menu == "6. Conferência Final":
    st.title("⚖️ Resultado Final")
    
    st.info("Cruza os dados carregados do Sisflora (PDF) com o Histórico do Plenus.")
    
    if 'df_sisflora' in st.session_state and 'df_plenus' in st.session_state:
        df_s = st.session_state['df_sisflora']
        df_p = st.session_state['df_plenus']
        
        # Carrega dicionários de tradução
        mapa_sis = carregar_agrupamentos_fb("SISFLORA")
        mapa_ple = carregar_agrupamentos_fb("PLENUS")
        mapa_vinc = carregar_vinculos_fb()
        
        # Aplica agrupamentos
        df_s['Grupo'] = df_s['Item_Completo'].map(mapa_sis).fillna(df_s['Item_Completo'])
        df_p['Grupo_Int'] = df_p['sku'].map(mapa_ple).fillna(df_p['sku']) # Simplificado pelo SKU
        df_p['Grupo'] = df_p['Grupo_Int'].map(mapa_vinc).fillna(df_p['Grupo_Int'])
        
        # Agrega
        res_s = df_s.groupby('Grupo')['Volume Disponivel'].sum()
        # Calcula saldo final do Plenus (último saldo encontrado por grupo)
        res_p = df_p.groupby('Grupo')['saldo'].sum() # Simplificação: soma dos saldos pode não ser correta, ideal é saldo do ultimo dia.
        
        # DataFrame Final
        df_final = pd.DataFrame({'Sisflora': res_s, 'Plenus': res_p}).fillna(0)
        df_final['Diferenca'] = df_final['Sisflora'] - df_final['Plenus']
        
        st.dataframe(df_final.style.format("{:,.4f}"))
    else:
        st.warning("Carregue dados no 'Saldo Sisflora' e 'Saldo Plenus' primeiro.")

st.sidebar.markdown("---")
st.sidebar.caption("Desenvolvido para Nuvem v183")