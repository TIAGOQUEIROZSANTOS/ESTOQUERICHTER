import streamlit as st
import pandas as pd
import pdfplumber
import plotly.express as px
import re
import io
import json
import time
from bs4 import BeautifulSoup
from datetime import datetime, date
from difflib import SequenceMatcher
import firebase_admin
from firebase_admin import credentials, firestore

# --- CONFIGURAÇÃO DA PÁGINA ---
st.set_page_config(page_title="Sistema S&P v185 Master", layout="wide", page_icon="🌲")

# --- CSS E ESTILOS VISUAIS ---
st.markdown("""
    <style>
    .block-container { padding-top: 1rem; }
    .stDataFrame { overscroll-behavior: contain; }
    div[data-testid="stMetricValue"] { font-size: 1.3rem !important; }
    .stTabs [data-baseweb="tab-list"] { gap: 10px; }
    .stTabs [data-baseweb="tab"] {
        height: 50px; white-space: pre-wrap; background-color: #f0f2f6; border-radius: 4px 4px 0 0; gap: 1px; padding-top: 10px; padding-bottom: 10px;
    }
    .stTabs [aria-selected="true"] { background-color: #e8f0fe; border-bottom: 2px solid #4285f4; }
    </style>
""", unsafe_allow_html=True)

# --- 1. MOTOR DE BANCO DE DADOS (FIREBASE) ---
@st.cache_resource
def get_db():
    if not firebase_admin._apps:
        try:
            if "firebase_service_account" in st.secrets:
                key_dict = dict(st.secrets["firebase_service_account"])
                cred = credentials.Certificate(key_dict)
                firebase_admin.initialize_app(cred)
            else:
                cred = credentials.Certificate("stok-c6d28-firebase-adminsdk-fbsvc-0ffa6c6012.json")
                firebase_admin.initialize_app(cred)
        except Exception as e:
            st.error(f"Erro Crítico de Conexão: {e}")
            return None
    return firestore.client()

# --- FUNÇÕES AUXILIARES DE FORMATAÇÃO ---
def parse_float_inteligente(valor):
    try:
        val_str = str(valor).strip()
        if not val_str: return 0.0
        if ',' in val_str and '.' in val_str:
             clean = val_str.replace('.', '').replace(',', '.')
             return float(clean)
        elif ',' in val_str: return float(val_str.replace(',', '.'))
        return float(val_str)
    except: return 0.0

def formatar_br(valor):
    if pd.isna(valor) or valor == "": return "0,0000"
    try:
        val = float(valor)
        return f"{val:,.4f}".replace(',', 'X').replace('.', ',').replace('X', '.')
    except: return str(valor)

# --- FUNÇÕES DE BANCO DE DADOS (CRUD FIREBASE) ---
def carregar_colecao(colecao, filtros=None, limite=None):
    db = get_db()
    if not db: return pd.DataFrame()
    ref = db.collection(colecao)
    if filtros:
        for campo, op, valor in filtros:
            if isinstance(valor, (date, datetime)): valor = str(valor)
            ref = ref.where(campo, op, valor)
    if limite: ref = ref.limit(limite)
    
    docs = ref.stream()
    return pd.DataFrame([d.to_dict() for d in docs])

def salvar_lote_firebase(colecao, df, campo_data_nome):
    if df.empty: return 0
    db = get_db()
    batch = db.batch()
    col = db.collection(colecao)
    count = 0
    total = 0
    
    for _, row in df.iterrows():
        dados = row.to_dict()
        # Higienização de Datas
        for k, v in dados.items():
            if isinstance(v, (date, datetime)): dados[k] = str(v)
        
        # ID Único Robusto
        chave_raw = f"{dados.get(campo_data_nome)}_{dados.get('sku', '')}_{dados.get('produto', '')}_{dados.get('volume', '')}"
        doc_id = re.sub(r'[^a-zA-Z0-9]', '', chave_raw)[:150]
        
        doc_ref = col.document(doc_id)
        batch.set(doc_ref, dados)
        count += 1
        
        if count >= 400:
            batch.commit()
            total += count
            batch = db.batch()
            count = 0
            time.sleep(0.5)
            
    if count > 0:
        batch.commit()
        total += count
    return total

def carregar_agrupamentos():
    df = carregar_colecao('agrupamentos')
    if df.empty: return {}
    return pd.Series(df.nome_grupo.values, index=df.item_original).to_dict()

def carregar_vinculos():
    df = carregar_colecao('vinculos')
    if df.empty: return {}
    return pd.Series(df.grupo_sisflora.values, index=df.grupo_plenus).to_dict()

def salvar_vinculo_novo(g_ple, g_sis):
    db = get_db()
    ref = db.collection('vinculos').document(re.sub(r'[^a-zA-Z0-9]', '_', g_ple))
    ref.set({'grupo_plenus': g_ple, 'grupo_sisflora': g_sis})

def salvar_grupo_novo(itens, nome, origem, cat):
    db = get_db()
    batch = db.batch()
    col = db.collection('agrupamentos')
    for item in itens:
        doc_id = re.sub(r'[^a-zA-Z0-9]', '_', f"{origem}_{item}")[:100]
        ref = col.document(doc_id)
        batch.set(ref, {'item_original': item, 'nome_grupo': nome.upper(), 'origem': origem, 'categoria': cat})
    batch.commit()

# --- INTELIGÊNCIA DE DATAS (V183/185) ---
def get_smart_dates(colecao, campo):
    try:
        db = get_db()
        r_min = db.collection(colecao).order_by(campo).limit(1).get()
        r_max = db.collection(colecao).order_by(campo, direction=firestore.Query.DESCENDING).limit(1).get()
        if r_min and r_max:
            d1 = pd.to_datetime(r_min[0].to_dict()[campo]).date()
            d2 = pd.to_datetime(r_max[0].to_dict()[campo]).date()
            return d1, d2
    except: pass
    return date(2023, 1, 1), date.today()

# --- LEITORES DE ARQUIVO (DO V176 ORIGINAL) ---
def extrair_dados_sisflora(arquivo):
    dados = []
    with pdfplumber.open(arquivo) as pdf:
        for page in pdf.pages:
            tabela = page.extract_table()
            if tabela:
                for linha in tabela:
                    txt = "".join([str(c) for c in linha if c]).lower()
                    if "governo" not in txt and "sisflora" not in txt:
                        dados.append(linha)
    
    if not dados: return pd.DataFrame()
    df = pd.DataFrame(dados)
    
    # Lógica de Mapeamento V184 (Corrige o KeyError)
    mapa = {}
    if len(df.columns) > 0: mapa[0] = "Produto"
    if len(df.columns) > 2: mapa[1] = "Essencia"
    
    idx_vol = len(df.columns) - 1
    for c in reversed(range(len(df.columns))):
        if df.iloc[:5, c].astype(str).str.contains(r'\d+,\d+').any():
            idx_vol = c; break
    mapa[idx_vol] = "Volume Disponivel"
    
    df.rename(columns=mapa, inplace=True)
    
    cols_req = ["Produto", "Essencia", "Volume Disponivel", "Unidade"]
    for c in cols_req: 
        if c not in df.columns: df[c] = ""
        
    df["Volume Disponivel"] = df["Volume Disponivel"].apply(parse_float_inteligente)
    df["Item_Completo"] = df.apply(lambda x: f"{x['Produto']} - {x['Essencia']}" if x['Essencia'] else x['Produto'], axis=1)
    
    def cat_auto(t):
        t = str(t).upper()
        if "TORA" in t: return "TORAS"
        elif "SERRADA" in t: return "SERRADAS"
        elif "BENEF" in t: return "BENEFICIADAS"
        return "OUTROS"
    df["Cat_Auto"] = df["Item_Completo"].apply(cat_auto)
    
    # CORREÇÃO CRÍTICA PARA O VISUAL
    # Padroniza nomes de colunas para o banco
    df.rename(columns={'Volume Disponivel': 'volume_disponivel', 'Produto': 'produto', 'Essencia': 'essencia', 'Unidade': 'unidade', 'Codigo': 'codigo', 'Cat_Auto': 'cat_auto'}, inplace=True)
    return df

def extrair_dados_plenus_html(arquivo_html):
    soup = BeautifulSoup(arquivo_html, 'html.parser')
    dados = []
    state = {'cat': '', 'sku': '', 'prod': ''}
    
    for tr in soup.find_all('tr'):
        cols = [c.get_text(strip=True) for c in tr.find_all('td')]
        if not cols: continue
        linha = " ".join(cols)
        
        if "Categoria:" in linha: 
            state['cat'] = linha.replace("Categoria:", "").strip()
        
        for t in cols:
            if re.match(r'^\d+\s*-\s*', t):
                parts = t.split('-', 1)
                state['sku'] = parts[0].strip()
                state['prod'] = parts[1].strip()
        
        if len(cols) >= 4 and state['sku']:
            try:
                ent = parse_float_inteligente(cols[-3])
                sai = parse_float_inteligente(cols[-2])
                sal = parse_float_inteligente(cols[-1])
                data = cols[0]
                
                dt_iso = None
                if "/" in data and len(data)<=10:
                    dt_iso = datetime.strptime(data, "%d/%m/%Y").strftime("%Y-%m-%d")
                elif "ANTERIOR" in linha.upper() or "TOTAL" in linha.upper():
                    dt_iso = "RESUMO"

                if dt_iso and dt_iso != "RESUMO":
                    dados.append({
                        "sku": state['sku'], "produto": state['prod'], "categoria": state['cat'],
                        "data_movimento": dt_iso, "entrada": ent, "saida": sai, "saldo": sal,
                        "tipo": "Movimento", "raw_data": data
                    })
            except: pass
    return pd.DataFrame(dados)

# --- MÓDULOS DE UI (INTERFACE) ---

def modulo_sisflora():
    st.title("🌲 Saldo Sisflora")
    
    # CONTROLE DE ABA PERSISTENTE (Do V183)
    if 'aba_sis' not in st.session_state: st.session_state.aba_sis = 0
    abas = ["Ler PDF (Upload)", "Histórico Nuvem", "Visualização"]
    op_sis = st.radio("Ações:", abas, horizontal=True, index=st.session_state.aba_sis, key="nav_sis_rad")
    
    if op_sis == "Ler PDF (Upload)":
        st.session_state.aba_sis = 0
        f = st.file_uploader("PDF Sisflora", type="pdf")
        if f:
            df = extrair_dados_sisflora(f)
            st.session_state['df_sisflora'] = df
            st.success("PDF Processado com Sucesso!")
            
            # Preview Rápido
            st.dataframe(df.head())
            
            c1, c2 = st.columns(2)
            d_ref = c1.date_input("Data de Referência:", date.today())
            if c2.button("☁️ SALVAR NO BANCO DE DADOS"):
                df['data_referencia'] = str(d_ref)
                total = salvar_lote_firebase('sisflora_historico', df, 'data_referencia')
                st.success(f"{total} registros salvos na nuvem!")
                st.balloons()

    elif op_sis == "Histórico Nuvem":
        st.session_state.aba_sis = 1
        db = get_db()
        # Otimização: Buscar apenas datas distintas (via agregação manual ou limit query)
        docs = db.collection('sisflora_historico').stream()
        dates = sorted(list(set([d.to_dict().get('data_referencia') for d in docs])), reverse=True)
        
        sel_data = st.selectbox("Selecione a Data Salva:", dates)
        if st.button("Carregar Data Selecionada"):
            df = carregar_colecao('sisflora_historico', [('data_referencia', '==', sel_data)])
            st.session_state['df_sisflora'] = df
            st.session_state.aba_sis = 2
            st.rerun()

    elif op_sis == "Visualização":
        st.session_state.aba_sis = 2
        if 'df_sisflora' in st.session_state:
            df = st.session_state['df_sisflora'].copy()
            
            # TRADUÇÃO DE COLUNAS PARA VISUALIZAÇÃO
            mapa_vis = {
                'volume_disponivel': 'Volume Disponivel',
                'produto': 'Produto',
                'essencia': 'Essencia',
                'unidade': 'Unidade',
                'cat_auto': 'Categoria Auto'
            }
            df.rename(columns=mapa_vis, inplace=True)
            
            # Barra de Filtros
            c1, c2, c3 = st.columns([2, 1, 1])
            txt = c1.text_input("🔎 Pesquisar:")
            cat = c2.multiselect("Categoria:", df['Categoria Auto'].unique() if 'Categoria Auto' in df.columns else [])
            
            if txt:
                mask = df.astype(str).apply(lambda x: x.str.contains(txt, case=False)).any(axis=1)
                df = df[mask]
            if cat:
                df = df[df['Categoria Auto'].isin(cat)]
                
            vol_total = df['Volume Disponivel'].sum() if 'Volume Disponivel' in df.columns else 0
            st.metric("Volume Total (m³)", formatar_br(vol_total))
            
            # Formatação Numérica na Tabela
            cols_num = ['Volume Disponivel']
            st.dataframe(df.style.format({c: formatar_br for c in cols_num if c in df.columns}), use_container_width=True, height=600)
        else:
            st.info("Nenhum dado carregado. Faça upload ou carregue do histórico.")

def modulo_plenus():
    st.title("📦 Saldo Plenus")
    
    if 'aba_ple' not in st.session_state: st.session_state.aba_ple = 0
    op_ple = st.radio("Ações:", ["Importar HTML", "Consultar Nuvem"], horizontal=True, index=st.session_state.aba_ple)
    
    if op_ple == "Importar HTML":
        st.session_state.aba_ple = 0
        f = st.file_uploader("Arquivo HTML Plenus", type=['html', 'htm'])
        if f:
            df = extrair_dados_plenus_html(f.getvalue().decode('utf-8', errors='ignore'))
            st.session_state['df_plenus'] = df
            
            # Verificação de Erros (Itens sem total)
            pendentes = df[(df['entrada']==0) & (df['saida']==0)]
            if not pendentes.empty:
                st.warning(f"⚠️ Atenção: {len(pendentes)} itens estão sem movimentação ou total.")
                with st.expander("Ver itens sem total"):
                    st.dataframe(pendentes)
            
            if st.button("☁️ SALVAR MOVIMENTOS"):
                total = salvar_lote_firebase('plenus_historico', df, 'data_movimento')
                st.success(f"{total} movimentos salvos!")

    elif op_ple == "Consultar Nuvem":
        st.session_state.aba_ple = 1
        d_min, d_max = get_smart_dates('plenus_historico', 'data_movimento')
        
        st.markdown("##### Filtro de Período")
        c1, c2 = st.columns(2)
        di = c1.date_input("Data Inicial:", d_min)
        dfim = c2.date_input("Data Final:", d_max)
        
        if st.button("🔍 Buscar Movimentação"):
            df = carregar_colecao('plenus_historico', [
                ('data_movimento', '>=', str(di)),
                ('data_movimento', '<=', str(dfim))
            ])
            st.session_state['df_plenus'] = df
    
    if 'df_plenus' in st.session_state:
        df = st.session_state['df_plenus'].copy()
        
        st.markdown("---")
        c1, c2, c3 = st.columns([2, 1, 1])
        txt = c1.text_input("Filtrar Produto:")
        cats = sorted(df['categoria'].unique()) if 'categoria' in df.columns else []
        cat_sel = c2.multiselect("Categoria:", cats)
        
        if txt: df = df[df['produto'].str.contains(txt, case=False, na=False)]
        if cat_sel: df = df[df['categoria'].isin(cat_sel)]
        
        st.caption(f"Exibindo {len(df)} registros.")
        
        # Formatação
        cols_fmt = ['entrada', 'saida', 'saldo']
        st.dataframe(df.style.format({c: formatar_br for c in cols_fmt if c in df.columns}), use_container_width=True, height=600)

def modulo_admin():
    st.title("⚙️ Gestão de Vínculos e Grupos")
    st.info("Aqui você define quais itens do Sisflora equivalem aos itens do Plenus.")
    
    # Métricas Reais do Banco
    db = get_db()
    skus_ref = db.collection('plenus_historico').limit(2000).stream()
    skus_plenus = set([d.to_dict().get('sku') for d in skus_ref if d.to_dict().get('sku')])
    
    vinculos = db.collection('agrupamentos').where('origem', '==', 'PLENUS').stream()
    skus_vinc = set([d.to_dict().get('item_original') for d in vinculos])
    
    pendentes = skus_plenus - skus_vinc
    
    kpi1, kpi2, kpi3 = st.columns(3)
    kpi1.metric("SKUs no Plenus", len(skus_plenus))
    kpi2.metric("Já Vinculados", len(skus_vinc))
    kpi3.metric("Pendentes de Vínculo", len(pendentes), delta_color="inverse")
    
    st.markdown("### Criar Novo Grupo")
    tipo = st.radio("Origem:", ["PLENUS", "SISFLORA"], horizontal=True)
    
    if tipo == "PLENUS":
        sel = st.multiselect("Selecione os Itens (Pendentes):", sorted(list(pendentes)))
        nome = st.text_input("Nome do Grupo (Ex: IPE 10x10):")
        cat = st.selectbox("Categoria:", ["SERRADAS", "TORAS", "BENEFICIADAS", "OUTROS"])
        
        if st.button("Salvar Grupo Plenus"):
            salvar_grupo_novo(sel, nome, "PLENUS", cat)
            st.success("Grupo Criado!")
            time.sleep(1)
            st.rerun()

def modulo_conferencia():
    st.title("⚖️ Conferência Final")
    
    if 'df_sisflora' not in st.session_state or 'df_plenus' not in st.session_state:
        st.warning("⚠️ Você precisa carregar o saldo do Sisflora e do Plenus primeiro.")
        return
        
    df_s = st.session_state['df_sisflora'].copy()
    df_p = st.session_state['df_plenus'].copy()
    
    # 1. Carregar Dicionários
    mapa_sis = carregar_agrupamentos() # {item: grupo}
    mapa_ple = carregar_agrupamentos()
    mapa_vin = carregar_vinculos() # {grupo_ple: grupo_sis}
    
    # 2. Aplicar Mapeamento Sisflora
    if 'Item_Completo' not in df_s.columns:
        df_s['Item_Completo'] = df_s['produto'] + " - " + df_s['essencia']
    
    # No V185, corrigimos a chave do dicionário para lowercase se necessário
    # Mas o ideal é mapear direto
    df_s['Grupo'] = df_s['Item_Completo'].map(mapa_sis).fillna(df_s['Item_Completo'])
    
    # 3. Aplicar Mapeamento Plenus
    df_p['Grupo_Int'] = df_p['sku'].map(mapa_ple).fillna(df_p['sku'])
    df_p['Grupo_Final'] = df_p['Grupo_Int'].map(mapa_vin).fillna(df_p['Grupo_Int'])
    
    # 4. Agregação
    # Cuidado com nomes de colunas: v185 usa 'volume_disponivel' (minusculo no banco) ou renomeado
    col_vol_s = 'Volume Disponivel' if 'Volume Disponivel' in df_s.columns else 'volume_disponivel'
    
    res_s = df_s.groupby('Grupo')[col_vol_s].sum()
    res_p = df_p.groupby('Grupo_Final')['saldo'].sum() # Pega a soma dos saldos
    
    # 5. Cruzamento
    df_final = pd.DataFrame({'Vol Sisflora': res_s, 'Vol Plenus': res_p}).fillna(0)
    df_final['Diferenca'] = df_final['Vol Sisflora'] - df_final['Vol Plenus']
    
    # 6. Visualização
    st.dataframe(df_final.style.format(formatar_br).applymap(
        lambda v: 'color: red; font-weight: bold' if v < -0.1 else ('color: blue' if v > 0.1 else 'color: green'),
        subset=['Diferenca']
    ), use_container_width=True, height=600)


# --- MENU PRINCIPAL (SIDEBAR) ---
st.sidebar.title("Navegação")
menu = st.sidebar.radio("Ir para:", ["1. Saldo Sisflora", "2. Saldo Plenus", "3. Gestão Vínculos", "4. Conferência"])

if menu == "1. Saldo Sisflora": modulo_sisflora()
elif menu == "2. Saldo Plenus": modulo_plenus()
elif menu == "3. Gestão Vínculos": modulo_admin()
elif menu == "4. Conferência": modulo_conferencia()

st.sidebar.markdown("---")
st.sidebar.caption("Sistema S&P Cloud v185")
