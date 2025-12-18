import streamlit as st
import pdfplumber
import pandas as pd
import plotly.express as px
import re
import io
import json
import time
from bs4 import BeautifulSoup
from datetime import datetime, date
from difflib import SequenceMatcher

# --- FIREBASE IMPORTS ---
import firebase_admin
from firebase_admin import credentials, firestore

# --- CONFIGURAÇÃO ---
st.set_page_config(page_title="🌲 Sistema S&P - Web Firebase", layout="wide")

# --- INICIALIZAÇÃO FIREBASE ---
# Verifica se já inicializou para não dar erro de "App already exists"
if not firebase_admin._apps:
    # Tenta pegar das secrets do Streamlit (Produção)
    if 'firebase' in st.secrets:
        cred_dict = dict(st.secrets['firebase'])
        cred = credentials.Certificate(cred_dict)
    # Senão, tenta pegar arquivo local (Desenvolvimento)
    else:
        try:
            cred = credentials.Certificate("serviceAccountKey.json")
        except:
            st.error("❌ Arquivo 'serviceAccountKey.json' não encontrado e secrets não configuradas.")
            st.stop()
    
    firebase_admin.initialize_app(cred)

db = firestore.client()

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

# --- FUNÇÕES UTILITÁRIAS FIREBASE (SUBSTITUINDO SQLITE) ---

def firestore_to_df(collection_name, query_ref=None):
    """Converte coleção ou query do Firestore para DataFrame."""
    try:
        if query_ref:
            docs = query_ref.stream()
        else:
            docs = db.collection(collection_name).stream()
        
        items = []
        for doc in docs:
            d = doc.to_dict()
            d['firebase_id'] = doc.id # Guarda ID para updates/deletes se precisar
            items.append(d)
        
        return pd.DataFrame(items)
    except Exception as e:
        st.error(f"Erro ao ler Firestore ({collection_name}): {e}")
        return pd.DataFrame()

def get_max_date_db(collection, col_data):
    """Retorna a data máxima salva no Firestore."""
    try:
        # Firestore ordena string de data YYYY-MM-DD corretamente
        query = db.collection(collection).order_by(col_data, direction=firestore.Query.DESCENDING).limit(1)
        docs = list(query.stream())
        if docs:
            val_str = docs[0].to_dict().get(col_data)
            if val_str:
                return datetime.strptime(str(val_str)[:10], "%Y-%m-%d").date()
    except Exception as e:
        # st.warning(f"Erro data max: {e}")
        pass
    return None

def get_smart_date_range(collection, col_data):
    """Retorna (dt_ini, dt_fim) baseado na inteligência."""
    max_date = get_max_date_db(collection, col_data)
    hoje = date.today()
    
    if max_date:
        dt_ini = date(max_date.year, max_date.month, 1)
        prox_mes = max_date.replace(day=28) + pd.Timedelta(days=4)
        dt_fim = prox_mes - pd.Timedelta(days=prox_mes.day)
        return dt_ini, dt_fim
    
    dt_ini = date(hoje.year, hoje.month, 1)
    dt_fim = hoje
    return dt_ini, dt_fim

def update_session_dates(prefix, dt_min, dt_max):
    if dt_min and dt_max:
        st.session_state[f'{prefix}_dt_ini'] = dt_min
        st.session_state[f'{prefix}_dt_fim'] = dt_max

# --- FUNÇÕES UI (MANTIDAS IDENTICAS AO ORIGINAL) ---
def render_filtered_table(df, key_prefix, show_total=True):
    if df.empty:
        st.info("Nenhum dado para exibir.")
        return

    # 1. Filtro Texto Global
    c1, c2 = st.columns([2, 1])
    txt_search = c1.text_input("🔎 Pesquisa Global:", key=f"txt_{key_prefix}")
    
    # 2. Filtro Colunas/Categoria
    cols_filter = c2.multiselect("Filtrar por Coluna(s):", df.columns, key=f"cols_{key_prefix}")
    
    df_view = df.copy()
    
    # Aplica busca textual
    if txt_search:
        mask = df_view.astype(str).apply(lambda x: x.str.contains(txt_search, case=False, na=False)).any(axis=1)
        df_view = df_view[mask]
        
    # Aplica filtro de colunas
    if cols_filter:
        df_view = df_view[cols_filter]

    # 3. Totais
    if show_total:
        numerics = df_view.select_dtypes(include=['float', 'int'])
        ignore_terms = ['id', 'sku', 'codigo', 'código', 'numero', 'número', 'nota', 'serie', 'série', 'ano', 'mes', 'dia', 'firebase_id']
        cols_to_sum = [c for c in numerics.columns if not any(term in c.lower() for term in ignore_terms)]
        
        if cols_to_sum:
            total_html = "<div style='display:flex; gap: 20px; flex-wrap: wrap; margin-bottom: 10px;'>"
            for col in cols_to_sum:
                val = df_view[col].sum()
                if abs(val) > 0.0001:
                    total_html += f"<div style='background:#e9ecef; padding:5px 10px; border-radius:4px;'><b>{col}:</b> {val:,.4f}".replace(",", "X").replace(".", ",").replace("X", ".") + "</div>"
            total_html += "</div>"
            st.markdown(total_html, unsafe_allow_html=True)

    # 4. Formatação Visual
    date_cols = []
    for col in df_view.columns:
        if pd.api.types.is_datetime64_any_dtype(df_view[col]):
            date_cols.append(col)
            df_view[col] = df_view[col].dt.strftime('%d/%m/%Y')
        elif df_view[col].astype(str).str.match(r'^\d{4}-\d{2}-\d{2}$').all():
             try:
                 df_view[col] = pd.to_datetime(df_view[col]).dt.strftime('%d/%m/%Y')
                 date_cols.append(col)
             except: pass

    cols_no_fmt = [c for c in df_view.columns if any(x in c.lower() for x in ['id', 'sku', 'numero', 'nota', 'serie', 'codigo', 'ano', 'firebase'])]
    
    def fmt_br(x):
        if isinstance(x, (float, int)) and not isinstance(x, bool):
            return f"{x:,.4f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return str(x)

    def fmt_id(x): return str(x)

    format_dict = {}
    for c in df_view.columns:
        if c in cols_no_fmt: format_dict[c] = fmt_id
        elif pd.api.types.is_numeric_dtype(df_view[c]): format_dict[c] = fmt_br
        else: format_dict[c] = str

    styler = df_view.style.format(format_dict)
    if date_cols: styler.set_properties(subset=date_cols, **{'text-align': 'center'})
    st.dataframe(styler, use_container_width=True, height=500)

def render_plenus_dashboard(df_full, key_prefix="p_dash", allow_save=True):
    min_d, max_d = date.today(), date.today()
    if 'data_movimento' in df_full.columns:
        try:
            dates = pd.to_datetime(df_full['data_movimento'], errors='coerce').dropna().dt.date
            if not dates.empty:
                min_d, max_d = dates.min(), dates.max()
        except: pass
    
    st.markdown("##### 🗓️ Filtrar Período")
    c_f1, c_f2 = st.columns(2)
    d_ini_f = c_f1.date_input("Filtrar Data De:", value=min_d, key=f"{key_prefix}_filtro_p_ini", format="DD/MM/YYYY", on_change=save_app_state)
    d_fim_f = c_f2.date_input("Filtrar Data Até:", value=max_d, key=f"{key_prefix}_filtro_p_fim", format="DD/MM/YYYY", on_change=save_app_state)

    df_view = df_full.copy()
    if 'data_movimento' in df_view.columns:
        df_view['dt_temp'] = pd.to_datetime(df_view['data_movimento'], errors='coerce').dt.date
        mask = (df_view['dt_temp'].isna()) | ((df_view['dt_temp'] >= d_ini_f) & (df_view['dt_temp'] <= d_fim_f))
        df_view = df_view[mask].drop(columns=['dt_temp'])
    
    c1, c2, c3 = st.columns(3)
    cats = sorted(df_view['categoria'].astype(str).unique()) if 'categoria' in df_view.columns else []
    f_cat = c1.multiselect("Categoria:", cats, key=f"{key_prefix}_fp_cat")
    f_txt = c3.text_input("Pesquisar:", key=f"{key_prefix}_fp_txt")
        
    if f_cat: df_view = df_view[df_view['categoria'].isin(f_cat)]
    if f_txt: df_view = df_view[df_view['Item_Completo'].str.contains(f_txt, case=False, na=False)]
    
    vol_total = 0
    if 'tipo' in df_view.columns:
        vol_total = df_view[df_view['tipo'].isin(['Total', 'TOTAL'])]['saldo'].sum()
        if vol_total == 0 and not df_view.empty and 'sku' in df_view.columns and 'saldo' in df_view.columns:
            df_calc = df_view.copy()
            cols_sort = []
            if 'data_movimento' in df_calc.columns: cols_sort.append('data_movimento')
            # Firestore não tem ID sequencial simples, mas ordenamos por data
            if cols_sort:
                df_calc = df_calc.sort_values(by=cols_sort, ascending=True)
            vol_total = df_calc.groupby('sku')['saldo'].last().sum()

    st.metric("Saldo Total", formatar_br(vol_total))
    st.caption(f"Exibindo {len(df_view)} registros.")
    
    cols_table = ['data', 'sku', 'produto', 'categoria', 'tipo', 'entrada', 'saida', 'saldo']
    cols_exist = [c for c in cols_table if c in df_view.columns]
        
    st.dataframe(
        df_view[cols_exist].style.format({
            'entrada': formatar_br, 'saida': formatar_br, 'saldo': formatar_br
        }), 
        use_container_width=True, height=600
    )

    if allow_save:
        st.divider()
        st.info("Para auditoria, é necessário salvar os movimentos no banco de dados.")
        if st.button("💾 Salvar Filtrados no Firebase", key=f"{key_prefix}_btn_save"):
            df_save = df_view.copy()
            rename_map = {}
            if 'tipo' in df_save.columns: rename_map['tipo'] = 'tipo_movimento'
            if 'saldo' in df_save.columns: rename_map['saldo'] = 'saldo_apos'
            df_save.rename(columns=rename_map, inplace=True)
            
            if 'data_movimento' in df_save.columns:
                df_save = df_save[df_save['data_movimento'].notna()]

            cols_db_plenus = ['sku', 'produto', 'categoria', 'data_movimento', 'tipo_movimento', 
                                'entrada', 'saida', 'saldo_apos', 'nota', 'serie', 'arquivo_origem']
            cols_to_save = [c for c in cols_db_plenus if c in df_save.columns]
            df_save = df_save[cols_to_save]

            inseridos, existentes = salvar_lote_smart('plenus_historico', 'data_movimento', df_save)
            
            dates = pd.to_datetime(df_save['data_movimento']).dt.date
            if not dates.empty:
                update_session_dates('p', dates.min(), dates.max())

            if inseridos > 0: st.success(f"✅ {inseridos} registros salvos no Firebase!")
            if existentes > 0: st.warning(f"⚠️ {existentes} registros já existiam.")
            if inseridos == 0 and existentes == 0: st.warning("Nada novo a salvar.")

# --- INICIALIZAÇÃO VARIAVEIS ---
def init_session_vars():
    if 'p_dt_ini' not in st.session_state: 
        i, f = get_smart_date_range('plenus_historico', 'data_movimento')
        st.session_state['p_dt_ini'] = i
        st.session_state['p_dt_fim'] = f
    if 'view_plenus' not in st.session_state: st.session_state['view_plenus'] = None 
    
    if 't_dt_ini' not in st.session_state:
        i, f = get_smart_date_range('transf_historico', 'data_realizacao')
        st.session_state['t_dt_ini'] = i
        st.session_state['t_dt_fim'] = f
    if 'view_transf' not in st.session_state: st.session_state['view_transf'] = None 
    
    if 'c_dt_ini' not in st.session_state:
        i, f = get_smart_date_range('consumo_historico', 'data_consumo')
        st.session_state['c_dt_ini'] = i
        st.session_state['c_dt_fim'] = f
    if 'view_consumo' not in st.session_state: st.session_state['view_consumo'] = None 
    
    if 'aud_dt_ini' not in st.session_state:
        d1 = get_max_date_db('plenus_historico', 'data_movimento')
        d2 = get_max_date_db('transf_historico', 'data_realizacao')
        mx = max([d for d in [d1, d2] if d]) if any([d1, d2]) else date.today()
        st.session_state['aud_dt_ini'] = date(mx.year, mx.month, 1)
        st.session_state['aud_dt_fim'] = mx

init_session_vars()

# --- CSS ---
st.markdown("""
    <style>
    .block-container { padding-top: 2rem; }
    .stDataFrame { overscroll-behavior: contain; }
    div[data-testid="stDataFrame"] > div { overscroll-behavior: contain; }
    section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] { display: flex; flex-direction: column; gap: 10px; }
    section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] label { background-color: #f8f9fa; border: 1px solid #dee2e6; border-radius: 5px; padding: 10px; font-weight: 500; cursor: pointer; }
    section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] label:hover { background-color: #e2e6ea; border-color: #adb5bd; }
    .stButton button { width: 100%; font-weight: bold; background-color: #28a745; color: white; height: 45px; border-radius: 5px; }
    .stButton button:hover { background-color: #218838; color: white; }
    div[data-testid="stVerticalBlock"] > div > button[kind="secondary"] { background-color: #007bff !important; color: white !important; border: none; }
    div[data-testid="stDataFrame"] div[role="grid"] div[role="row"] { min-height: 30px !important; font-size: 14px !important; }
    .grupo-title { padding: 8px 12px; border-radius: 5px; font-weight: bold; margin-top: 15px; margin-bottom: 5px; }
    </style>
""", unsafe_allow_html=True)

# --- FUNÇÕES AUXILIARES GERAIS (PARSERS) ---
def parse_float_inteligente(valor):
    try:
        if isinstance(valor, (float, int)): return float(valor)
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
    if origin == "SISFLORA":
        if str(item).startswith("10"): cat = "TORAS"
        elif str(item).startswith("20") or str(item).startswith("3030"): cat = "SERRADAS"
        elif str(item).startswith("50"): cat = "BENEFICIADAS"
    else:
        txt = str(item).upper()
        if "BENEF" in txt or "DECK" in txt or "FORRO" in txt or "ASSOALHO" in txt: cat = "BENEFICIADAS"
        elif "TORA" in txt or "TORO" in txt: cat = "TORAS"
        elif "SERRAD" in txt or "CAIBRO" in txt or "VIGA" in txt or "PRANCH" in txt or "RIPA" in txt: cat = "SERRADAS"
    return cat

def detectar_categoria_plenus(item_completo):
    return detecting_category(item_completo, "PLENUS")

def sort_key_nomes(item):
    parts = str(item).split(' - ', 1)
    if len(parts) > 1: return parts[1].strip()
    return str(item)

# --- FUNÇÕES AUXILIARES SISTRANSF/EXCEL ---
def transform_data_sistransf(df, filename="Upload"):
    rows = []
    for idx, row in df.iterrows():
        essencia_origem = str(row.get("Essência Origem", ""))
        popular = ""
        if "-" in essencia_origem:
            parts = essencia_origem.split("-", 1)
            if len(parts) > 1: popular = parts[1].strip()
        
        dt_real = row.get("Data Realização", "")
        if isinstance(dt_real, (pd.Timestamp, datetime, date)):
            dt_real = dt_real.strftime("%Y-%m-%d")
        else:
            try:
                dt_obj = datetime.strptime(str(dt_real).strip(), "%d/%m/%Y")
                dt_real = dt_obj.strftime("%Y-%m-%d")
            except: pass 

        base_obj = {
            "numero": str(row.get("Número", "")),
            "data_realizacao": str(dt_real),
            "situacao": str(row.get("Situação", "")),
            "essencia": str(row.get("Essência Origem", "")),
            "arquivo_origem": filename,
            "popular": popular
        }

        origem = base_obj.copy()
        origem.update({
            "tipo_produto": "PRODUTO DE ORIGEM",
            "produto": str(row.get("Produto Origem", "")),
            "volume": float(row.get("Volume Origem", 0) if pd.notnull(row.get("Volume Origem")) else 0),
            "unidade": str(row.get("Unidade Origem", ""))
        })
        rows.append(origem)
        
        produto_gerado = row.get("Produto Gerado", "")
        if pd.notnull(produto_gerado) and str(produto_gerado).strip() != "":
            popular_gerado = ""
            if "-" in str(row.get("Essência Origem", "")):
                 parts = str(row.get("Essência Origem", "")).split("-", 1)
                 if len(parts) > 1: popular_gerado = parts[1].strip()
            
            gerado = base_obj.copy()
            gerado.update({
                "tipo_produto": "PRODUTO GERADO",
                "produto": str(produto_gerado),
                "popular": popular_gerado,
                "volume": float(row.get("Volume Gerado", 0) if pd.notnull(row.get("Volume Gerado")) else 0),
                "unidade": str(row.get("Unidade Gerada", ""))
            })
            rows.append(gerado)

    final_df = pd.DataFrame(rows)
    # Remove duplicação visual de origens (mantem a logica do usuario)
    mask_origem = final_df["tipo_produto"] == "PRODUTO DE ORIGEM"
    df_origem = final_df[mask_origem].drop_duplicates(subset=["numero", "essencia", "volume"], keep="first")
    df_outros = final_df[~mask_origem]
    final_df = pd.concat([df_origem, df_outros], ignore_index=True)
    return final_df

def to_excel_autoajustado(df):
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False, sheet_name='Relatorio')
        worksheet = writer.sheets['Relatorio']
        for i, col in enumerate(df.columns):
            worksheet.set_column(i, i, 20)
    return output.getvalue()

# --- ALGORITMO VÍNCULO (Fuzzy) ---
def limpar_para_comparacao(texto):
    palavras_lixo = {
        "MADEIRA", "NATIVA", "SERRADA", "SERRADOS", "SERRADO", "BENEFICIADA", "BENEFICIADO", "BENEFICIADOS", 
        "TORAS", "TOROS", "TORA", "TORO", "EM", "DE", "DO", "DA", "BRUTO", "APROVEITAMENTO", 
        "TABUA", "VIGA", "CAIBRO", "PRANCHA", "RIPA", "SARRAFO", "DECK", "ASSOALHO", "FORRO", "-", ".", ",", "(", ")"
    }
    texto_limpo = re.sub(r'[^\w\s]', ' ', texto.upper())
    parts = texto_limpo.split()
    clean_parts = [p for p in parts if p not in palavras_lixo and not p.isdigit()]
    if not clean_parts: return texto.upper()
    return " ".join(clean_parts)

def calcular_similaridade_avancada(nome_plenus, nome_sisflora):
    essencia_p = limpar_para_comparacao(nome_plenus)
    essencia_s = limpar_para_comparacao(nome_sisflora)
    ratio_essencia = SequenceMatcher(None, essencia_p, essencia_s).ratio()
    bonus = 0.15 if (len(essencia_p) > 3 and len(essencia_s) > 3) and (essencia_p in essencia_s or essencia_s in essencia_p) else 0
    return min(ratio_essencia + bonus, 1.0)

def gerar_sugestao_nome_primeiro(itens_selecionados, categoria_filtro, origem="SISFLORA"):
    if not itens_selecionados: return ""
    primeiro_item = itens_selecionados[0]
    nome_bruto = ""
    cat_detectada = ""
    s_upper = primeiro_item.upper()

    if origem == "SISFLORA":
        parts = primeiro_item.split(' - ')
        candidatos = [p.strip() for p in parts if not re.match(r'^\d+$', p.strip()) and p.strip().upper() not in TERMOS_GENERICOS]
        nome_bruto = candidatos[-1] if candidatos else parts[-1]
        if primeiro_item.startswith("10"): cat_detectada = "TORAS"
        elif primeiro_item.startswith("20") or primeiro_item.startswith("3030"): cat_detectada = "SERRADAS"
        elif primeiro_item.startswith("50"): cat_detectada = "BENEFICIADAS"
    else: 
        s_clean = primeiro_item.split(' (')[0]
        nome_bruto = s_clean.split(' - ', 1)[1].strip() if ' - ' in s_clean else s_clean.strip()
        if "BENEF" in s_upper or "DECK" in s_upper: cat_detectada = "BENEFICIADAS"
        elif "TORA" in s_upper or "TORO" in s_upper: cat_detectada = "TORAS"
        elif "SERRAD" in s_upper or "CAIBRO" in s_upper or "VIGA" in s_upper: cat_detectada = "SERRADAS"

    TERMOS_LIMPEZA = ["SERRADA", "BENEFICIADA", "TORAS", "TOROS", "TORA", "DE", "DO", "BRUTO", "APROVEITAMENTO"]
    palavras = nome_bruto.upper().replace(".", " ").replace("-", " ").split()
    essencia_parts = [p for p in palavras if p not in TERMOS_LIMPEZA and not p.isdigit()]
    essencia_final = " ".join(essencia_parts)
    
    cat_final = categoria_filtro if categoria_filtro else cat_detectada
    if cat_final == "OUTROS": cat_final = ""
    
    if cat_final and cat_final not in essencia_final:
        return f"{essencia_final} {cat_final}"
    return essencia_final

# --- PERSISTENCIA DE ESTADO (FIREBASE) ---
def load_app_state():
    """Carrega estado de navegação do Firebase (Simulado ou Real)."""
    # Em Web App multi-usuário, o 'estado' geralmente é da sessão (cookie).
    # Aqui vamos usar apenas st.session_state que já persiste enquanto a aba está aberta.
    # Se quiser salvar preferências de usuário no DB, usaria uma coleção 'user_prefs'.
    if 'menu_sel_idx' not in st.session_state: st.session_state['menu_sel_idx'] = 0

def save_app_state():
    """Salva apenas na sessão local por enquanto."""
    pass

# --- FUNÇÕES DB (AGRUPAMENTOS / VINCULOS) ---
@st.cache_data(ttl="1h")
def carregar_agrupamentos_db(origem):
    # Collection: agrupamentos
    query = db.collection('agrupamentos').where('origem', '==', origem)
    df = firestore_to_df('agrupamentos', query)
    if df.empty: return {}
    return pd.Series(df.nome_grupo.values, index=df.item_original).to_dict()

def carregar_todos_agrupamentos_db():
    return firestore_to_df('agrupamentos')

def get_categorias_dos_grupos(origem):
    df = carregar_todos_agrupamentos_db()
    if df.empty: return {}
    df = df[df['origem'] == origem]
    if df.empty: return {}
    # Mode logic
    return df.groupby('nome_grupo')['categoria'].agg(lambda x: x.mode()[0] if not x.mode().empty else "OUTROS").to_dict()

def salvar_agrupamento_db(itens, nome_grupo, origem, categoria_detectada):
    batch = db.batch()
    coll = db.collection('agrupamentos')
    
    try:
        for it in itens:
            # Cria ID único composto para evitar duplicatas: ORIGEM_ITEM
            safe_item = re.sub(r'[^a-zA-Z0-9]', '', it)[:100]
            doc_id = f"{origem}_{safe_item}"
            doc_ref = coll.document(doc_id)
            batch.set(doc_ref, {
                'item_original': it,
                'nome_grupo': nome_grupo,
                'origem': origem,
                'categoria': categoria_detectada
            })
        batch.commit()
        st.toast(f"✅ Grupo Salvo: {nome_grupo}", icon="💾")
        carregar_agrupamentos_db.clear()
    except Exception as e: st.error(f"Erro Firebase: {e}")

def excluir_grupo_db(nome_grupo, origem):
    # Query docs to delete
    docs = db.collection('agrupamentos').where('nome_grupo', '==', nome_grupo).where('origem', '==', origem).stream()
    batch = db.batch()
    count = 0
    for doc in docs:
        batch.delete(doc.reference)
        count += 1
    if count > 0:
        batch.commit()
    carregar_agrupamentos_db.clear()

def carregar_vinculos_db():
    df = firestore_to_df('vinculos')
    if df.empty: return {}
    # Assumindo que o ID do documento é o grupo_plenus
    return pd.Series(df.grupo_sisflora.values, index=df.grupo_plenus).to_dict()

def carregar_lista_grupos_db(origem):
    agrups = carregar_agrupamentos_db(origem)
    return sorted(list(set(agrups.values())))

def salvar_vinculo_db(grupos_plenus_lista, grupo_sisflora):
    batch = db.batch()
    coll = db.collection('vinculos')
    for gp in grupos_plenus_lista:
        # ID do documento é o grupo plenus (chave primária)
        safe_id = re.sub(r'[/]', '_', gp) # Firestore não gosta de barras em IDs
        doc_ref = coll.document(safe_id)
        batch.set(doc_ref, {
            'grupo_plenus': gp,
            'grupo_sisflora': grupo_sisflora
        })
    batch.commit()
    st.toast("🔗 Vínculo criado!", icon="🔗")

def excluir_vinculo_db(grupo_plenus):
    # Tenta achar pelo campo ou ID
    safe_id = re.sub(r'[/]', '_', grupo_plenus)
    db.collection('vinculos').document(safe_id).delete()

# --- FUNÇÕES DE ESCRITA INTELIGENTE (BATCH + VERIFICAÇÃO) ---
def check_dates_exist(collection, col_data, datas_lista):
    """Verifica datas existentes (Firestore não tem WHERE IN eficiente para muitas datas)."""
    # Para datasets grandes, o ideal é consultar por range ou carregar metadados
    # Aqui, vamos fazer uma query por range para reduzir leituras
    if not datas_lista: return set()
    
    d_min = min(datas_lista).strftime("%Y-%m-%d")
    d_max = max(datas_lista).strftime("%Y-%m-%d")
    
    # Pega todos docs nesse range de datas
    docs = db.collection(collection)\
             .where(col_data, '>=', d_min)\
             .where(col_data, '<=', d_max)\
             .select([col_data]).stream() # Seleciona só o campo data para economizar
             
    found = set()
    for d in docs:
        val = d.to_dict().get(col_data)
        if val: found.add(val)
    
    return found

def salvar_lote_smart(collection, col_data, df):
    """Salva dados no Firebase filtrando datas já existentes."""
    if df.empty: return 0, 0
    
    # Converte coluna de data para string YYYY-MM-DD
    df_check = df.copy()
    if pd.api.types.is_datetime64_any_dtype(df_check[col_data]):
        df_check[col_data] = df_check[col_data].dt.strftime("%Y-%m-%d")
    
    dates_unique = [datetime.strptime(d, "%Y-%m-%d").date() for d in df_check[col_data].unique()]
    existing = check_dates_exist(collection, col_data, dates_unique)
    
    df_to_save = df_check[~df_check[col_data].isin(existing)]
    
    if df_to_save.empty:
        return 0, len(existing)
    
    # Batch writes (limit 500 ops per batch)
    records = df_to_save.to_dict(orient='records')
    batch = db.batch()
    count = 0
    total_saved = 0
    
    for rec in records:
        doc_ref = db.collection(collection).document() # Auto ID
        batch.set(doc_ref, rec)
        count += 1
        if count >= 450:
            batch.commit()
            batch = db.batch()
            total_saved += count
            count = 0
    
    if count > 0:
        batch.commit()
        total_saved += count
        
    return total_saved, len(existing)

def excluir_periodo_tabela(collection, col_data, dt_ini, dt_fim):
    d_i = dt_ini.strftime("%Y-%m-%d")
    d_f = dt_fim.strftime("%Y-%m-%d")
    
    docs = db.collection(collection).where(col_data, '>=', d_i).where(col_data, '<=', d_f).stream()
    
    count = 0
    batch = db.batch()
    for doc in docs:
        batch.delete(doc.reference)
        count += 1
        if count % 450 == 0:
            batch.commit()
            batch = db.batch()
    
    if count % 450 != 0:
        batch.commit()
    return count

# --- FUNÇÕES DE LEITURA ESPECÍFICAS ---
def carregar_transf_filtrado_db(dt_ini, dt_fim, lista_filtros=None):
    # Firestore Filter
    query = db.collection('transf_historico')\
              .where('data_realizacao', '>=', dt_ini.strftime("%Y-%m-%d"))\
              .where('data_realizacao', '<=', dt_fim.strftime("%Y-%m-%d"))
    
    df = firestore_to_df('transf_historico', query)
    
    if df.empty: return df
    
    # Aplica filtros extras via Pandas (Firestore tem limitações com múltiplos filtros 'in')
    mapa_cols = {
        "Número": "numero", "Situação": "situacao",
        "PRODUTO": "tipo_produto", "Produto": "produto", "Popular": "popular",
        "Essência": "essencia", "Unidade": "unidade"
    }
    
    if lista_filtros:
        for f in lista_filtros:
            col_db = mapa_cols.get(f['col'])
            valores = f['vals']
            if col_db and valores and col_db in df.columns:
                df = df[df[col_db].isin(valores)]
                
    return df

def carregar_plenus_movimento_db(dt_ini, dt_fim):
    query = db.collection('plenus_historico')\
              .where('data_movimento', '>=', dt_ini.strftime("%Y-%m-%d"))\
              .where('data_movimento', '<=', dt_fim.strftime("%Y-%m-%d"))
    return firestore_to_df('plenus_historico', query)

def carregar_consumo_filtrado_db(dt_ini, dt_fim):
    query = db.collection('consumo_historico')\
              .where('data_consumo', '>=', dt_ini.strftime("%Y-%m-%d"))\
              .where('data_consumo', '<=', dt_fim.strftime("%Y-%m-%d"))
    df = firestore_to_df('consumo_historico', query)
    
    # Expand JSON logic
    if not df.empty and 'dados_json' in df.columns:
        try:
            # Dropna and iterate
            json_series = df['dados_json'].dropna()
            if not json_series.empty:
                list_of_dicts = [json.loads(x) for x in json_series if x]
                df_expanded = pd.json_normalize(list_of_dicts)
                # Merge logic needs simple concat if index aligns, or just return expanded
                # Simpler: return fields that matter
                return df_expanded
        except: pass
    return df

# --- SISFLORA DB SPECIFIC ---
def salvar_lote_sisflora_db(df, data_ref, nome_arquivo):
    # 1. Deleta existente nessa data
    excluir_sisflora_por_data(data_ref)
    
    # 2. Prepara dados
    df_save = df.copy()
    rename_map = {
        "Produto": "produto", "Essencia": "essencia", "Unidade": "unidade",
        "Volume Disponivel": "volume_disponivel", "Codigo": "codigo", "Cat_Auto": "cat_auto"
    }
    df_save.rename(columns=rename_map, inplace=True)
    cols_db = ["produto", "essencia", "unidade", "volume_disponivel", "codigo", "cat_auto"]
    for c in cols_db:
        if c not in df_save.columns: df_save[c] = ""
    
    df_save = df_save[cols_db].copy()
    df_save["data_referencia"] = data_ref.strftime("%Y-%m-%d")
    df_save["arquivo_origem"] = nome_arquivo
    
    # 3. Salva Lote
    records = df_save.to_dict(orient='records')
    batch = db.batch()
    count = 0
    for rec in records:
        doc_ref = db.collection('sisflora_historico').document()
        batch.set(doc_ref, rec)
        count += 1
        if count >= 450:
            batch.commit()
            batch = db.batch()
            count = 0
    if count > 0: batch.commit()
    return True

def carregar_sisflora_data_db(data_ref):
    query = db.collection('sisflora_historico').where('data_referencia', '==', data_ref.strftime("%Y-%m-%d"))
    df = firestore_to_df('sisflora_historico', query)
    if not df.empty:
        df.rename(columns={
            "produto": "Produto", "essencia": "Essencia", "unidade": "Unidade",
            "volume_disponivel": "Volume Disponivel", "codigo": "Codigo", "cat_auto": "Cat_Auto"
        }, inplace=True)
        df["Item_Completo"] = df.apply(lambda x: f"{x['Produto']} - {x['Essencia']}" if x['Essencia'] else x['Produto'], axis=1)
    return df

def get_datas_sisflora_disponiveis():
    # Firestore nao tem SELECT DISTINCT. Tem que puxar tudo (leve se for so metadados, mas aqui nao temos tabela separada)
    # Solucao: Criar uma coleção 'meta_sisflora_dates' seria o ideal.
    # Por agora, scan all (cuidado com custo).
    # ALTERNATIVA: Limite
    docs = db.collection('sisflora_historico').select(['data_referencia']).stream()
    datas = set()
    for d in docs:
        val = d.to_dict().get('data_referencia')
        if val: datas.add(val)
    
    dt_objs = [datetime.strptime(d, "%Y-%m-%d").date() for d in datas]
    return sorted(dt_objs, reverse=True)

def excluir_sisflora_por_data(data_ref):
    d_str = data_ref.strftime("%Y-%m-%d")
    docs = db.collection('sisflora_historico').where('data_referencia', '==', d_str).stream()
    batch = db.batch()
    c = 0
    for doc in docs:
        batch.delete(doc.reference)
        c += 1
        if c >= 450:
            batch.commit()
            batch = db.batch()
            c = 0
    if c > 0: batch.commit()
    return True

# --- LEITURA SISFLORA (PDF) ---
@st.cache_data(show_spinner=False)
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
    
    linhas_corrigidas = []
    linha_anterior = None
    for row in dados_brutos:
        row_str = [str(x) if x is not None else "" for x in row]
        eh_novo_registro = re.match(r'^\d+\s*-', row_str[0])
        if eh_novo_registro:
            if linha_anterior: linhas_corrigidas.append(linha_anterior)
            linha_anterior = row_str
        else:
            if linha_anterior and len(row_str) > 1:
                 linha_anterior[1] = (linha_anterior[1] + " " + row_str[1]).strip()
    if linha_anterior: linhas_corrigidas.append(linha_anterior)
    dados_finais = linhas_corrigidas if linhas_corrigidas else dados_brutos
    
    colunas_padrao = ["Produto", "Essencia", "Volume Disponivel", "Item_Completo", "Cat_Auto"]
    if not dados_finais: return pd.DataFrame(columns=colunas_padrao)
    
    df = pd.DataFrame(dados_finais)
    idx_dados = -1
    for i in range(min(len(df), 50)):
        c0 = str(df.iloc[i, 0]).strip()
        if re.match(r'^\d+\s*-', c0): idx_dados = i; break
    if idx_dados == -1: return pd.DataFrame(columns=colunas_padrao)
    df = df[idx_dados:].reset_index(drop=True)
    
    idx_vol = len(df.columns) - 1 
    for c in range(len(df.columns)-1, -1, -1):
        if df.iloc[:20, c].astype(str).str.contains(r'\d+,\d+').any(): idx_vol = c; break
    
    mapa_cols = {0: "Produto"}
    if idx_vol > 1: mapa_cols[1] = "Essencia"
    if idx_vol >= 2: mapa_cols[idx_vol-1] = "Unidade"
    mapa_cols[idx_vol] = "Volume Disponivel"
    df.rename(columns=mapa_cols, inplace=True)
    
    if "Produto" not in df.columns: df["Produto"] = ""
    if "Essencia" not in df.columns: df["Essencia"] = ""
    if "Unidade" not in df.columns: df["Unidade"] = ""
    if "Volume Disponivel" not in df.columns: df["Volume Disponivel"] = "0"
    
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

# --- LEITURA PLENUS (HTML) ---
@st.cache_data(show_spinner=False)
def extrair_dados_plenus_html(arquivo_html, nome_arquivo="Upload"):
    soup = BeautifulSoup(arquivo_html, 'html.parser')
    dados_extraidos = []
    skus_vistos = set()
    skus_com_total = set()
    state = {'categoria': None, 'sku': None, 'produto': None}
    
    def safe_txt(c): return c.get_text(strip=True) if c else ''
    
    rows = soup.find_all('tr')
    for tr in rows:
        cols = tr.find_all('td')
        if not cols: continue
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
                    if state['sku']: skus_vistos.add(state['sku'])
            continue
        if state['sku']:
            tipo_cell = tr.find('td', class_=['s14', 's25'])
            if not tipo_cell: continue
            tipo = safe_txt(tipo_cell)
            
            ent = parse_float_inteligente(safe_txt(tr.find('td', class_='s15') or tr.find('td', class_='s21')))
            sai = parse_float_inteligente(safe_txt(tr.find('td', class_='s16') or tr.find('td', class_='s22')))
            sal = parse_float_inteligente(safe_txt(tr.find('td', class_='s17') or tr.find('td', class_='s23')))
            
            data_raw = safe_txt(tr.find('td', class_='s13'))
            
            if tipo.upper() in ['TOTAL', 'TOTAL:']:
                skus_com_total.add(state['sku'])
                data_raw = "Total"
            elif tipo.upper() in ['ANTERIOR', 'ANTERIOR:']:
                data_raw = "Anterior"
            
            data_db = None
            if data_raw and data_raw not in ["Total", "Anterior"]:
                try: data_db = datetime.strptime(data_raw, "%d/%m/%Y").strftime("%Y-%m-%d")
                except: pass
            
            dados_extraidos.append({
                "sku": state['sku'],
                "produto": state['produto'],
                "categoria": state['categoria'],
                "data": data_raw,
                "data_movimento": data_db,
                "tipo": tipo,
                "entrada": ent,
                "saida": sai,
                "saldo": sal,
                "arquivo_origem": nome_arquivo
            })
    
    lista_erros_skus = list(skus_vistos - skus_com_total)
    lista_erros_detalhada = []
    
    df_temp = pd.DataFrame(dados_extraidos)
    if not df_temp.empty:
        df_temp["Item_Completo"] = df_temp["produto"] + " (" + df_temp["categoria"].fillna("") + ")"
        df_temp["Cat_Auto"] = df_temp["Item_Completo"].apply(detectar_categoria_plenus)
        
        for sku_erro in lista_erros_skus:
            prod_nome = "Desconhecido"
            matches = df_temp[df_temp['sku'] == sku_erro]
            if not matches.empty:
                prod_nome = matches.iloc[0]['produto']
            lista_erros_detalhada.append({"SKU": sku_erro, "Produto": prod_nome, "Erro": "Sem Total"})
    else:
        df_temp = pd.DataFrame(columns=["sku", "produto", "categoria", "saldo", "tipo", "Item_Completo", "Cat_Auto", "data", "data_movimento", "entrada", "saida", "arquivo_origem"])

    return df_temp, lista_erros_detalhada

# --- CALLBACKS ADMIN ---
def salvar_sis_click():
    if st.session_state['cesta_sis'] and st.session_state['input_sis_name']:
        cat = st.session_state.get('s_cat_adm', '')
        salvar_agrupamento_db(st.session_state['cesta_sis'], st.session_state['input_sis_name'].upper(), "SISFLORA", cat)
        st.session_state['agrup_sis'] = carregar_agrupamentos_db("SISFLORA")
        st.session_state['cesta_sis'] = []
        st.session_state['input_sis_name'] = ""
def limpar_sis_click():
    st.session_state['cesta_sis'] = []
    st.session_state['input_sis_name'] = ""
def salvar_ple_click():
    if st.session_state['cesta_ple'] and st.session_state['input_ple_name']:
        cat = st.session_state.get('p_cat_adm', '')
        salvar_agrupamento_db(st.session_state['cesta_ple'], st.session_state['input_ple_name'].upper(), "PLENUS", cat)
        st.session_state['agrup_ple'] = carregar_agrupamentos_db("PLENUS")
        st.session_state['cesta_ple'] = []
        st.session_state['input_ple_name'] = ""
def limpar_ple_click():
    st.session_state['cesta_ple'] = []
    st.session_state['input_ple_name'] = ""

# --- INIT SESSION STATE ---
load_app_state()
if 'agrup_sis' not in st.session_state: st.session_state['agrup_sis'] = carregar_agrupamentos_db("SISFLORA")
if 'agrup_ple' not in st.session_state: st.session_state['agrup_ple'] = carregar_agrupamentos_db("PLENUS")
if 'vinculos' not in st.session_state: st.session_state['vinculos'] = carregar_vinculos_db()
if 'cesta_sis' not in st.session_state: st.session_state['cesta_sis'] = []
if 'cesta_ple' not in st.session_state: st.session_state['cesta_ple'] = []
if 'input_sis_name' not in st.session_state: st.session_state['input_sis_name'] = ""
if 'input_ple_name' not in st.session_state: st.session_state['input_ple_name'] = ""
if 'cnt_sis' not in st.session_state: st.session_state['cnt_sis'] = 0
if 'cnt_ple' not in st.session_state: st.session_state['cnt_ple'] = 0
if 'st_df_transf_preview' not in st.session_state: st.session_state['st_df_transf_preview'] = None
if 'filtros_ativos_transf' not in st.session_state: st.session_state['filtros_ativos_transf'] = []

# --- MENU FLUXO DE TRABALHO ---
st.title("🌲 CONFERENCIA ESTOQUE v186 - CLOUD")
st.markdown("### SISFLORA E PLENUSERP")

ordem_menu = [
    "1. SALDO SISFLORA",
    "2. SALDO PLENUS",
    "3. HISTORICO TRANSFORMAÇÃO",
    "4. DEBITO CONSUMO",
    "5. Gestão: Vínculos (Admin)",
    "6. Conferência & Auditoria"
]

idx_inicial = st.session_state.get('menu_sel_idx', 0)
if not isinstance(idx_inicial, int) or idx_inicial >= len(ordem_menu): idx_inicial = 0

def on_menu_change():
    sel = st.session_state.get('menu_main_nav')
    if sel in ordem_menu:
        st.session_state['menu_sel_idx'] = ordem_menu.index(sel)
    save_app_state()

menu_sel = st.sidebar.radio("Fluxo de Trabalho", ordem_menu, index=idx_inicial, key="menu_main_nav", on_change=on_menu_change)
st.sidebar.divider()
st.sidebar.info("💡 Versão Web com Firebase.")

# --- 1. SALDO SISFLORA ---
if menu_sel == "1. SALDO SISFLORA":
    st.header("📄 SALDO SISFLORA")
    op_sis = st.radio("Navegação Sisflora:", ["Ler PDF (Upload)", "Carregar do Histórico", "Gerenciar / Excluir"], 
                      horizontal=True, label_visibility="collapsed", key="nav_sis_183")
    st.divider()
    
    if op_sis == "Ler PDF (Upload)":
        f = st.file_uploader("PDF Sisflora (Saldo Atual)", type="pdf", key="up_sisflora")
        if f:
            with st.spinner("Lendo PDF..."):
                st.session_state['df_sisflora'] = extrair_dados_sisflora(f)
                st.session_state['sis_source'] = 'upload'
        
        if 'df_sisflora' in st.session_state and not st.session_state['df_sisflora'].empty and st.session_state.get('sis_source') == 'upload':
            df_s = st.session_state['df_sisflora']
            st.metric("Volume Total (PDF)", formatar_br(df_s['Volume Disponivel'].sum()))
            
            with st.expander("💾 Salvar este Saldo no Banco", expanded=True):
                data_ref = st.date_input("Data de Referência:", value=date.today(), format="DD/MM/YYYY")
                if st.button("Confirmar Salvamento no DB"):
                    if salvar_lote_sisflora_db(df_s, data_ref, f.name if f else "Upload"):
                        st.success(f"Saldo de {data_ref.strftime('%d/%m/%Y')} salvo!")
            
            cols_show = [c for c in ['Codigo', 'Produto', 'Essencia', 'Unidade', 'Volume Disponivel', 'Cat_Auto'] if c in df_s.columns]
            render_filtered_table(df_s[cols_show], "sis_upload")

    elif op_sis == "Carregar do Histórico":
        datas_disp = get_datas_sisflora_disponiveis()
        if datas_disp:
            sel_data = st.selectbox("Escolha uma data salva:", datas_disp, format_func=lambda x: x.strftime("%d/%m/%Y"))
            if st.button("Carregar Saldo desta Data"):
                st.session_state['df_sisflora'] = carregar_sisflora_data_db(sel_data)
                st.session_state['sis_source'] = 'history'
                st.success(f"Carregado!")
            
            if 'df_sisflora' in st.session_state and not st.session_state['df_sisflora'].empty:
                 st.divider()
                 render_filtered_table(st.session_state['df_sisflora'], "sis_db")
        else:
            st.warning("Nenhum histórico salvo.")

    elif op_sis == "Gerenciar / Excluir":
        st.markdown("### 🗑️ Excluir Saldo")
        datas_del = get_datas_sisflora_disponiveis()
        if datas_del:
            sel_del = st.selectbox("Selecione data:", datas_del, format_func=lambda x: x.strftime("%d/%m/%Y"), key="sel_del_sf")
            if st.button("Apagar Definitivamente", type="primary"):
                if excluir_sisflora_por_data(sel_del):
                    st.success("Apagado!")
                    time.sleep(1)
                    st.rerun()

# --- 2. SALDO PLENUS ---
elif menu_sel == "2. SALDO PLENUS":
    st.header("📂 SALDO PLENUS")
    op_ple = st.radio("Navegação Plenus:", ["Ler HTML / Importar", "Carregar do Histórico", "Gerenciar / Excluir"], 
                      horizontal=True, label_visibility="collapsed", key="nav_ple_183")
    st.divider()

    if op_ple == "Ler HTML / Importar":
        f_plenus = st.file_uploader("Importar HTML Plenus", type=["html", "htm"], key="up_plenus")
        if f_plenus:
            with st.spinner("Processando..."):
                df, erros = extrair_dados_plenus_html(f_plenus.getvalue().decode('utf-8', errors='ignore'), f_plenus.name)
                st.session_state['df_plenus'] = df
                st.session_state['lista_erro_plenus'] = erros
                st.session_state['ple_source'] = 'upload'

        if 'df_plenus' in st.session_state and not st.session_state['df_plenus'].empty and st.session_state.get('ple_source') == 'upload':
            c_btn, c_rest = st.columns([1, 4])
            with c_btn:
                if st.button("Limpar Plenus", type="primary"): 
                    del st.session_state['df_plenus']
                    st.rerun()
            
            if st.session_state.get('lista_erro_plenus'):
                 with st.expander("⚠️ Ver Erros de Leitura (Sem Total)", expanded=False):
                     st.dataframe(pd.DataFrame(st.session_state['lista_erro_plenus']))

            render_plenus_dashboard(st.session_state['df_plenus'], key_prefix="upload", allow_save=True)

    elif op_ple == "Carregar do Histórico":
        st.markdown("### 📂 Carregar Movimentos do DB")
        c_h1, c_h2 = st.columns(2)
        d_ini_h = c_h1.date_input("De:", value=st.session_state['p_dt_ini'], key="hist_p_ini", format="DD/MM/YYYY")
        d_fim_h = c_h2.date_input("Até:", value=st.session_state['p_dt_fim'], key="hist_p_fim", format="DD/MM/YYYY")
        st.session_state['p_dt_ini'] = d_ini_h
        st.session_state['p_dt_fim'] = d_fim_h

        if st.button("Carregar do Histórico", key="btn_load_hist_p"):
            df_hist = carregar_plenus_movimento_db(d_ini_h, d_fim_h)
            if not df_hist.empty:
                df_hist.rename(columns={'tipo_movimento': 'tipo', 'saldo_apos': 'saldo'}, inplace=True)
                df_hist['data'] = pd.to_datetime(df_hist['data_movimento']).dt.strftime("%d/%m/%Y")
                if 'categoria' not in df_hist.columns: df_hist['categoria'] = ""
                df_hist["Item_Completo"] = df_hist["produto"] + " (" + df_hist["categoria"].fillna("") + ")" 
                df_hist["Cat_Auto"] = df_hist["Item_Completo"].apply(detectar_categoria_plenus)
                
                st.session_state['df_plenus'] = df_hist
                st.session_state['lista_erro_plenus'] = []
                st.session_state['ple_source'] = 'history'
                st.success(f"Carregado {len(df_hist)} registros.")
                st.rerun()
            else:
                st.warning("Nenhum dado encontrado.")
        
        if 'df_plenus' in st.session_state and not st.session_state['df_plenus'].empty and st.session_state.get('ple_source') == 'history':
            st.divider()
            render_plenus_dashboard(st.session_state['df_plenus'], key_prefix="history", allow_save=False)
    
    elif op_ple == "Gerenciar / Excluir":
        st.markdown("### 🗑️ Excluir Movimentos")
        c_d1, c_d2 = st.columns(2)
        del_ini = c_d1.date_input("Início Exclusão:", key="del_ini_p", format="DD/MM/YYYY")
        del_fim = c_d2.date_input("Fim Exclusão:", key="del_fim_p", format="DD/MM/YYYY")
        if st.button("🗑️ Apagar Período (Plenus)", type="primary", key="btn_del_p"):
            qtde = excluir_periodo_tabela("plenus_historico", "data_movimento", del_ini, del_fim)
            st.success(f"{qtde} registros apagados.")

# --- 3. HISTORICO TRANSFORMAÇÃO ---
elif menu_sel == "3. HISTORICO TRANSFORMAÇÃO":
    st.header("🔄 HISTÓRICO TRANSFORMAÇÃO")
    
    if st.session_state['view_transf'] is not None and not st.session_state['view_transf'].empty:
        tab_query, tab_import, tab_manage = st.tabs(["📊 Consultar DB (Ativo)", "📥 Importar Excel", "🗑️ Limpar Período"])
    else:
        tab_import, tab_query, tab_manage = st.tabs(["📥 Importar Excel", "📊 Consultar DB", "🗑️ Limpar Período"])

    with tab_import:
        uploaded_files = st.file_uploader("Carregar Excel (Transf)", type=["xlsx", "xls"], accept_multiple_files=True, key="up_transf")
        if uploaded_files:
            current_file_names = sorted([f.name for f in uploaded_files])
            if st.session_state['st_df_transf_preview'] is None or st.session_state.get('last_files_transf') != current_file_names:
                all_dfs = []
                with st.spinner(f"Processando {len(uploaded_files)} arquivos..."):
                    for file in uploaded_files:
                        try:
                            df_raw = pd.read_excel(file, usecols=COLS_SISTRANSF_EXCEL, dtype=str)
                            if "Volume Origem" in df_raw.columns:
                                df_raw["Volume Origem"] = df_raw["Volume Origem"].str.replace(",", ".").astype(float)
                            if "Volume Gerado" in df_raw.columns:
                                df_raw["Volume Gerado"] = df_raw["Volume Gerado"].str.replace(",", ".").astype(float)
                            df_transformed = transform_data_sistransf(df_raw, filename=file.name)
                            all_dfs.append(df_transformed)
                        except Exception as e:
                            st.error(f"Erro {file.name}: {e}")
                if all_dfs:
                    final_df = pd.concat(all_dfs, ignore_index=True)
                    df_datas = pd.to_datetime(final_df['data_realizacao'], errors='coerce').dropna()
                    if not df_datas.empty:
                        min_d, max_d = df_datas.min().date(), df_datas.max().date()
                        update_session_dates('t', min_d, max_d)
                    
                    st.session_state['st_df_transf_preview'] = final_df
                    st.session_state['last_files_transf'] = current_file_names
                    st.success(f"Processado! {len(final_df)} linhas.")

        if st.session_state['st_df_transf_preview'] is not None:
            render_filtered_table(st.session_state['st_df_transf_preview'], "transf_preview")
            if st.button("💾 Salvar Transformações no DB", key="btn_save_transf"):
                ins, ext = salvar_lote_smart('transf_historico', 'data_realizacao', st.session_state['st_df_transf_preview'])
                if ins > 0: st.success(f"✅ {ins} salvos.")
                if ext > 0: st.warning(f"⚠️ {ext} já existiam.")
                st.session_state['st_df_transf_preview'] = None
                st.rerun()

    with tab_query:
        c_dt1, c_dt2 = st.columns(2)
        dt_ini = c_dt1.date_input("De:", value=st.session_state['t_dt_ini'], key="t_dt_ini_w", format="DD/MM/YYYY")
        dt_fim = c_dt2.date_input("Até:", value=st.session_state['t_dt_fim'], key="t_dt_fim_w", format="DD/MM/YYYY")
        st.session_state['t_dt_ini'] = dt_ini
        st.session_state['t_dt_fim'] = dt_fim
        
        if st.button("Consultar", key="btn_search_transf"):
            df_banco = carregar_transf_filtrado_db(dt_ini, dt_fim, st.session_state['filtros_ativos_transf'])
            if not df_banco.empty and 'numero' in df_banco.columns and 'data_realizacao' in df_banco.columns:
                df_banco = df_banco.sort_values(by=['numero', 'data_realizacao'])
            st.session_state['view_transf'] = df_banco
            st.rerun()
        
        if st.session_state['view_transf'] is not None:
            render_filtered_table(st.session_state['view_transf'], "transf_view")

    with tab_manage:
        c_del1, c_del2 = st.columns(2)
        del_ini = c_del1.date_input("Início:", key="del_ini_transf", format="DD/MM/YYYY")
        del_fim = c_del2.date_input("Fim:", key="del_fim_transf", format="DD/MM/YYYY")
        if st.button("Apagar Período Transf", type="primary", key="btn_del_transf"):
            qtde = excluir_periodo_tabela("transf_historico", "data_realizacao", del_ini, del_fim)
            st.success(f"{qtde} registros removidos.")

# --- 4. DEBITO CONSUMO ---
elif menu_sel == "4. DEBITO CONSUMO":
    st.header("🚚 DEBITO CONSUMO")
    
    if st.session_state['view_consumo'] is not None and not st.session_state['view_consumo'].empty:
        tab_c_view, tab_c_import, tab_c_del = st.tabs(["📊 Consultar DB (Ativo)", "📥 Importar Excel", "🗑️ Limpar Período"])
    else:
        tab_c_import, tab_c_view, tab_c_del = st.tabs(["📥 Importar Excel", "📊 Consultar DB", "🗑️ Limpar Período"])
    
    with tab_c_import:
        uploaded_files = st.file_uploader("Carregue arquivos Excel", type=["xlsx", "csv"], accept_multiple_files=True, key="up_consumo")
        
        if uploaded_files:
            try:
                all_dataframes = []
                with st.spinner("Lendo..."):
                    for file in uploaded_files:
                        df_temp = load_data_consumo_excel(file)
                        df_temp['_arquivo_origem_temp'] = file.name
                        all_dataframes.append(df_temp)
                
                if all_dataframes:
                    df_loaded = pd.concat(all_dataframes, ignore_index=True)
                    if 'Data' in df_loaded.columns: 
                        df_loaded = df_loaded.sort_values(by='Data')
                        df_datas = pd.to_datetime(df_loaded['Data'], errors='coerce').dropna()
                        if not df_datas.empty:
                             min_d, max_d = df_datas.min().date(), df_datas.max().date()
                             update_session_dates('c', min_d, max_d)

                    render_filtered_table(df_loaded, "cons_preview")
                    
                    if st.button("💾 CONFIRMAR: Salvar no DB", key="btn_save_consumo"):
                        df_to_save = pd.DataFrame()
                        col_data = 'Data' if 'Data' in df_loaded.columns else df_loaded.columns[0]
                        col_prod = 'Nome Popular' if 'Nome Popular' in df_loaded.columns else 'Produto'
                        col_vol = 'Quantidade' if 'Quantidade' in df_loaded.columns else 'Volume'
                        col_motivo = 'Motivo' if 'Motivo' in df_loaded.columns else 'Documento'

                        df_to_save['data_consumo'] = df_loaded.get(col_data, "")
                        df_to_save['produto'] = df_loaded.get(col_prod, "")
                        df_to_save['essencia'] = "" 
                        df_to_save['volume'] = df_loaded.get(col_vol, 0)
                        df_to_save['documento'] = df_loaded.get(col_motivo, "")
                        df_to_save['arquivo_origem'] = df_loaded['_arquivo_origem_temp']
                        
                        df_json_prep = df_loaded.copy()
                        if 'Data' in df_json_prep.columns: df_json_prep['Data'] = df_json_prep['Data'].astype(str)
                        df_to_save['dados_json'] = df_json_prep.apply(lambda x: json.dumps(x.to_dict(), default=str), axis=1)
                        
                        ins, ext = salvar_lote_smart('consumo_historico', 'data_consumo', df_to_save)
                        if ins > 0: st.success(f"✅ {ins} salvos.")
                        if ext > 0: st.warning(f"⚠️ {ext} já existiam.")
                        time.sleep(1)
                        st.rerun()
            except Exception as e: st.error(f"Erro: {e}")

    with tab_c_view:
        d_ini_c = st.date_input("De:", value=st.session_state['c_dt_ini'], key="c_dt_ini_w", format="DD/MM/YYYY")
        d_fim_c = st.date_input("Até:", value=st.session_state['c_dt_fim'], key="c_dt_fim_w", format="DD/MM/YYYY")
        st.session_state['c_dt_ini'] = d_ini_c
        st.session_state['c_dt_fim'] = d_fim_c
        
        if st.button("Consultar Consumo", key="btn_search_consumo"):
            df_c_res = carregar_consumo_filtrado_db(d_ini_c, d_fim_c)
            st.session_state['view_consumo'] = df_c_res
            st.rerun()
        
        if st.session_state['view_consumo'] is not None:
            render_filtered_table(st.session_state['view_consumo'], "cons_view")

    with tab_c_del:
        del_ini_c = st.date_input("Início:", key="deli_c", format="DD/MM/YYYY")
        del_fim_c = st.date_input("Fim:", key="delf_c", format="DD/MM/YYYY")
        if st.button("Apagar Período Consumo", type="primary", key="btn_del_consumo"):
            qtde = excluir_periodo_tabela("consumo_historico", "data_consumo", del_ini_c, del_fim_c)
            st.success(f"{qtde} registros apagados.")

# --- 5. GESTÃO VÍNCULOS ---
elif menu_sel == "5. Gestão: Vínculos (Admin)":
    st.header("⚙️ Gestão de Vínculos e Grupos")
    
    pend_sis_count = 0
    pend_ple_count = 0
    pend_vinc_count = 0
    if 'df_sisflora' in st.session_state:
        lista_agrupados = list(st.session_state['agrup_sis'].keys())
        pend_sis_count = st.session_state['df_sisflora'][~st.session_state['df_sisflora']['Item_Completo'].isin(lista_agrupados)]['Item_Completo'].nunique()
    if 'df_plenus' in st.session_state:
        lista_agrupados_p = list(st.session_state['agrup_ple'].keys())
        if 'Item_Completo' not in st.session_state['df_plenus'].columns:
             st.session_state['df_plenus']['Item_Completo'] = st.session_state['df_plenus']["produto"] + " (" + st.session_state['df_plenus']["categoria"].fillna("") + ")"
        pend_ple_count = st.session_state['df_plenus'][~st.session_state['df_plenus']['Item_Completo'].isin(lista_agrupados_p)]['Item_Completo'].nunique()
        
    grps_sis = carregar_lista_grupos_db("SISFLORA")
    grps_vinc = carregar_vinculos_db().values()
    pend_vinc_count = len([g for g in grps_sis if g not in grps_vinc])
    
    col_a, col_b, col_c = st.columns(3)
    col_a.metric("Pend. Sisflora", pend_sis_count)
    col_b.metric("Pend. Plenus", pend_ple_count)
    col_c.metric("Sem Vínculo", pend_vinc_count)
    st.divider()

    admin_mode = st.radio("Ação:", ["Agrupar Sisflora", "Agrupar Plenus", "Vincular (IA)", "Vínculo Manual", "Gerenciar Grupos"], horizontal=True)
    
    if admin_mode == "Agrupar Sisflora":
        if 'df_sisflora' in st.session_state:
            c1, c2 = st.columns([2, 1])
            cat_sel = c1.selectbox("Categoria:", [""] + sorted(st.session_state['df_sisflora']['Cat_Auto'].unique()), key="s_cat_adm")
            txt_sel = c2.text_input("Pesquisar Nome:", key="s_txt_adm")
            lista_agrupados = list(st.session_state['agrup_sis'].keys())
            mask_pend = ~st.session_state['df_sisflora']['Item_Completo'].isin(lista_agrupados)
            mask_cesta = ~st.session_state['df_sisflora']['Item_Completo'].isin(st.session_state['cesta_sis'])
            df_pend = st.session_state['df_sisflora'][mask_pend & mask_cesta].copy()
            if cat_sel: df_pend = df_pend[df_pend['Cat_Auto'] == cat_sel]
            if txt_sel: df_pend = df_pend[df_pend['Item_Completo'].str.contains(txt_sel, case=False, na=False)]
            lista_filtrada = sorted(df_pend['Item_Completo'].unique(), key=sort_key_nomes)
            c_esq, c_dir = st.columns([1, 1])
            with c_esq:
                st.markdown(f"#### 📡 Radar ({len(lista_filtrada)})")
                if lista_filtrada:
                    df_view = pd.DataFrame(lista_filtrada, columns=["Itens"])
                    event = st.dataframe(df_view, use_container_width=True, height=400, on_select="rerun", selection_mode="single-row", hide_index=True, key=f"rad_sis_{st.session_state['cnt_sis']}")
                    if event.selection.rows:
                        item_clicado = df_view.iloc[event.selection.rows[0]]['Itens']
                        st.session_state['cesta_sis'].append(item_clicado)
                        st.session_state['cnt_sis'] += 1
                        st.session_state['input_sis_name'] = gerar_sugestao_nome_primeiro(st.session_state['cesta_sis'], cat_sel, "SISFLORA")
                        st.rerun()
            with c_dir:
                st.markdown(f"#### 🧺 Cesta ({len(st.session_state['cesta_sis'])})")
                cesta_atual = st.multiselect("Itens:", st.session_state['cesta_sis'], default=st.session_state['cesta_sis'], key="ms_cesta_sis")
                if len(cesta_atual) != len(st.session_state['cesta_sis']):
                     st.session_state['cesta_sis'] = cesta_atual
                     st.rerun()
                st.text_input("Nome do Grupo Final:", key="input_sis_name")
                st.button("💾 SALVAR GRUPO", key="sav_sis", type="primary", on_click=salvar_sis_click)
    
    elif admin_mode == "Agrupar Plenus":
        if 'df_plenus' in st.session_state:
            if 'Item_Completo' not in st.session_state['df_plenus'].columns:
                 st.session_state['df_plenus']['Item_Completo'] = st.session_state['df_plenus']["produto"] + " (" + st.session_state['df_plenus']["categoria"].fillna("") + ")"
            c1, c2 = st.columns([2, 1])
            cats_p = sorted(st.session_state['df_plenus']['categoria'].fillna("").unique()) if 'categoria' in st.session_state['df_plenus'].columns else []
            cat_sel_p = c1.selectbox("Categoria:", [""] + cats_p, key="p_cat_adm")
            txt_sel_p = c2.text_input("Pesquisar Nome:", key="p_txt_adm")
            lista_agrupados_p = list(st.session_state['agrup_ple'].keys())
            mask_pend_p = ~st.session_state['df_plenus']['Item_Completo'].isin(lista_agrupados_p)
            mask_cesta_p = ~st.session_state['df_plenus']['Item_Completo'].isin(st.session_state['cesta_ple'])
            df_pend_p = st.session_state['df_plenus'][mask_pend_p & mask_cesta_p].copy()
            if cat_sel_p: df_pend_p = df_pend_p[df_pend_p['categoria'] == cat_sel_p]
            if txt_sel_p: df_pend_p = df_pend_p[df_pend_p['Item_Completo'].str.contains(txt_sel_p, case=False, na=False)]
            lista_filtrada_p = sorted(df_pend_p['Item_Completo'].unique(), key=sort_key_nomes)
            c_esq, c_dir = st.columns([1, 1])
            with c_esq:
                st.markdown(f"#### 📡 Radar ({len(lista_filtrada_p)})")
                if lista_filtrada_p:
                    df_view_p = pd.DataFrame(lista_filtrada_p, columns=["Itens"])
                    event_p = st.dataframe(df_view_p, use_container_width=True, height=400, on_select="rerun", selection_mode="single-row", hide_index=True, key=f"rad_ple_{st.session_state['cnt_ple']}")
                    if event_p.selection.rows:
                        item_p = df_view_p.iloc[event_p.selection.rows[0]]['Itens']
                        st.session_state['cesta_ple'].append(item_p)
                        st.session_state['cnt_ple'] += 1
                        st.session_state['input_ple_name'] = gerar_sugestao_nome_primeiro(st.session_state['cesta_ple'], cat_sel_p, "PLENUS")
                        st.rerun()
            with c_dir:
                st.markdown(f"#### 🧺 Cesta ({len(st.session_state['cesta_ple'])})")
                cesta_p = st.multiselect("Itens:", st.session_state['cesta_ple'], default=st.session_state['cesta_ple'], key="ms_cesta_ple")
                if len(cesta_p) != len(st.session_state['cesta_ple']):
                    st.session_state['cesta_ple'] = cesta_p
                    st.rerun()
                st.text_input("Nome do Grupo Final:", key="input_ple_name")
                st.button("💾 SALVAR GRUPO", key="sav_ple", type="primary", on_click=salvar_ple_click)

    elif admin_mode == "Vincular (IA)":
        grps_sis = carregar_lista_grupos_db("SISFLORA")
        grps_ple = carregar_lista_grupos_db("PLENUS")
        vinculos_atuais = carregar_vinculos_db()
        with st.expander("🤖 Sugestões Inteligentes", expanded=True):
            f_cat_ia = st.selectbox("Filtrar por Categoria:", ["TODAS", "TORAS", "SERRADAS", "BENEFICIADAS"], key="sel_cat_ia")
            if st.button("🔎 Buscar Sugestões"):
                with st.spinner(f"Analisando..."):
                    mapa_cat_sis = get_categorias_dos_grupos("SISFLORA")
                    mapa_cat_ple = get_categorias_dos_grupos("PLENUS")
                    sugestoes = []
                    for gp in grps_ple:
                        is_vinculado = gp in vinculos_atuais
                        status_vinc = f"✅ Já vinculado a: {vinculos_atuais[gp]}" if is_vinculado else ""
                        cat_p = mapa_cat_ple.get(gp, "OUTROS")
                        if f_cat_ia != "TODAS":
                            cat_normal = cat_p
                            if cat_p in ["TORAS", "TOROS", "TORA"]: cat_normal = "TORAS"
                            elif "SERRADA" in cat_p: cat_normal = "SERRADAS"
                            elif "BENEF" in cat_p: cat_normal = "BENEFICIADAS"
                            if cat_normal != f_cat_ia: continue
                        
                        melhor_match = None
                        maior_score = 0.0
                        for gs in grps_sis:
                            score = calcular_similaridade_avancada(gp, gs)
                            if score > 0.65 and score > maior_score:
                                maior_score = score
                                melhor_match = gs
                        if melhor_match:
                            sugestoes.append({
                                "Plenus": gp,
                                "Sisflora (Sugerido)": melhor_match,
                                "Categoria": cat_p,
                                "Status": status_vinc,
                                "Confiança": f"{maior_score:.0%}",
                                "Aceitar": False,
                                "is_locked": is_vinculado
                            })
                    st.session_state['sugestoes_ia'] = sugestoes
            if 'sugestoes_ia' in st.session_state and st.session_state['sugestoes_ia']:
                df_sug = pd.DataFrame(st.session_state['sugestoes_ia'])
                if not df_sug.empty:
                    edited_df = st.data_editor(
                        df_sug,
                        column_config={
                            "Aceitar": st.column_config.CheckboxColumn("Vincular?", default=False, disabled="is_locked"),
                        },
                        disabled=["Plenus", "Sisflora (Sugerido)", "Categoria", "Confiança", "Status", "is_locked"],
                        hide_index=True, use_container_width=True
                    )
                    if st.button("✅ Confirmar Vínculos"):
                        vincular_agora = edited_df[edited_df["Aceitar"] == True]
                        if not vincular_agora.empty:
                            pl_list = vincular_agora['Plenus'].tolist()
                            si_list = vincular_agora['Sisflora (Sugerido)'].tolist()
                            for p, s in zip(pl_list, si_list):
                                salvar_vinculo_db([p], s)
                            st.success("Vínculos criados!")
                            st.session_state['vinculos'] = carregar_vinculos_db()
                            st.rerun()

    elif admin_mode == "Vínculo Manual":
        grps_sis = carregar_lista_grupos_db("SISFLORA")
        grps_ple = carregar_lista_grupos_db("PLENUS")
        vinculos_atuais = carregar_vinculos_db()
        txt_manual = st.text_input("🔎 Pesquisar Nome:", key="search_manual_both")
        c_esq, c_dir = st.columns(2)
        selected_sis = None
        selected_ple = None
        
        with c_esq:
            st.markdown("**1. Sisflora**")
            df_s_view = pd.DataFrame([g for g in sorted(grps_sis) if not txt_manual or txt_manual.upper() in g.upper()], columns=["Grupo Sisflora"])
            event_s = st.dataframe(df_s_view, use_container_width=True, height=400, on_select="rerun", selection_mode="single-row", hide_index=True)
            if event_s.selection.rows:
                selected_sis = df_s_view.iloc[event_s.selection.rows[0]]['Grupo Sisflora']
        with c_dir:
            st.markdown("**2. Plenus**")
            df_p_view = pd.DataFrame([g for g in sorted(grps_ple) if not txt_manual or txt_manual.upper() in g.upper()], columns=["Grupo Plenus"])
            event_p = st.dataframe(df_p_view, use_container_width=True, height=400, on_select="rerun", selection_mode="single-row", hide_index=True)
            if event_p.selection.rows:
                selected_ple = df_p_view.iloc[event_p.selection.rows[0]]['Grupo Plenus']
        
        if selected_sis and selected_ple:
            if st.button(f"🔗 Vincular: {selected_ple} -> {selected_sis}", type="primary"):
                salvar_vinculo_db([selected_ple], selected_sis)
                st.session_state['vinculos'] = carregar_vinculos_db()
                st.rerun()

    elif admin_mode == "Gerenciar Grupos":
        ger_mode = st.radio("Opção:", ["🗑️ Excluir/Editar", "📊 Relatório Geral"], horizontal=True)
        if ger_mode == "🗑️ Excluir/Editar":
            tipo_ger = st.radio("Origem:", ["Sisflora", "Plenus"], horizontal=True, key="rad_ger")
            origem_db = "SISFLORA" if tipo_ger == "Sisflora" else "PLENUS"
            mapa_atual = carregar_agrupamentos_db(origem_db)
            grupos_unicos = sorted(list(set(mapa_atual.values())))
            grp_del = st.selectbox("Selecione o Grupo para EXCLUIR:", [""] + grupos_unicos, key="sel_grp_del")
            if grp_del:
                itens_no_grupo = [k for k, v in mapa_atual.items() if v == grp_del]
                st.dataframe(pd.DataFrame(itens_no_grupo, columns=["Itens"]), use_container_width=True)
                if st.button("🗑️ CONFIRMAR EXCLUSÃO", type="primary"):
                    excluir_grupo_db(grp_del, origem_db)
                    st.success("Excluído!")
                    time.sleep(1)
                    st.rerun()
        else:
            df_rel = carregar_todos_agrupamentos_db()
            if not df_rel.empty:
                excel_bytes = to_excel_autoajustado(df_rel)
                st.download_button("📥 Baixar Relatório", excel_bytes, "relatorio_grupos.xlsx")
                st.dataframe(df_rel, use_container_width=True)

# --- 6. CONFERÊNCIA ---
elif menu_sel == "6. Conferência & Auditoria":
    st.header("⚖️ Resultado Final")
    tab_conf_saldo, tab_conf_auditoria = st.tabs(["SALDO ESTÁTICO", "AUDITORIA DE FLUXO"])

    with tab_conf_saldo:
        if 'df_sisflora' in st.session_state and 'df_plenus' in st.session_state:
            df_s = st.session_state['df_sisflora'].copy()
            df_s['Grupo'] = df_s['Item_Completo'].map(st.session_state['agrup_sis'])
            res_s = df_s.dropna(subset=['Grupo']).groupby('Grupo')['Volume Disponivel'].sum().reset_index()
            
            df_p = st.session_state['df_plenus'].copy()
            if 'Item_Completo' not in df_p.columns:
                 df_p['Item_Completo'] = df_p["produto"] + " (" + df_p["categoria"].fillna("") + ")"
            
            df_p_last = df_p.sort_values(by=['data_movimento'] if 'data_movimento' in df_p.columns else ['sku']).drop_duplicates(subset=['sku'], keep='last').copy()
            df_p_last['Grupo_Inter'] = df_p_last['Item_Completo'].map(st.session_state['agrup_ple'])
            df_p_last['Grupo_Calc'] = df_p_last['Grupo_Inter'].map(st.session_state['vinculos']).fillna(df_p_last['Grupo_Inter'])
            
            col_saldo = 'saldo_apos' if 'saldo_apos' in df_p_last.columns else 'saldo'
            if col_saldo in df_p_last.columns: df_p_last[col_saldo] = pd.to_numeric(df_p_last[col_saldo], errors='coerce').fillna(0)
            res_p = df_p_last.dropna(subset=['Grupo_Calc']).groupby('Grupo_Calc')[col_saldo].sum().reset_index()
            
            res_s.columns = ['Grupo', 'Vol_Sis']
            res_p.columns = ['Grupo', 'Vol_Ple']
            df_final = pd.merge(res_s, res_p, on='Grupo', how='outer').fillna(0)
            df_final['Diferenca'] = df_final['Vol_Sis'] - df_final['Vol_Ple']
            
            def highlight_diff(val):
                color = 'green' if abs(val) < 0.01 else ('red' if val < 0 else 'blue')
                return f'color: {color}; font-weight: bold'
            
            st.dataframe(df_final.style.map(highlight_diff, subset=['Diferenca']).format("{:,.4f}"), use_container_width=True, height=600)
        else:
            st.info("Carregue os saldos Sisflora e Plenus primeiro.")

    with tab_conf_auditoria:
        c_dt1, c_dt2 = st.columns(2)
        dt_ini_aud = c_dt1.date_input("Início:", value=st.session_state['aud_dt_ini'], key="aud_i", format="DD/MM/YYYY")
        dt_fim_aud = c_dt2.date_input("Fim:", value=st.session_state['aud_dt_fim'], key="aud_f", format="DD/MM/YYYY")
        st.session_state['aud_dt_ini'] = dt_ini_aud
        st.session_state['aud_dt_fim'] = dt_fim_aud
        
        if st.button("🚀 Processar Auditoria"):
            with st.spinner("Analisando DB..."):
                df_transf = carregar_transf_filtrado_db(dt_ini_aud, dt_fim_aud)
                df_consumo = carregar_consumo_filtrado_db(dt_ini_aud, dt_fim_aud)
                df_plenus_mov = carregar_plenus_movimento_db(dt_ini_aud, dt_fim_aud)
                
                saldo_aud_sis = {}
                agrup_sis = st.session_state['agrup_sis']
                vinculos = st.session_state['vinculos']
                
                # ... (LÓGICA DE AUDITORIA MANTIDA IDÊNTICA AO ORIGINAL) ...
                if not df_transf.empty:
                    gerados = df_transf[df_transf['tipo_produto'] == 'PRODUTO GERADO'].copy()
                    if not gerados.empty:
                        gerados['Item_Check'] = gerados.apply(lambda x: f"{x['produto']} - {x['essencia']}" if x['essencia'] else x['produto'], axis=1)
                        gerados['Grupo'] = gerados['Item_Check'].map(agrup_sis).fillna(gerados['Item_Check'])
                        for _, r in gerados.iterrows():
                             if pd.notnull(r['Grupo']):
                                 if r['Grupo'] not in saldo_aud_sis: saldo_aud_sis[r['Grupo']] = {'Entrada': 0, 'Saida': 0}
                                 saldo_aud_sis[r['Grupo']]['Entrada'] += r['volume']

                    origens = df_transf[df_transf['tipo_produto'] == 'PRODUTO DE ORIGEM'].copy()
                    if not origens.empty:
                        origens['Item_Check'] = origens.apply(lambda x: f"{x['produto']} - {x['essencia']}" if x['essencia'] else x['produto'], axis=1)
                        origens['Grupo'] = origens['Item_Check'].map(agrup_sis).fillna(origens['Item_Check'])
                        for _, r in origens.iterrows():
                             if pd.notnull(r['Grupo']):
                                 if r['Grupo'] not in saldo_aud_sis: saldo_aud_sis[r['Grupo']] = {'Entrada': 0, 'Saida': 0}
                                 saldo_aud_sis[r['Grupo']]['Saida'] += r['volume']

                if not df_consumo.empty:
                    df_consumo['Item_Check'] = df_consumo.apply(lambda x: f"{x.get('produto','')}" if not x.get('essencia') else f"{x.get('produto','')} - {x.get('essencia','')}", axis=1)
                    df_consumo['Grupo'] = df_consumo['Item_Check'].map(agrup_sis).fillna(df_consumo['Item_Check'])
                    for _, r in df_consumo.iterrows():
                        if pd.notnull(r['Grupo']):
                             if r['Grupo'] not in saldo_aud_sis: saldo_aud_sis[r['Grupo']] = {'Entrada': 0, 'Saida': 0}
                             saldo_aud_sis[r['Grupo']]['Saida'] += float(r.get('volume', 0))

                saldo_aud_ple = {}
                agrup_ple = st.session_state['agrup_ple']
                if not df_plenus_mov.empty:
                    df_plenus_mov['Item_Completo'] = df_plenus_mov["produto"] + " (" + df_plenus_mov["categoria"].fillna("") + ")"
                    df_plenus_mov['Grupo_Inter'] = df_plenus_mov['Item_Completo'].map(agrup_ple)
                    df_plenus_mov['Grupo_Calc'] = df_plenus_mov['Grupo_Inter'].map(vinculos).fillna(df_plenus_mov['Grupo_Inter'])
                    for _, r in df_plenus_mov.iterrows():
                        if pd.notnull(r['Grupo_Calc']):
                            grp = r['Grupo_Calc']
                            if grp not in saldo_aud_ple: saldo_aud_ple[grp] = {'Entrada': 0, 'Saida': 0}
                            saldo_aud_ple[grp]['Entrada'] += r['entrada']
                            saldo_aud_ple[grp]['Saida'] += r['saida']

                todos_grupos = set(saldo_aud_sis.keys()) | set(saldo_aud_ple.keys())
                relatorio = []
                for g in todos_grupos:
                    s = saldo_aud_sis.get(g, {'Entrada': 0, 'Saida': 0})
                    p = saldo_aud_ple.get(g, {'Entrada': 0, 'Saida': 0})
                    s_liq = s['Entrada'] - s['Saida']
                    p_liq = p['Entrada'] - p['Saida']
                    diff = s_liq - p_liq
                    
                    if any(abs(x) > 0.0001 for x in [s['Entrada'], s['Saida'], s_liq, p['Entrada'], p['Saida'], p_liq]):
                        relatorio.append({
                            "Grupo": g,
                            "Sis_Ent": s['Entrada'], "Sis_Sai": s['Saida'], "Sis_Liq": s_liq,
                            "Ple_Ent": p['Entrada'], "Ple_Sai": p['Saida'], "Ple_Liq": p_liq,
                            "Diferenca": diff
                        })

                if relatorio:
                    df_rel = pd.DataFrame(relatorio)
                    st.dataframe(df_rel.style.format("{:,.4f}"), use_container_width=True)
                else:
                    st.info("Nenhuma movimentação no período.")
