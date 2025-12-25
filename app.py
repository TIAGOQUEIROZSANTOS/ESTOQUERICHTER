import streamlit as st
import pandas as pd
import json
import os
import time
import shutil
import logging
from datetime import datetime, date

# Importações dos módulos criados
from database import (
    init_db, load_app_state, save_app_state, get_db_connection,
    get_max_date_db, get_smart_date_range, update_session_dates,
    carregar_agrupamentos_db, carregar_todos_agrupamentos_db, get_categorias_dos_grupos,
    salvar_agrupamento_db, excluir_grupo_db, carregar_vinculos_db,
    carregar_lista_grupos_db, salvar_vinculo_db, excluir_vinculo_db,
    salvar_lote_smart, excluir_periodo_tabela,
    carregar_transf_filtrado_db, get_valores_unicos_coluna,
    carregar_plenus_movimento_db, buscar_ultimo_saldo_antes_data_db, buscar_todos_skus_db, carregar_consumo_filtrado_db,
    salvar_lote_sisflora_db, carregar_sisflora_data_db,
    get_datas_sisflora_disponiveis, excluir_sisflora_por_data
)
from processamento import (
    parse_float_inteligente, formatar_br, parse_data_br,
    detecting_category, detectar_categoria_plenus, sort_key_nomes,
    transform_data_sistransf, to_excel_autoajustado,
    limpar_para_comparacao, calcular_similaridade_avancada,
    gerar_sugestao_nome_primeiro,
    extrair_dados_sisflora, load_data_consumo_excel,
    extrair_dados_plenus_html,
    COLS_SISTRANSF_EXCEL
)
from ui import render_filtered_table, render_plenus_dashboard

# --- CONFIGURAÇÃO ---
st.set_page_config(page_title="🌲 Sistema S&P - Conferência v194", layout="wide")
PASTA_HISTORICO_LEGADO = "historico_sessoes"

if not os.path.exists(PASTA_HISTORICO_LEGADO):
    os.makedirs(PASTA_HISTORICO_LEGADO)

# --- LOGGING ---
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s: %(message)s")

# --- ESTILOS CSS ---
st.markdown("""
    <style>
    /* Ajustes Gerais */
    .block-container { padding-top: 2rem; }

    /* Scroll Behavior v180 */
    .stDataFrame { overscroll-behavior: contain; }
    div[data-testid="stDataFrame"] > div { overscroll-behavior: contain; }

    /* Sidebar */
    section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] {
        display: flex;
        flex-direction: column;
        gap: 10px;
    }
    section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] label {
        background-color: #f8f9fa;
        border: 1px solid #dee2e6;
        border-radius: 5px;
        padding: 10px;
        font-weight: 500;
        cursor: pointer;
    }
    section[data-testid="stSidebar"] .stRadio div[role="radiogroup"] label:hover {
        background-color: #e2e6ea;
        border-color: #adb5bd;
    }

    /* Botões */
    .stButton button {
        width: 100%;
        font-weight: bold;
        background-color: #28a745;
        color: white;
        height: 45px;
        border-radius: 5px;
    }
    .stButton button:hover {
        background-color: #218838;
        color: white;
    }
    div[data-testid="stVerticalBlock"] > div > button[kind="secondary"] {
        background-color: #007bff !important;
        color: white !important;
        border: none;
    }

    /* Tabela */
    div[data-testid="stDataFrame"] div[role="grid"] div[role="row"] {
       min-height: 30px !important;
       font-size: 14px !important;
    }

    /* Subtítulos de Grupos */
    .grupo-title {
        padding: 8px 12px;
        border-radius: 5px;
        font-weight: bold;
        margin-top: 15px;
        margin-bottom: 5px;
    }
    </style>
""", unsafe_allow_html=True)

# --- INICIALIZAÇÃO DE VARIÁVEIS PERSISTENTES ---
def init_session_vars():
    # Plenus
    if 'p_dt_ini' not in st.session_state:
        i, f = get_smart_date_range('plenus_historico', 'data_movimento')
        st.session_state['p_dt_ini'] = i
        st.session_state['p_dt_fim'] = f
    if 'view_plenus' not in st.session_state:
        st.session_state['view_plenus'] = None

    # Sistransf
    if 't_dt_ini' not in st.session_state:
        i, f = get_smart_date_range('transf_historico', 'data_realizacao')
        st.session_state['t_dt_ini'] = i
        st.session_state['t_dt_fim'] = f
    if 'view_transf' not in st.session_state:
        st.session_state['view_transf'] = None

    # SisConsumo
    if 'c_dt_ini' not in st.session_state:
        i, f = get_smart_date_range('consumo_historico', 'data_consumo')
        st.session_state['c_dt_ini'] = i
        st.session_state['c_dt_fim'] = f
    if 'view_consumo' not in st.session_state:
        st.session_state['view_consumo'] = None

    # Auditoria
    if 'aud_dt_ini' not in st.session_state:
        d1 = get_max_date_db('plenus_historico', 'data_movimento')
        d2 = get_max_date_db('transf_historico', 'data_realizacao')
        mx = max([d for d in [d1, d2] if d]) if any([d1, d2]) else date.today()
        st.session_state['aud_dt_ini'] = date(mx.year, mx.month, 1)
        st.session_state['aud_dt_fim'] = mx

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

# --- INIT SESSION ---
init_db()
load_app_state()
init_session_vars()
if 'agrup_sis' not in st.session_state:
    st.session_state['agrup_sis'] = carregar_agrupamentos_db("SISFLORA")
if 'agrup_ple' not in st.session_state:
    st.session_state['agrup_ple'] = carregar_agrupamentos_db("PLENUS")
if 'vinculos' not in st.session_state:
    st.session_state['vinculos'] = carregar_vinculos_db()
if 'cesta_sis' not in st.session_state:
    st.session_state['cesta_sis'] = []
if 'cesta_ple' not in st.session_state:
    st.session_state['cesta_ple'] = []
if 'input_sis_name' not in st.session_state:
    st.session_state['input_sis_name'] = ""
if 'input_ple_name' not in st.session_state:
    st.session_state['input_ple_name'] = ""
if 'cnt_sis' not in st.session_state:
    st.session_state['cnt_sis'] = 0
if 'cnt_ple' not in st.session_state:
    st.session_state['cnt_ple'] = 0
if 'st_df_transf_preview' not in st.session_state:
    st.session_state['st_df_transf_preview'] = None
if 'filtros_ativos_transf' not in st.session_state:
    st.session_state['filtros_ativos_transf'] = []

# --- MENU FLUXO DE TRABALHO ---
ordem_menu = [
    "1. SALDO SISFLORA",
    "2. SALDO PLENUS",
    "3. HISTORICO TRANSFORMAÇÃO",
    "4. DEBITO CONSUMO",
    "5. Gestão: Vínculos (Admin)",
    "6. Conferência & Auditoria"
]

# Determina índice inicial baseado no salvo
idx_inicial = st.session_state.get('menu_sel_idx', 0)
if not isinstance(idx_inicial, int) or idx_inicial >= len(ordem_menu):
    idx_inicial = 0

def on_menu_change():
    # Salva o indice da opção selecionada
    sel = st.session_state.get('menu_main_nav')
    if sel in ordem_menu:
        st.session_state['menu_sel_idx'] = ordem_menu.index(sel)
    save_app_state()

menu_sel = st.sidebar.radio("Fluxo de Trabalho", ordem_menu, index=idx_inicial, key="menu_main_nav", on_change=on_menu_change)
st.sidebar.divider()
st.sidebar.info("💡 Siga a ordem numérica para o fluxo correto.")

# --- SISTEMA DE NOTAS DE VERSÃO ---
CHANGELOG = {
    "v194": {
        "data": "2025-01-31",
        "melhorias": [
            "✅ Adição das colunas vlr_unit e vlr_total ao banco de dados",
            "✅ Garantia de que colunas vlr_unit e vlr_total sempre existem (criadas com 0 se não existirem)",
            "✅ Reorganização de colunas: Saldo, Vlr Unitário, Total Vlr, Nota, Série",
            "✅ Correção do cálculo de saldo total ao carregar do histórico (usa mesma lógica do HTML)",
            "✅ Correção do erro TypeError ao filtrar por categoria",
            "✅ Documentação crítica: Regras para cálculo de saldo total (ver comentários no código)",
            "✅ Prevenção de importação duplicada: verifica se arquivo já foi importado antes de salvar",
            "✅ Correção: lógica unificada para cálculo com e sem filtros (identifica se veio do HTML)",
            "⚠️ REGRA CRÍTICA DE CÁLCULO DE SALDO (LER SEMPRE ANTES DE MODIFICAR):",
            "   1. Somar APENAS linhas com tipo='TOTAL' ou 'TOTAL:'",
            "   2. Somar APENAS o campo 'saldo' dessas linhas",
            "   3. NÃO somar linhas ANTERIOR, NÃO somar movimentações",
            "   4. Para HTML: NÃO remover duplicatas (HTML já tem todas corretas)",
            "   5. Para histórico que veio do HTML: NÃO remover duplicatas (banco é espelho do HTML)",
            "   6. Para histórico que não veio do HTML: remover duplicatas por SKU antes de somar",
            "   7. A MESMA LÓGICA deve ser usada tanto para saldo geral quanto para saldo com filtros",
            "   8. Com filtros: aplicar filtros ANTES de filtrar por tipo TOTAL",
            "   9. Identificar se veio do HTML pelo campo 'arquivo_origem'"
        ]
    },
    "v193": {
        "data": "2025-01-31",
        "melhorias": [
            "✅ Correção do cálculo de saldo total ao carregar do histórico (usa mesma lógica do HTML)",
            "✅ Correção do erro TypeError ao filtrar por categoria",
            "✅ Reorganização de colunas (NOTA e SÉRIE antes de VLR_UNIT)",
            "✅ Correção do cálculo de vlr_total na linha TOTAL GERAL da tabela",
            "✅ Melhorias na consistência entre saldo total da métrica e da tabela"
        ]
    },
    "v192": {
        "data": "2025-01-31",
        "melhorias": [
            "✅ Definição automática de datas ao carregar do histórico (início = primeiro dia do mês da última data)",
            "✅ Exibição de quantidade de SKUs listados na consulta",
            "✅ Correção de ordenação: linhas ANTERIOR e TOTAL aparecem juntas automaticamente",
            "✅ Correção de duplicatas: remoção de linhas TOTAL duplicadas",
            "✅ Produtos sem movimentação aparecem corretamente com linhas ANTERIOR e TOTAL",
            "✅ Melhorias na preservação de linhas TOTAL para produtos sem movimentações"
        ]
    },
    "v191": {
        "data": "2025-01-30",
        "melhorias": [
            "✅ Cálculo sequencial de saldo anterior e saldo final corrigido",
            "✅ Criação automática de linhas ANTERIOR e TOTAL para produtos",
            "✅ Melhorias no processamento de dados do histórico"
        ]
    },
    "v190": {
        "data": "2025-01-29",
        "melhorias": [
            "✅ Ordenação por SKU e tipo (ANTERIOR primeiro, TOTAL por último)",
            "✅ Melhorias na organização dos dados"
        ]
    },
    "v189": {
        "data": "2025-01-28",
        "melhorias": [
            "✅ Inclusão de saldo_anterior, nota, serie e valores sempre visíveis",
            "✅ Filtro de categoria aprimorado"
        ]
    }
}

# Exibir notas de versão na sidebar
with st.sidebar.expander("📋 Notas de Versão v194", expanded=False):
    st.markdown("### 🆕 O que melhorou na v194:")
    for melhoria in CHANGELOG["v194"]["melhorias"]:
        st.markdown(f"{melhoria}")

    st.divider()

    # Checkbox "Como ler"
    mostrar_como_ler = st.checkbox("❓ Como ler as notas de versão", key="chk_como_ler_versao", value=False)

    if mostrar_como_ler:
        st.info("""
        **📖 Como interpretar as notas de versão:**

        - ✅ **Check verde**: Funcionalidade implementada ou correção aplicada
        - Cada versão lista as melhorias e correções implementadas
        - As notas são organizadas por versão, da mais recente para a mais antiga
        - Marque/desmarque o checkbox acima para mostrar/ocultar esta explicação

        **💡 Dica:** As melhorias mais recentes aparecem primeiro na lista.

        **📝 Estrutura:**
        - Versão atual (v194) é exibida expandida por padrão
        - Versões anteriores podem ser visualizadas expandindo os itens abaixo
        - Cada melhoria é descrita de forma clara e objetiva
        """)

    st.divider()
    st.markdown("**📚 Versões anteriores:**")
    for versao in sorted(CHANGELOG.keys(), reverse=True)[1:]:  # Pula a v194 (já mostrada)
        with st.expander(f"Versão {versao} ({CHANGELOG[versao]['data']})"):
            for melhoria in CHANGELOG[versao]["melhorias"]:
                st.markdown(f"{melhoria}")

# Título dinâmico baseado no módulo selecionado
if menu_sel == "1. SALDO SISFLORA":
    st.title("🌲 CONFERENCIA ESTOQUE v194 - SALDO SISFLORA")
elif menu_sel == "2. SALDO PLENUS":
    st.title("🌲 CONFERENCIA ESTOQUE v194 - SALDO PLENUS")
    # Nota de versão expandível no topo
    with st.expander("📋 Notas da Versão v194 - Clique para ver o que melhorou", expanded=False):
        st.markdown("### 🆕 Principais melhorias na v194:")
        for melhoria in CHANGELOG["v194"]["melhorias"]:
            st.markdown(f"{melhoria}")

        st.divider()
        mostrar_como_ler_topo = st.checkbox("❓ Como ler as notas de versão", key="chk_como_ler_versao_topo", value=False)
        if mostrar_como_ler_topo:
            st.info("""
            **📖 Como interpretar as notas de versão:**

            - ✅ **Check verde**: Funcionalidade implementada ou correção aplicada
            - Cada versão lista as melhorias e correções implementadas
            - As notas são organizadas por versão, da mais recente para a mais antiga

            **💡 Dica:** Consulte também a sidebar para ver versões anteriores completas.
            """)
    st.caption("✅ v194: Adição das colunas vlr_unit e vlr_total, reorganização de colunas, correções de cálculo")
elif menu_sel == "3. HISTORICO TRANSFORMAÇÃO":
    st.title("🌲 CONFERENCIA ESTOQUE v194 - HISTÓRICO TRANSFORMAÇÃO")
elif menu_sel == "4. DEBITO CONSUMO":
    st.title("🌲 CONFERENCIA ESTOQUE v194 - DÉBITO CONSUMO")
elif menu_sel == "5. Gestão: Vínculos (Admin)":
    st.title("🌲 CONFERENCIA ESTOQUE v194 - GESTÃO DE VÍNCULOS")
elif menu_sel == "6. Conferência & Auditoria":
    st.title("🌲 CONFERENCIA ESTOQUE v194 - CONFERÊNCIA & AUDITORIA")
else:
    st.title("🌲 CONFERENCIA ESTOQUE v194")

# --- 1. SALDO SISFLORA ---
if menu_sel == "1. SALDO SISFLORA":
    st.header("📄 SALDO SISFLORA")
    st.info("Este é o saldo 'estático' (Foto do estoque). Se salvar no banco, escolha a data de referência.")

    # V183: Trocado tabs por radio para nao resetar ao clicar em botoes
    op_sis = st.radio("Navegação Sisflora:", ["Ler PDF (Upload)", "Carregar do Histórico", "Gerenciar / Excluir"],
                      horizontal=True, label_visibility="collapsed", key="nav_sis_183", on_change=save_app_state)
    st.divider()

    if op_sis == "Ler PDF (Upload)":
        f = st.file_uploader("PDF Sisflora (Saldo Atual)", type="pdf", key="up_sisflora")
        if f:
            with st.spinner("Lendo PDF..."):
                st.session_state['df_sisflora'] = extrair_dados_sisflora(f)
                st.session_state['sis_source'] = 'upload'

        if 'df_sisflora' in st.session_state and not st.session_state['df_sisflora'].empty and st.session_state.get('sis_source') == 'upload':
            df_s = st.session_state['df_sisflora']

            # Filtro de Categoria (igual à seção "Carregar do Histórico")
            if 'Cat_Auto' in df_s.columns:
                categorias_unicas = ['Todos'] + sorted(df_s['Cat_Auto'].dropna().unique().tolist())
                cat_sel = st.selectbox("Filtrar por Categoria:", categorias_unicas, key="cat_sis_upload")
            else:
                cat_sel = 'Todos'

            # Aplicar filtro de categoria
            df_para_total = df_s.copy()
            if cat_sel != 'Todos' and 'Cat_Auto' in df_para_total.columns:
                df_para_total = df_para_total[df_para_total['Cat_Auto'] == cat_sel].copy()

            # Filtros de Texto Global e Coluna (igual à seção "Carregar do Histórico")
            c1, c2 = st.columns([2, 1])
            txt_search = c1.text_input("🔎 Pesquisa Global:", key="txt_sis_upload")
            cols_filter = c2.multiselect("Filtrar por Coluna(s):", df_para_total.columns, key="cols_sis_upload")

            # Aplicar busca textual (filtra LINHAS)
            if txt_search and txt_search.strip():
                try:
                    mask = df_para_total.astype(str).apply(lambda x: x.str.contains(txt_search.strip(), case=False, na=False)).any(axis=1)
                    df_para_total = df_para_total[mask].copy()
                except Exception as e:
                    st.warning(f"Erro ao aplicar pesquisa: {e}")

            # Exibe total de Volume Disponivel conforme filtros aplicados (DEPOIS dos filtros)
            # IMPORTANTE: Este cálculo acontece APÓS aplicar categoria e pesquisa global
            if 'Volume Disponivel' in df_para_total.columns:
                try:
                    # Garante que Volume Disponivel é numérico
                    df_para_total['Volume Disponivel'] = pd.to_numeric(df_para_total['Volume Disponivel'], errors='coerce').fillna(0)
                    total_volume = df_para_total['Volume Disponivel'].sum()
                    # Mostrar também quantas linhas estão sendo somadas para debug
                    st.metric("📊 Volume Total (PDF) - CONFORME FILTROS APLICADOS", formatar_br(total_volume))
                    st.caption(f"📊 Soma de {len(df_para_total)} registros (após filtros de categoria e pesquisa)")
                except Exception as e:
                    st.error(f"Erro ao calcular total: {e}")
            else:
                st.warning("Coluna 'Volume Disponivel' não encontrada no dataframe.")

            # Renderiza a tabela manualmente aplicando os mesmos filtros
            if df_para_total.empty:
                st.info("Nenhum dado para exibir após aplicar os filtros.")
            else:
                # Aplica filtro de colunas se necessário
                if cols_filter:
                    df_view = df_para_total[cols_filter].copy()
                else:
                    df_view = df_para_total.copy()

                # Formatação Visual (Datas BR, 4 Casas) - igual à seção "Carregar do Histórico"
                date_cols = []
                for col in df_view.columns:
                    if pd.api.types.is_datetime64_any_dtype(df_view[col]):
                        date_cols.append(col)
                        df_view[col] = df_view[col].dt.strftime('%d/%m/%Y')
                    elif df_view[col].astype(str).str.match(r'^\d{4}-\d{2}-\d{2}$').any():
                        try:
                            df_view[col] = pd.to_datetime(df_view[col], errors='coerce').dt.strftime('%d/%m/%Y')
                            date_cols.append(col)
                        except:
                            pass

                # Formatação numérica
                format_dict = {}
                for c in df_view.columns:
                    if pd.api.types.is_numeric_dtype(df_view[c]):
                        format_dict[c] = lambda x: formatar_br(x) if pd.notna(x) else ""
                    else:
                        format_dict[c] = str

                styler = df_view.style.format(format_dict)
                if date_cols:
                    styler.set_properties(subset=date_cols, **{'text-align': 'center'})

                st.dataframe(styler, use_container_width=True, height=500)

            # Salvar no banco (usa df_s original, não filtrado)
            with st.expander("💾 Salvar este Saldo no Banco de Dados", expanded=True):
                data_ref = st.date_input("Data de Referência deste Saldo:", value=date.today(), format="DD/MM/YYYY")
                if st.button("Confirmar Salvamento no BD"):
                    if salvar_lote_sisflora_db(df_s, data_ref, f.name if f else "Upload"):
                        st.success(f"Saldo de {data_ref.strftime('%d/%m/%Y')} salvo com sucesso!")

    elif op_sis == "Carregar do Histórico":
        datas_disp = get_datas_sisflora_disponiveis()
        if datas_disp:
            sel_data = st.selectbox("Escolha uma data salva:", datas_disp, format_func=lambda x: x.strftime("%d/%m/%Y"))
            if st.button("Carregar Saldo desta Data"):
                st.session_state['df_sisflora'] = carregar_sisflora_data_db(sel_data)
                st.session_state['sis_source'] = 'history'
                st.success(f"Carregado saldo de {sel_data.strftime('%d/%m/%Y')} para a memória!")

            if 'df_sisflora' in st.session_state and not st.session_state['df_sisflora'].empty:
                st.divider()
                st.subheader("Dados Carregados na Memória:")

                # Filtro de Categoria
                df_sis_mem = st.session_state['df_sisflora'].copy()
                if 'Cat_Auto' in df_sis_mem.columns:
                    categorias_disponiveis = sorted(df_sis_mem['Cat_Auto'].dropna().unique())
                    if categorias_disponiveis:
                        cat_selecionadas = st.multiselect(
                            "Filtrar por Categoria (Cat_Auto):",
                            options=["TODOS"] + categorias_disponiveis,
                            default=["TODOS"],
                            key="filtro_cat_sis_db"
                        )

                        # Aplica filtro de categoria
                        if cat_selecionadas:
                            # Se "TODOS" está nas seleções, mostra tudo (não filtra)
                            if "TODOS" in cat_selecionadas:
                                # Se TODOS está selecionado, não filtra (mostra todas as categorias)
                                pass  # Mantém df_sis_mem sem filtrar
                            else:
                                # Remove "TODOS" se existir e filtra pelas categorias selecionadas
                                cat_para_filtrar = [c for c in cat_selecionadas if c != "TODOS"]
                                if cat_para_filtrar:
                                    df_sis_mem = df_sis_mem[df_sis_mem['Cat_Auto'].isin(cat_para_filtrar)].copy()

                # Cria os filtros manualmente para poder calcular o total antes de renderizar
                # 1. Filtro Texto Global (mesmo que em render_filtered_table)
                c1, c2 = st.columns([2, 1])
                txt_search = c1.text_input("🔎 Pesquisa Global:", key="txt_sis_db")
                cols_filter = c2.multiselect("Filtrar por Coluna(s):", df_sis_mem.columns, key="cols_sis_db")

                # Aplica todos os filtros ao dataframe para calcular o total correto
                # IMPORTANTE: Aplicar filtros na mesma ordem que serão aplicados na tabela
                df_para_total = df_sis_mem.copy()

                # Aplica busca textual (filtra LINHAS)
                if txt_search and txt_search.strip():
                    try:
                        mask = df_para_total.astype(str).apply(lambda x: x.str.contains(txt_search.strip(), case=False, na=False)).any(axis=1)
                        df_para_total = df_para_total[mask].copy()
                    except Exception as e:
                        st.warning(f"Erro ao aplicar pesquisa: {e}")

                # Exibe total de Volume Disponivel conforme filtros aplicados (ANTES do filtro de colunas)
                # O filtro de colunas (cols_filter) apenas limita quais colunas mostrar, não filtra linhas
                if 'Volume Disponivel' in df_para_total.columns:
                    try:
                        # Garante que Volume Disponivel é numérico
                        df_para_total['Volume Disponivel'] = pd.to_numeric(df_para_total['Volume Disponivel'], errors='coerce').fillna(0)
                        total_volume = df_para_total['Volume Disponivel'].sum()
                        st.metric("📊 Total Volume Disponivel (conforme filtros)", formatar_br(total_volume))
                    except Exception as e:
                        st.error(f"Erro ao calcular total: {e}")
                else:
                    st.warning("Coluna 'Volume Disponivel' não encontrada no dataframe.")

                # Renderiza a tabela manualmente aplicando os mesmos filtros
                # (não usa render_filtered_table para evitar duplicação de controles)
                if df_para_total.empty:
                    st.info("Nenhum dado para exibir após aplicar os filtros.")
                else:
                    # Aplica filtro de colunas se necessário
                    if cols_filter:
                        df_view = df_para_total[cols_filter].copy()
                    else:
                        df_view = df_para_total.copy()

                    # Formatação Visual (Datas BR, 4 Casas)
                    date_cols = []
                    for col in df_view.columns:
                        if pd.api.types.is_datetime64_any_dtype(df_view[col]):
                            date_cols.append(col)
                            df_view[col] = df_view[col].dt.strftime('%d/%m/%Y')
                        elif df_view[col].astype(str).str.match(r'^\d{4}-\d{2}-\d{2}$').any():
                            try:
                                df_view[col] = pd.to_datetime(df_view[col]).dt.strftime('%d/%m/%Y')
                                date_cols.append(col)
                            except:
                                pass

                    # Identificar colunas que não devem receber formatação de volume
                    cols_no_fmt = [c for c in df_view.columns if any(x in c.lower() for x in ['id', 'sku', 'numero', 'nota', 'serie', 'codigo', 'ano'])]

                    def fmt_br(x):
                        if isinstance(x, (float, int)) and not isinstance(x, bool):
                            return f"{x:,.4f}".replace(",", "X").replace(".", ",").replace("X", ".")
                        return str(x)

                    def fmt_id(x):
                        return str(x)

                    format_dict = {}
                    for c in df_view.columns:
                        if c in cols_no_fmt:
                            format_dict[c] = fmt_id
                        elif pd.api.types.is_numeric_dtype(df_view[c]):
                            format_dict[c] = fmt_br
                        else:
                            format_dict[c] = str

                    styler = df_view.style.format(format_dict)

                    if date_cols:
                        styler.set_properties(subset=date_cols, **{'text-align': 'center'})

                    st.dataframe(styler, use_container_width=True, height=500)
        else:
            st.warning("Nenhum histórico salvo ainda.")

    elif op_sis == "Gerenciar / Excluir":
        st.markdown("### 🗑️ Excluir Saldo do Banco")
        datas_del = get_datas_sisflora_disponiveis()
        if datas_del:
            sel_del = st.selectbox("Selecione data para apagar:", datas_del, format_func=lambda x: x.strftime("%d/%m/%Y"), key="sel_del_sf")
            if st.button("Apagar Definitivamente", type="primary"):
                if excluir_sisflora_por_data(sel_del):
                    st.success("Apagado com sucesso!")
                    st.rerun()
        else:
            st.info("Banco vazio.")

# --- 2. SALDO PLENUS (UPDATED v183) ---
elif menu_sel == "2. SALDO PLENUS":
    st.header("📂 SALDO PLENUS")

    # V183: st.radio para persistencia
    op_ple = st.radio("Navegação Plenus:", ["Ler HTML / Importar", "Carregar do Histórico", "Gerenciar / Excluir"],
                      horizontal=True, label_visibility="collapsed", key="nav_ple_183", on_change=save_app_state)
    st.divider()

    if op_ple == "Ler HTML / Importar":
        # File Uploader
        f_plenus = st.file_uploader("Importar HTML Plenus", type=["html", "htm"], key="up_plenus")
        if f_plenus:
            with st.spinner("Processando..."):
                df, erros = extrair_dados_plenus_html(f_plenus.getvalue().decode('utf-8', errors='ignore'), f_plenus.name)

                # V187: VALIDAÇÃO - Não aceita upload se houver produtos sem TOTAL
                if erros and len(erros) > 0:
                    st.error("❌ IMPORTAÇÃO BLOQUEADA: Existem produtos sem linha TOTAL no arquivo HTML!")
                    st.warning("Por favor, corrija o arquivo HTML para incluir a linha TOTAL para todos os produtos antes de importar.")
                    with st.expander("🔍 Ver produtos sem TOTAL (corrigir no HTML):", expanded=True):
                        st.dataframe(pd.DataFrame(erros), use_container_width=True)
                    # Não salva no session_state - bloqueia importação
                else:
                    # Importação aceita - sem erros
                    st.session_state['df_plenus'] = df
                    st.session_state['lista_erro_plenus'] = []
                    st.session_state['ple_source'] = 'upload'
                    st.success("✅ Arquivo importado com sucesso! Todos os produtos possuem linha TOTAL.")

        # If data exists (only show in upload tab if source is upload)
        if 'df_plenus' in st.session_state and not st.session_state['df_plenus'].empty and st.session_state.get('ple_source') == 'upload':
            # CORREÇÃO CRÍTICA: Normalizar categoria "TOROS" para "TORAS" ANTES de renderizar
            # Isso garante que produtos sejam categorizados corretamente
            df_ple_upload = st.session_state['df_plenus'].copy()
            if 'categoria' in df_ple_upload.columns:
                df_ple_upload['categoria'] = df_ple_upload['categoria'].replace('TOROS', 'TORAS')
            if 'Cat_Auto' in df_ple_upload.columns:
                df_ple_upload['Cat_Auto'] = df_ple_upload['Cat_Auto'].replace('TOROS', 'TORAS')

            # Botão Limpar (Verde) e Alertas
            c_btn, c_rest = st.columns([1, 4])
            with c_btn:
                if st.button("Limpar Plenus", type="primary"):
                    del st.session_state['df_plenus']
                    if 'view_plenus' in st.session_state:
                        st.session_state['view_plenus'] = None
                    st.rerun()

            # REFACTOR V183: Use shared dashboard function
            # CORREÇÃO: Passar dados_do_html=True para não criar linhas TOTAL automaticamente (HTML já tem todas)
            render_plenus_dashboard(df_ple_upload, key_prefix="upload", allow_save=True, dados_do_html=True)

    elif op_ple == "Carregar do Histórico":
        # Load History
        st.markdown("### 📂 Carregar Movimentos do Banco")

        # CORREÇÃO: Buscar última data no banco e definir automaticamente o período
        # Se última data for 31/01/2025, data inicial = 01/01/2025
        # Se última data for 28/02/2025, data inicial = 01/02/2025
        ultima_data_banco = get_max_date_db('plenus_historico', 'data_movimento')

        # Definir valores padrão baseados na última data do banco
        if ultima_data_banco:
            # Data inicial = primeiro dia do mês da última data
            d_ini_padrao = date(ultima_data_banco.year, ultima_data_banco.month, 1)
            # Data final = última data encontrada no banco
            d_fim_padrao = ultima_data_banco
        else:
            # Se não encontrou data, usar valores da sessão ou hoje
            d_ini_padrao = st.session_state.get('p_dt_ini', date.today().replace(day=1))
            d_fim_padrao = st.session_state.get('p_dt_fim', date.today())

        c_h1, c_h2 = st.columns(2)
        d_ini_h = c_h1.date_input("De:", value=d_ini_padrao, key="hist_p_ini", format="DD/MM/YYYY")
        d_fim_h = c_h2.date_input("Até:", value=d_fim_padrao, key="hist_p_fim", format="DD/MM/YYYY")
        st.session_state['p_dt_ini'] = d_ini_h
        st.session_state['p_dt_fim'] = d_fim_h

        if st.button("Carregar do Histórico", key="btn_load_hist_p"):
            df_hist = carregar_plenus_movimento_db(d_ini_h, d_fim_h)

            # V192: Buscar todos os SKUs do banco para incluir produtos sem movimentação no período
            todos_skus = buscar_todos_skus_db()
            skus_com_movimento = set(df_hist['sku'].unique()) if not df_hist.empty and 'sku' in df_hist.columns else set()

            # CORREÇÃO: Verificar quais SKUs já têm linhas TOTAL/ANTERIOR no df_hist
            # (para não criar duplicatas)
            if not df_hist.empty and 'tipo_movimento' in df_hist.columns:
                tipos_especiais = df_hist['tipo_movimento'].astype(str).str.strip().str.upper().isin(['TOTAL', 'TOTAL:', 'ANTERIOR', 'ANTERIOR:'])
                skus_com_totais = set(df_hist[tipos_especiais]['sku'].unique()) if tipos_especiais.any() else set()
            else:
                skus_com_totais = set()

            # SKUs sem movimentação = todos os SKUs - SKUs com movimentação normal
            # Mas não incluir SKUs que já têm linhas TOTAL/ANTERIOR (já foram carregados do banco)
            skus_sem_movimento = set(todos_skus['sku'].unique()) - skus_com_movimento - skus_com_totais if not todos_skus.empty else set()

            # Buscar último saldo antes do período para produtos sem movimentação
            if skus_sem_movimento:
                ultimos_saldos_sem_mov = buscar_ultimo_saldo_antes_data_db(d_ini_h, list(skus_sem_movimento))

                # CORREÇÃO: Se não encontrou saldo anterior, buscar o último saldo de QUALQUER data (incluindo dentro do período)
                # Isso garante que produtos como AMAPA que só têm linha TOTAL sejam incluídos
                for sku_sem_mov in list(skus_sem_movimento):
                    if sku_sem_mov not in ultimos_saldos_sem_mov:
                        # Buscar último saldo de qualquer data para este SKU
                        conn_temp = get_db_connection()
                        try:
                            c_temp = conn_temp.cursor()
                            c_temp.execute("""
                                SELECT saldo_apos
                                FROM plenus_historico
                                WHERE sku = ?
                                ORDER BY data_movimento DESC, id DESC
                                LIMIT 1
                            """, (sku_sem_mov,))
                            row_temp = c_temp.fetchone()
                            if row_temp and row_temp[0] is not None:
                                ultimos_saldos_sem_mov[sku_sem_mov] = float(row_temp[0])
                            else:
                                ultimos_saldos_sem_mov[sku_sem_mov] = 0.0
                        finally:
                            conn_temp.close()

                # Criar linhas ANTERIOR e TOTAL para produtos sem movimentação no período
                linhas_sem_movimento = []
                for _, row_sku in todos_skus[todos_skus['sku'].isin(skus_sem_movimento)].iterrows():
                    sku_atual = row_sku['sku']
                    ultimo_saldo = ultimos_saldos_sem_mov.get(sku_atual, 0.0)

                    # CORREÇÃO: Normalizar categoria "TOROS" para "TORAS" também aqui
                    categoria_sku = row_sku.get('categoria', '')
                    if categoria_sku == 'TOROS':
                        categoria_sku = 'TORAS'

                    # Criar linha ANTERIOR (usar nomes de colunas do banco: tipo_movimento e saldo_apos)
                    linha_anterior = {
                        'sku': sku_atual,
                        'produto': row_sku.get('produto', ''),
                        'categoria': categoria_sku,  # Usar categoria normalizada
                        'data_movimento': None,
                        'tipo_movimento': 'ANTERIOR',  # Nome da coluna no banco
                        'entrada': 0.0,
                        'saida': 0.0,
                        'saldo_apos': ultimo_saldo,  # Nome da coluna no banco
                        'saldo_anterior': ultimo_saldo,
                        'nota': None,
                        'serie': None,
                        'arquivo_origem': None
                    }
                    linhas_sem_movimento.append(linha_anterior)

                    # Criar linha TOTAL (mesmo saldo, pois não houve movimentação)
                    # V192: Usar None para data_movimento para garantir que passe no filtro de data da UI
                    linha_total = {
                        'sku': sku_atual,
                        'produto': row_sku.get('produto', ''),
                        'categoria': categoria_sku,  # Usar categoria normalizada
                        'data_movimento': None,  # V192: None para garantir que passe no filtro de data
                        'tipo_movimento': 'TOTAL',  # Nome da coluna no banco
                        'entrada': 0.0,
                        'saida': 0.0,
                        'saldo_apos': ultimo_saldo,  # Nome da coluna no banco
                        'saldo_anterior': 0.0,
                        'nota': None,
                        'serie': None,
                        'arquivo_origem': None
                    }
                    linhas_sem_movimento.append(linha_total)

                # Adicionar linhas de produtos sem movimentação ao df_hist
                if linhas_sem_movimento:
                    df_sem_movimento = pd.DataFrame(linhas_sem_movimento)
                    if df_hist.empty:
                        df_hist = df_sem_movimento.copy()
                    else:
                        df_hist = pd.concat([df_hist, df_sem_movimento], ignore_index=True)

            if not df_hist.empty:
                # Adaptação de colunas para formato esperado (renomear após concatenação)
                if 'tipo_movimento' in df_hist.columns:
                    df_hist.rename(columns={'tipo_movimento': 'tipo', 'saldo_apos': 'saldo'}, inplace=True)

                # Criar coluna 'data' formatada (importante para linhas ANTERIOR que têm data_movimento None)
                if 'data' not in df_hist.columns:
                    if 'data_movimento' in df_hist.columns:
                        def formatar_data(x):
                            if pd.isna(x) or x is None:
                                return None
                            try:
                                dt = pd.to_datetime(x, errors='coerce')
                                if pd.notna(dt):
                                    return dt.strftime("%d/%m/%Y")
                            except:
                                pass
                            return None
                        df_hist['data'] = df_hist['data_movimento'].apply(formatar_data)
                    else:
                        df_hist['data'] = None

                # V190: Se o tipo for 'TOTAL' ou 'TOTAL:', manter como está (linhas TOTAL explícitas salvas)
                # Isso permite que render_plenus_dashboard identifique e use essas linhas no cálculo
                # Campos extras
                if 'categoria' not in df_hist.columns:
                    df_hist['categoria'] = ""
                df_hist["Item_Completo"] = df_hist["produto"].astype(str) + " (" + df_hist["categoria"].fillna("").astype(str) + ")"
                df_hist["Cat_Auto"] = df_hist["Item_Completo"].apply(detectar_categoria_plenus)

                # CORREÇÃO: Normalizar categoria "TOROS" para "TORAS" (compatibilidade)
                # Isso garante que produtos com categoria "TOROS" no banco sejam tratados como "TORAS"
                if 'categoria' in df_hist.columns:
                    df_hist['categoria'] = df_hist['categoria'].replace('TOROS', 'TORAS')
                # Também normalizar Cat_Auto se necessário
                if 'Cat_Auto' in df_hist.columns:
                    df_hist['Cat_Auto'] = df_hist['Cat_Auto'].replace('TOROS', 'TORAS')

                # LOG: Salvar log de carregamento do histórico com SKUs e saldos (M3)
                import json
                import os
                if not df_hist.empty and 'sku' in df_hist.columns and 'saldo' in df_hist.columns:
                    log_carregamento = {
                        "timestamp": datetime.now().isoformat(),
                        "fonte": "HISTORICO_CARREGADO",
                        "periodo": {
                            "data_inicio": d_ini_h.strftime("%Y-%m-%d"),
                            "data_fim": d_fim_h.strftime("%Y-%m-%d")
                        },
                        "total_skus": len(df_hist['sku'].dropna().unique()),
                        "skus_detalhado": {}
                    }

                    # Agrupar por SKU e pegar o saldo da linha TOTAL ou último saldo
                    for sku in df_hist['sku'].dropna().unique():
                        df_sku = df_hist[df_hist['sku'] == sku].copy()
                        if not df_sku.empty:
                            # Tentar pegar saldo da linha TOTAL
                            if 'tipo' in df_sku.columns:
                                mask_total = df_sku['tipo'].astype(str).str.strip().str.upper().isin(['TOTAL', 'TOTAL:'])
                                if mask_total.any():
                                    saldo_valor = float(df_sku[mask_total].iloc[0]['saldo'])
                                else:
                                    # Pegar último saldo
                                    if 'data_movimento' in df_sku.columns:
                                        df_sku['data_dt'] = pd.to_datetime(df_sku['data_movimento'], errors='coerce')
                                        df_sku = df_sku.sort_values(by='data_dt', ascending=False)
                                    saldo_valor = float(df_sku.iloc[0]['saldo'])
                            else:
                                # Se não tem tipo, pegar último saldo
                                if 'data_movimento' in df_sku.columns:
                                    df_sku['data_dt'] = pd.to_datetime(df_sku['data_movimento'], errors='coerce')
                                    df_sku = df_sku.sort_values(by='data_dt', ascending=False)
                                saldo_valor = float(df_sku.iloc[0]['saldo'])

                            produto = df_sku.iloc[0].get('produto', '')
                            categoria = df_sku.iloc[0].get('categoria', '')
                            cat_auto = df_sku.iloc[0].get('Cat_Auto', '')

                            log_carregamento["skus_detalhado"][str(sku)] = {
                                "sku": str(sku),
                                "produto": str(produto),
                                "categoria": str(categoria),
                                "cat_auto": str(cat_auto),
                                "saldo_m3": float(saldo_valor),
                                "saldo_m3_formatado": formatar_br(saldo_valor)
                            }

                    # Salvar log
                    os.makedirs("logs", exist_ok=True)
                    log_file = os.path.join("logs", f"plenus_load_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
                    with open(log_file, 'w', encoding='utf-8') as f:
                        json.dump(log_carregamento, f, indent=2, ensure_ascii=False)

                # NOVA FUNCIONALIDADE: Buscar último saldo antes da data inicial para cada SKU
                # Isso permite que o saldo_anterior seja calculado corretamente quando consultar períodos específicos
                # Exemplo: se último saldo até 28/01/2025 é 1.607,796 e consulta período 01/02/2025-28/02/2025,
                # o saldo_anterior para a primeira movimentação de fev será 1.607,796
                skus_no_periodo = df_hist['sku'].unique().tolist() if 'sku' in df_hist.columns else []
                ultimos_saldos_antes = buscar_ultimo_saldo_antes_data_db(d_ini_h, skus_no_periodo) if skus_no_periodo else {}
                # Garantir que é um dicionário (não None)
                if ultimos_saldos_antes is None:
                    ultimos_saldos_antes = {}

                # Garantir que saldo_anterior existe
                if 'saldo_anterior' not in df_hist.columns:
                    df_hist['saldo_anterior'] = 0.0
                else:
                    df_hist['saldo_anterior'] = df_hist['saldo_anterior'].fillna(0.0)

                # Atualizar saldo_anterior com os valores corretos do último saldo antes da data inicial
                # Aplica apenas para a primeira movimentação de cada SKU no período (ordenado por data)
                if ultimos_saldos_antes and 'sku' in df_hist.columns and 'data_movimento' in df_hist.columns:
                    # Ordenar por SKU e data para identificar primeira movimentação de cada SKU
                    df_hist['data_mov_dt'] = pd.to_datetime(df_hist['data_movimento'], errors='coerce')
                    df_hist = df_hist.sort_values(by=['sku', 'data_mov_dt', 'id'], ascending=True, na_position='last')
                    df_hist = df_hist.reset_index(drop=True)

                    # Para cada SKU que tem último saldo antes da data inicial, atualizar o saldo_anterior
                    # IMPORTANTE: Propagá-lo para TODAS as linhas do SKU (como no processamento.py)
                    for sku, ultimo_saldo in ultimos_saldos_antes.items():
                        mask_sku = (df_hist['sku'] == sku)
                        if mask_sku.any():
                            # Atualizar saldo_anterior em TODAS as linhas deste SKU
                            df_hist.loc[mask_sku, 'saldo_anterior'] = ultimo_saldo

                    df_hist = df_hist.drop(columns=['data_mov_dt'])

                # V192: Propagar saldo_anterior para TODAS as linhas de cada SKU
                # Se o SKU tem linha ANTERIOR, usar o saldo dela como saldo_anterior para todas as linhas
                # Se não tem linha ANTERIOR mas tem último saldo antes, usar esse valor
                if 'sku' in df_hist.columns and 'saldo_anterior' in df_hist.columns:
                    for sku in df_hist['sku'].dropna().unique():
                        mask_sku = (df_hist['sku'] == sku)
                        if mask_sku.any():
                            # Primeiro, tentar pegar o saldo_anterior da linha ANTERIOR se existir
                            mask_anterior = mask_sku & (df_hist['tipo'].astype(str).str.strip().str.upper().isin(['ANTERIOR', 'ANTERIOR:']))
                            if mask_anterior.any() and 'saldo' in df_hist.columns:
                                saldo_anterior_linha = df_hist.loc[mask_anterior, 'saldo'].iloc[0]
                                if pd.notna(saldo_anterior_linha):
                                    # Usar o saldo da linha ANTERIOR como saldo_anterior para todas as linhas do SKU
                                    df_hist.loc[mask_sku, 'saldo_anterior'] = saldo_anterior_linha
                            else:
                                # Se não tem linha ANTERIOR, verificar se tem último saldo antes
                                if sku in ultimos_saldos_antes:
                                    df_hist.loc[mask_sku, 'saldo_anterior'] = ultimos_saldos_antes[sku]
                else:
                    # Se não encontrou últimos saldos, preenche com 0
                    df_hist['saldo_anterior'] = df_hist['saldo_anterior'].fillna(0.0)

                st.session_state['df_plenus'] = df_hist
                st.session_state['lista_erro_plenus'] = []
                st.session_state['ple_source'] = 'history'
                st.success(f"Carregado {len(df_hist)} registros do banco.")
                st.rerun()
            else:
                st.warning("Nenhum dado encontrado neste período.")

        # V187: Show loaded data in history tab with filters (similar to SISFLORA)
        if 'df_plenus' in st.session_state and not st.session_state['df_plenus'].empty and st.session_state.get('ple_source') == 'history':
            st.divider()
            st.subheader("Dados Carregados do Histórico")

            # Filtro de Categoria (v189)
            df_ple_mem = st.session_state['df_plenus'].copy()

            # CORREÇÃO CRÍTICA: Garantir que categoria "TOROS" seja normalizada para "TORAS" ANTES do filtro
            # Isso garante que produtos como AMAPA sejam incluídos corretamente
            if 'categoria' in df_ple_mem.columns:
                df_ple_mem['categoria'] = df_ple_mem['categoria'].replace('TOROS', 'TORAS')
            if 'Cat_Auto' in df_ple_mem.columns:
                df_ple_mem['Cat_Auto'] = df_ple_mem['Cat_Auto'].replace('TOROS', 'TORAS')

            # CORREÇÃO: Remover filtro de categoria aqui - será aplicado dentro de render_plenus_dashboard
            # Isso evita duplicação de filtros e garante que o cálculo do total use o df_view filtrado corretamente
            # O filtro de categoria será aplicado em render_plenus_dashboard ANTES do cálculo do total

            # Renderiza dashboard (os filtros estão dentro de render_plenus_dashboard)
            render_plenus_dashboard(df_ple_mem, key_prefix="history", allow_save=False)

    elif op_ple == "Gerenciar / Excluir":
        # Delete
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
        tab_query, tab_import, tab_manage = st.tabs(["📊 Consultar Banco (Ativo)", "📥 Importar Excel", "🗑️ Limpar Período"])
    else:
        tab_import, tab_query, tab_manage = st.tabs(["📥 Importar Excel", "📊 Consultar Banco", "🗑️ Limpar Período"])

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
                            st.error(f"Erro ao processar arquivo {file.name}: {e}")
                if all_dfs:
                    final_df = pd.concat(all_dfs, ignore_index=True)

                    # Auto Update Dates
                    df_datas = pd.to_datetime(final_df['data_realizacao'], errors='coerce').dropna()
                    if not df_datas.empty:
                        min_d, max_d = df_datas.min().date(), df_datas.max().date()
                        update_session_dates('t', min_d, max_d)
                        st.info(f"📅 Datas ajustadas para: {min_d.strftime('%d/%m/%Y')} a {max_d.strftime('%d/%m/%Y')}")

                    st.session_state['st_df_transf_preview'] = final_df
                    st.session_state['last_files_transf'] = current_file_names
                    st.success(f"Processado! {len(final_df)} linhas.")

        if st.session_state['st_df_transf_preview'] is not None:
            def color_transf(val):
                if val == "PRODUTO DE ORIGEM":
                    return "color: red; font-weight: bold"
                if val == "PRODUTO GERADO":
                    return "color: green; font-weight: bold"
                return ""

            df_prev = st.session_state['st_df_transf_preview'].copy()
            render_filtered_table(df_prev, "transf_preview")

            if st.button("💾 Salvar Transformações no Banco", key="btn_save_transf"):
                ins, ext = salvar_lote_smart('transf_historico', 'data_realizacao', st.session_state['st_df_transf_preview'])
                if ins > 0:
                    st.success(f"✅ {ins} registros salvos.")
                if ext > 0:
                    st.warning(f"⚠️ {ext} registros ignorados (já existiam).")
                st.session_state['st_df_transf_preview'] = None
                st.session_state['last_files_transf'] = None
                st.rerun()

    with tab_query:
        c_dt1, c_dt2 = st.columns(2)
        dt_ini = c_dt1.date_input("De:", value=st.session_state['t_dt_ini'], key="t_dt_ini_w", format="DD/MM/YYYY", on_change=save_app_state)
        dt_fim = c_dt2.date_input("Até:", value=st.session_state['t_dt_fim'], key="t_dt_fim_w", format="DD/MM/YYYY", on_change=save_app_state)
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
        tab_c_view, tab_c_import, tab_c_del = st.tabs(["📊 Consultar Banco (Ativo)", "📥 Importar Excel (Multi)", "🗑️ Limpar Período"])
    else:
        tab_c_import, tab_c_view, tab_c_del = st.tabs(["📥 Importar Excel (Multi)", "📊 Consultar Banco", "🗑️ Limpar Período"])

    with tab_c_import:
        uploaded_files = st.file_uploader("Carregue arquivos Excel", type=["xlsx", "csv"], accept_multiple_files=True, key="up_consumo")

        if uploaded_files:
            try:
                all_dataframes = []
                with st.spinner("Lendo e consolidando arquivos..."):
                    for file in uploaded_files:
                        df_temp = load_data_consumo_excel(file)
                        df_temp['_arquivo_origem_temp'] = file.name
                        all_dataframes.append(df_temp)

                if all_dataframes:
                    df_loaded = pd.concat(all_dataframes, ignore_index=True)
                    if 'Data' in df_loaded.columns:
                        df_loaded = df_loaded.sort_values(by='Data')
                        # Auto Update Dates
                        df_datas = pd.to_datetime(df_loaded['Data'], errors='coerce').dropna()
                        if not df_datas.empty:
                            min_d, max_d = df_datas.min().date(), df_datas.max().date()
                            update_session_dates('c', min_d, max_d)
                            st.info(f"📅 Datas ajustadas: {min_d.strftime('%d/%m/%Y')} a {max_d.strftime('%d/%m/%Y')}")

                    st.markdown("### Pré-visualização")
                    render_filtered_table(df_loaded, "cons_preview")

                    if st.button("💾 CONFIRMAR: Salvar no Banco", key="btn_save_consumo"):
                        # Mapeamento
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
                        if 'Data' in df_json_prep.columns:
                            df_json_prep['Data'] = df_json_prep['Data'].astype(str)
                        df_to_save['dados_json'] = df_json_prep.apply(lambda x: json.dumps(x.to_dict(), default=str), axis=1)

                        ins, ext = salvar_lote_smart('consumo_historico', 'data_consumo', df_to_save)
                        if ins > 0:
                            st.success(f"✅ {ins} salvos.")
                        if ext > 0:
                            st.warning(f"⚠️ {ext} já existiam.")
                        time.sleep(1)
                        st.rerun()
            except Exception as e:
                st.error(f"Erro: {e}")

    with tab_c_view:
        d_ini_c = st.date_input("De:", value=st.session_state['c_dt_ini'], key="c_dt_ini_w", format="DD/MM/YYYY", on_change=save_app_state)
        d_fim_c = st.date_input("Até:", value=st.session_state['c_dt_fim'], key="c_dt_fim_w", format="DD/MM/YYYY", on_change=save_app_state)
        st.session_state['c_dt_ini'] = d_ini_c
        st.session_state['c_dt_fim'] = d_fim_c

        if st.button("Consultar Consumo no Banco", key="btn_search_consumo"):
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
        # Ensure compatibility with DB
        if 'Item_Completo' not in st.session_state['df_plenus'].columns:
            # FIX v181: Match old logic "Produto (Categoria)"
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

    # ... (MANTIDO ADMIN ORIGINAL) ...
    if admin_mode == "Agrupar Sisflora":
        if 'df_sisflora' in st.session_state:
            c1, c2 = st.columns([2, 1])
            cat_sel = c1.selectbox("Categoria:", [""] + sorted(st.session_state['df_sisflora']['Cat_Auto'].unique()), key="s_cat_adm")
            txt_sel = c2.text_input("Pesquisar Nome:", key="s_txt_adm")
            lista_agrupados = list(st.session_state['agrup_sis'].keys())
            mask_pend = ~st.session_state['df_sisflora']['Item_Completo'].isin(lista_agrupados)
            mask_cesta = ~st.session_state['df_sisflora']['Item_Completo'].isin(st.session_state['cesta_sis'])
            df_pend = st.session_state['df_sisflora'][mask_pend & mask_cesta].copy()
            if cat_sel:
                df_pend = df_pend[df_pend['Cat_Auto'] == cat_sel]
            if txt_sel:
                df_pend = df_pend[df_pend['Item_Completo'].str.contains(txt_sel, case=False, na=False)]
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
                # FIX v181
                st.session_state['df_plenus']['Item_Completo'] = st.session_state['df_plenus']["produto"] + " (" + st.session_state['df_plenus']["categoria"].fillna("") + ")"
            c1, c2 = st.columns([2, 1])
            cats_p = sorted(st.session_state['df_plenus']['categoria'].fillna("").unique()) if 'categoria' in st.session_state['df_plenus'].columns else []
            cat_sel_p = c1.selectbox("Categoria:", [""] + cats_p, key="p_cat_adm")
            txt_sel_p = c2.text_input("Pesquisar Nome:", key="p_txt_adm")
            lista_agrupados_p = list(st.session_state['agrup_ple'].keys())
            mask_pend_p = ~st.session_state['df_plenus']['Item_Completo'].isin(lista_agrupados_p)
            mask_cesta_p = ~st.session_state['df_plenus']['Item_Completo'].isin(st.session_state['cesta_ple'])
            df_pend_p = st.session_state['df_plenus'][mask_pend_p & mask_cesta_p].copy()
            if cat_sel_p:
                df_pend_p = df_pend_p[df_pend_p['categoria'] == cat_sel_p]
            if txt_sel_p:
                df_pend_p = df_pend_p[df_pend_p['Item_Completo'].str.contains(txt_sel_p, case=False, na=False)]
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
        with st.expander("🤖 Sugestões Inteligentes de Vínculo", expanded=True):
            f_cat_ia = st.selectbox("Filtrar por Categoria:", ["TODAS", "TORAS", "SERRADAS", "BENEFICIADAS"], key="sel_cat_ia")
            if st.button("🔎 Buscar Sugestões"):
                with st.spinner(f"Analisando essência dos nomes ({f_cat_ia})..."):
                    mapa_cat_sis = get_categorias_dos_grupos("SISFLORA")
                    mapa_cat_ple = get_categorias_dos_grupos("PLENUS")
                    pendentes_ple = grps_ple
                    sugestoes = []
                    for gp in pendentes_ple:
                        status_vinc = ""
                        is_vinculado = gp in vinculos_atuais
                        if is_vinculado:
                            status_vinc = f"✅ JÁ VINCULADO A: {vinculos_atuais[gp]}"
                        cat_p = mapa_cat_ple.get(gp, "OUTROS")
                        if f_cat_ia != "TODAS":
                            cat_normal = cat_p
                            if cat_p in ["TORAS", "TOROS", "TORA"]:
                                cat_normal = "TORAS"
                            elif "SERRADA" in cat_p:
                                cat_normal = "SERRADAS"
                            elif "BENEF" in cat_p:
                                cat_normal = "BENEFICIADAS"
                            if cat_normal != f_cat_ia:
                                continue
                        if cat_p.endswith('S') and cat_p != "TORAS" and cat_p != "OUTROS":
                            cat_p_norm = cat_p[:-1]
                        else:
                            cat_p_norm = cat_p
                        melhor_match = None
                        maior_score = 0.0
                        for gs in grps_sis:
                            cat_s = mapa_cat_sis.get(gs, "OUTROS")
                            if cat_s.endswith('S') and cat_s != "TORAS" and cat_s != "OUTROS":
                                cat_s_norm = cat_s[:-1]
                            else:
                                cat_s_norm = cat_s
                            compativel = False
                            if cat_p == cat_s:
                                compativel = True
                            elif cat_p in ["TORAS", "TOROS", "TORA"] and cat_s in ["TORAS", "TOROS", "TORA"]:
                                compativel = True
                            elif cat_p_norm == cat_s_norm and cat_p != "OUTROS":
                                compativel = True
                            if compativel:
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
                            "Aceitar": st.column_config.CheckboxColumn("Vincular?", help="Marque para confirmar", default=False, disabled="is_locked"),
                            "Status": st.column_config.TextColumn("Status", help="Indica se já está vinculado", width="medium")
                        },
                        disabled=["Plenus", "Sisflora (Sugerido)", "Categoria", "Confiança", "Status", "is_locked"],
                        hide_index=True, use_container_width=True
                    )
                    if st.button("✅ Confirmar Vínculos Selecionados"):
                        vincular_agora = edited_df[edited_df["Aceitar"] == True]
                        if not vincular_agora.empty:
                            count_ok = 0
                            conn = get_db_connection()
                            c = conn.cursor()
                            try:
                                for index, row in vincular_agora.iterrows():
                                    c.execute("INSERT OR REPLACE INTO vinculos (grupo_plenus, grupo_sisflora) VALUES (?, ?)", (row['Plenus'], row['Sisflora (Sugerido)']))
                                    count_ok += 1
                                conn.commit()
                                st.success(f"{count_ok} vínculos criados com sucesso!")
                                st.session_state['vinculos'] = carregar_vinculos_db()
                                mask_restantes = edited_df["Aceitar"] == False
                                restantes = edited_df[mask_restantes].to_dict('records')
                                st.session_state['sugestoes_ia'] = restantes
                                st.rerun()
                            except Exception as e:
                                st.error(f"Erro ao salvar: {e}")
                            finally:
                                conn.close()
                        else:
                            st.warning("Nenhum item marcado.")
                else:
                    st.info(f"Nenhuma sugestão encontrada para o filtro {f_cat_ia}.")

    elif admin_mode == "Vínculo Manual":
        grps_sis = carregar_lista_grupos_db("SISFLORA")
        grps_ple = carregar_lista_grupos_db("PLENUS")
        vinculos_atuais = carregar_vinculos_db()
        txt_manual = st.text_input("🔎 Pesquisar Nome (nos dois lados):", key="search_manual_both")
        col_filtro, col_check = st.columns([2, 1])
        cat_filtro = col_filtro.selectbox("Filtrar Categoria:", ["TODAS", "TORAS", "SERRADAS", "BENEFICIADAS", "OUTROS"], key="sel_cat_manual")
        ocultar_vinc = col_check.checkbox("Ocultar vinculados", value=True)
        lista_sis = sorted(grps_sis)
        lista_ple = sorted(grps_ple)
        pais_com_vinculo = set(vinculos_atuais.values())
        lista_sis_visual = []
        for g in lista_sis:
            if txt_manual and txt_manual.upper() not in g.upper():
                continue
            mark = "✅ " if g in pais_com_vinculo else ""
            lista_sis_visual.append(f"{mark}{g}")
        lista_ple_visual = []
        for g in lista_ple:
            if txt_manual and txt_manual.upper() not in g.upper():
                continue
            mark = "✅ " if g in vinculos_atuais else ""
            if ocultar_vinc and mark:
                continue
            lista_ple_visual.append(f"{mark}{g}")
        if cat_filtro != "TODAS":
            mapa_cat_s = get_categorias_dos_grupos("SISFLORA")
            mapa_cat_p = get_categorias_dos_grupos("PLENUS")
            def normalizar_cat(c):
                if c in ["TORAS", "TOROS", "TORA"]:
                    return "TORAS"
                if "SERRADA" in c:
                    return "SERRADAS"
                if "BENEF" in c:
                    return "BENEFICIADAS"
                return "OUTROS"
            lista_sis_visual = [g for g in lista_sis_visual if normalizar_cat(mapa_cat_s.get(g.replace("✅ ", ""), "OUTROS")) == cat_filtro]
            lista_ple_visual = [g for g in lista_ple_visual if normalizar_cat(mapa_cat_p.get(g.replace("✅ ", ""), "OUTROS")) == cat_filtro]
        c_esq, c_dir = st.columns(2)
        selected_sis = None
        selected_ple = None
        with c_esq:
            st.markdown("**1. Sisflora**")
            df_s_view = pd.DataFrame(lista_sis_visual, columns=["Grupo Sisflora"])
            event_s = st.dataframe(df_s_view, use_container_width=True, height=400, on_select="rerun", selection_mode="single-row", hide_index=True, key="grid_sis_manual")
            if event_s.selection.rows:
                raw_sis = df_s_view.iloc[event_s.selection.rows[0]]['Grupo Sisflora']
                selected_sis = raw_sis.replace("✅ ", "")
        with c_dir:
            st.markdown("**2. Plenus**")
            df_p_view = pd.DataFrame(lista_ple_visual, columns=["Grupo Plenus"])
            event_p = st.dataframe(df_p_view, use_container_width=True, height=400, on_select="rerun", selection_mode="single-row", hide_index=True, key="grid_ple_manual")
            if event_p.selection.rows:
                raw_ple = df_p_view.iloc[event_p.selection.rows[0]]['Grupo Plenus']
                selected_ple = raw_ple.replace("✅ ", "")
        st.divider()
        if selected_sis and selected_ple:
            if st.button(f"🔗 Vincular: {selected_ple}  ->  {selected_sis}", type="primary", use_container_width=True):
                salvar_vinculo_db([selected_ple], selected_sis)
                st.session_state['vinculos'] = carregar_vinculos_db()
                st.rerun()
        else:
            st.button("Selecione um item de cada lado para vincular", disabled=True, use_container_width=True)
        st.divider()
        with st.expander("Ver/Remover Vínculos Existentes"):
            df_vinc = pd.DataFrame(list(st.session_state['vinculos'].items()), columns=['Plenus', 'Sisflora'])
            f_edit = st.text_input("Filtrar tabela:", key="filtro_edit_vinc")
            if f_edit:
                df_vinc = df_vinc[df_vinc['Plenus'].str.contains(f_edit, case=False) | df_vinc['Sisflora'].str.contains(f_edit, case=False)]
            c_e1, c_e2 = st.columns([3, 1])
            with c_e1:
                st.dataframe(df_vinc, use_container_width=True)
            with c_e2:
                to_delete = st.selectbox("Remover:", [""] + sorted(df_vinc['Plenus'].unique()), key="sel_del_vinc")
                if st.button("🗑️ Remover", key="btn_del_vinc"):
                    if to_delete:
                        excluir_vinculo_db(to_delete)
                        st.session_state['vinculos'] = carregar_vinculos_db()
                        st.rerun()

    elif admin_mode == "Gerenciar Grupos":
        ger_mode = st.radio("Opção:", ["🗑️ Excluir/Editar", "📊 Relatório Geral", "🛡️ Segurança & Backup"], horizontal=True)

        if ger_mode == "🛡️ Segurança & Backup":
            st.markdown("### 🛡️ Backup de Segurança")
            st.info("Aqui você pode criar uma cópia de segurança do banco de dados atual.")

            if st.button("💾 Criar Backup Agora"):
                try:
                    if not os.path.exists("backups"):
                        os.makedirs("backups")

                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    backup_name = f"backups/backup_dados_{timestamp}.db"

                    # Copia o arquivo do banco
                    from database import ARQUIVO_DB as DB_FILE
                    shutil.copy2(DB_FILE, backup_name)
                    st.success(f"✅ Backup criado com sucesso: `{backup_name}`")

                    # Lista backups existentes
                    backups = sorted([f for f in os.listdir("backups") if f.endswith(".db")], reverse=True)
                    st.write("Backups recentes:", backups[:5])

                except Exception as e:
                    st.error(f"Erro ao criar backup: {e}")

        elif ger_mode == "🗑️ Excluir/Editar":
            tipo_ger = st.radio("Origem:", ["Sisflora", "Plenus"], horizontal=True, key="rad_ger")
            origem_db = "SISFLORA" if tipo_ger == "Sisflora" else "PLENUS"
            mapa_atual = carregar_agrupamentos_db(origem_db)
            grupos_unicos = sorted(list(set(mapa_atual.values())))
            grp_del = st.selectbox("Selecione o Grupo para EXCLUIR:", [""] + grupos_unicos, key="sel_grp_del")
            if grp_del:
                itens_no_grupo = [k for k, v in mapa_atual.items() if v == grp_del]
                st.dataframe(pd.DataFrame(itens_no_grupo, columns=["Itens"]), use_container_width=True)
                def deletar_grupo_click():
                    excluir_grupo_db(grp_del, origem_db)
                    if tipo_ger == "Sisflora":
                        st.session_state['agrup_sis'] = carregar_agrupamentos_db("SISFLORA")
                    else:
                        st.session_state['agrup_ple'] = carregar_agrupamentos_db("PLENUS")
                st.button("🗑️ CONFIRMAR EXCLUSÃO", type="primary", key="btn_conf_del", on_click=deletar_grupo_click)
        else:
            st.markdown("### 📊 Relatório Visual de Agrupamentos")
            df_rel = carregar_todos_agrupamentos_db()
            if not df_rel.empty:
                c1, c2 = st.columns(2)
                f_cat = c1.multiselect("Filtrar Categoria:", sorted(df_rel['categoria'].unique()))
                f_orig = c2.multiselect("Filtrar Origem:", sorted(df_rel['origem'].unique()))
                if f_cat:
                    df_rel = df_rel[df_rel['categoria'].isin(f_cat)]
                if f_orig:
                    df_rel = df_rel[df_rel['origem'].isin(f_orig)]
                excel_bytes = to_excel_autoajustado(df_rel)
                st.download_button("📥 Baixar Relatório (Excel .xlsx)", excel_bytes, "relatorio_grupos.xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
                grupos = sorted(df_rel['nome_grupo'].unique())
                for g in grupos:
                    itens = df_rel[df_rel['nome_grupo'] == g]
                    orig = itens.iloc[0]['origem']
                    cat = itens.iloc[0]['categoria']
                    cor = "#007bff" if orig == "SISFLORA" else "#ffc107"
                    txt = "white" if orig == "SISFLORA" else "black"
                    st.markdown(f"<div class='grupo-title' style='background-color:{cor}; color:{txt};'>{g} <span style='opacity:0.7; font-size:0.8em; float:right'>({orig} - {cat})</span></div>", unsafe_allow_html=True)
                    st.dataframe(itens[['item_original']], use_container_width=True, hide_index=True)
            else:
                st.info("Nenhum agrupamento salvo ainda.")

# --- 6. CONFERÊNCIA & AUDITORIA ---
elif menu_sel == "6. Conferência & Auditoria":
    st.header("⚖️ Resultado Final")

    tab_conf_saldo, tab_conf_auditoria = st.tabs(["COMPARAÇÃO SALDO SISFLORA E SALDO PLENUS", "AUDITORIA DE FLUXO"])

    # --- ABA: SALDO ESTÁTICO ---
    with tab_conf_saldo:
        if 'df_sisflora' in st.session_state:
            st.success("✅ Saldo Sisflora Carregado")
        else:
            st.error("❌ Saldo Sisflora Pendente (Vá em '1. Importar Sisflora')")

        if 'df_plenus' in st.session_state:
            st.success("✅ Saldo Plenus Carregado")
        else:
            st.warning("⚠️ Saldo Plenus Pendente (Vá em '2. Importar Plenus')")

        if 'df_sisflora' in st.session_state and 'df_plenus' in st.session_state:
            c_conf_1, c_conf_2 = st.columns([2, 2])
            conf_filter_cat = c_conf_1.radio("Filtrar Categoria:", ["TODAS", "TORAS", "SERRADAS", "BENEFICIADAS"], horizontal=True, key="rad_conf_cat")
            conf_search = c_conf_2.text_input("🔎 Pesquisar Grupo:")

            df_s = st.session_state['df_sisflora'].copy()
            df_s['Grupo'] = df_s['Item_Completo'].map(st.session_state['agrup_sis'])
            df_s = df_s.dropna(subset=['Grupo'])
            res_s = df_s.groupby('Grupo')['Volume Disponivel'].sum().reset_index()

            df_p = st.session_state['df_plenus'].copy()
            if 'Item_Completo' not in df_p.columns:
                # FIX v181: Match old logic "Produto (Categoria)"
                df_p['Item_Completo'] = df_p["produto"] + " (" + df_p["categoria"].fillna("") + ")"

            if 'data_movimento' in df_p.columns:
                # Add check for 'id' to avoid crash if loading from HTML (which has no id)
                cols_sort = ['data_movimento']
                if 'id' in df_p.columns:
                    cols_sort.append('id')
                df_p = df_p.sort_values(by=cols_sort, ascending=[False, False] if 'id' in df_p.columns else [False])

            df_p_last = df_p.drop_duplicates(subset=['sku'], keep='first').copy()
            df_p_last['Grupo_Inter'] = df_p_last['Item_Completo'].map(st.session_state['agrup_ple'])
            vinculos = st.session_state['vinculos']
            df_p_last['Grupo_Calc'] = df_p_last['Grupo_Inter'].map(vinculos)
            df_p_last['Grupo_Calc'] = df_p_last['Grupo_Calc'].fillna(df_p_last['Grupo_Inter'])
            df_p_last = df_p_last.dropna(subset=['Grupo_Calc'])

            col_saldo = 'saldo_apos' if 'saldo_apos' in df_p_last.columns else 'saldo'
            if col_saldo in df_p_last.columns:
                df_p_last[col_saldo] = pd.to_numeric(df_p_last[col_saldo], errors='coerce').fillna(0)

            res_p = df_p_last.groupby('Grupo_Calc')[col_saldo].sum().reset_index()

            res_s.columns = ['Grupo', 'Vol_Sis']
            res_p.columns = ['Grupo', 'Vol_Ple']

            df_final = pd.merge(res_s, res_p, on='Grupo', how='outer').fillna(0)
            df_final['Diferenca'] = df_final['Vol_Sis'] - df_final['Vol_Ple']

            if conf_search:
                df_final = df_final[df_final['Grupo'].str.contains(conf_search, case=False, na=False)]

            def highlight_diff(val):
                color = 'green' if abs(val) < 0.01 else ('red' if val < 0 else 'blue')
                return f'color: {color}; font-weight: bold'

            def fmt_br_val(x):
                return f"{x:,.4f}".replace(",", "X").replace(".", ",").replace("X", ".")

            st.dataframe(
                df_final.style.format(fmt_br_val, subset=['Vol_Sis', 'Vol_Ple', 'Diferenca'])
                .map(highlight_diff, subset=['Diferenca']),
                use_container_width=True, height=600
            )

    # --- ABA: AUDITORIA DE FLUXO ---
    with tab_conf_auditoria:
        st.markdown("##### 📅 Defina o Período de Análise")
        st.caption("O sistema irá buscar no banco de dados todas as movimentações dentro destas datas.")
        c_dt1, c_dt2 = st.columns(2)
        dt_ini_aud = c_dt1.date_input("Data Início:", value=st.session_state['aud_dt_ini'], key="aud_i", format="DD/MM/YYYY", on_change=save_app_state)
        dt_fim_aud = c_dt2.date_input("Data Fim:", value=st.session_state['aud_dt_fim'], key="aud_f", format="DD/MM/YYYY", on_change=save_app_state)
        st.session_state['aud_dt_ini'] = dt_ini_aud
        st.session_state['aud_dt_fim'] = dt_fim_aud

        if st.button("🚀 Processar Auditoria"):
            with st.spinner("Cruzando dados do Sistransf, SisConsumo e Plenus..."):
                df_transf = carregar_transf_filtrado_db(dt_ini_aud, dt_fim_aud)
                df_consumo = carregar_consumo_filtrado_db(dt_ini_aud, dt_fim_aud)

                # FIX V175
                cols_padrao = ['produto', 'essencia', 'volume']
                for c in cols_padrao:
                    if c not in df_consumo.columns:
                        df_consumo[c] = "" if c != 'volume' else 0.0

                df_plenus_mov = carregar_plenus_movimento_db(dt_ini_aud, dt_fim_aud)

                saldo_aud_sis = {}
                agrup_sis = st.session_state['agrup_sis']
                vinculos = st.session_state['vinculos']

                if not df_transf.empty:
                    gerados = df_transf[df_transf['tipo_produto'] == 'PRODUTO GERADO'].copy()
                    if not gerados.empty:
                        gerados['Item_Check'] = gerados.apply(lambda x: f"{x['produto']} - {x['essencia']}" if x['essencia'] else x['produto'], axis=1)
                        gerados['Grupo'] = gerados['Item_Check'].map(agrup_sis)
                        gerados['Grupo'] = gerados['Grupo'].fillna(gerados['Item_Check'])

                        for _, r in gerados.iterrows():
                            if pd.notnull(r['Grupo']):
                                if r['Grupo'] not in saldo_aud_sis:
                                    saldo_aud_sis[r['Grupo']] = {'Entrada': 0, 'Saida': 0}
                                saldo_aud_sis[r['Grupo']]['Entrada'] += r['volume']

                    origens = df_transf[df_transf['tipo_produto'] == 'PRODUTO DE ORIGEM'].copy()
                    if not origens.empty:
                        origens['Item_Check'] = origens.apply(lambda x: f"{x['produto']} - {x['essencia']}" if x['essencia'] else x['produto'], axis=1)
                        origens['Grupo'] = origens['Item_Check'].map(agrup_sis)
                        origens['Grupo'] = origens['Grupo'].fillna(origens['Item_Check'])

                        for _, r in origens.iterrows():
                            if pd.notnull(r['Grupo']):
                                if r['Grupo'] not in saldo_aud_sis:
                                    saldo_aud_sis[r['Grupo']] = {'Entrada': 0, 'Saida': 0}
                                saldo_aud_sis[r['Grupo']]['Saida'] += r['volume']

                if not df_consumo.empty:
                    df_consumo['Item_Check'] = df_consumo.apply(lambda x: f"{x.get('produto','')} - {x.get('essencia','')}" if x.get('essencia') else x.get('produto',''), axis=1)
                    df_consumo['Grupo'] = df_consumo['Item_Check'].map(agrup_sis)
                    df_consumo['Grupo'] = df_consumo['Grupo'].fillna(df_consumo['Item_Check'])

                    for _, r in df_consumo.iterrows():
                        if pd.notnull(r['Grupo']):
                            if r['Grupo'] not in saldo_aud_sis:
                                saldo_aud_sis[r['Grupo']] = {'Entrada': 0, 'Saida': 0}
                            vol = r.get('volume', 0)
                            try:
                                vol = float(vol)
                            except:
                                vol = 0
                            saldo_aud_sis[r['Grupo']]['Saida'] += vol

                saldo_aud_ple = {}
                agrup_ple = st.session_state['agrup_ple']

                if not df_plenus_mov.empty:
                    # FIX v181: Match old logic "Produto (Categoria)"
                    df_plenus_mov['Item_Completo'] = df_plenus_mov["produto"] + " (" + df_plenus_mov["categoria"].fillna("") + ")"
                    df_plenus_mov['Grupo_Inter'] = df_plenus_mov['Item_Completo'].map(agrup_ple)
                    df_plenus_mov['Grupo_Calc'] = df_plenus_mov['Grupo_Inter'].map(vinculos)
                    df_plenus_mov['Grupo_Calc'] = df_plenus_mov['Grupo_Calc'].fillna(df_plenus_mov['Grupo_Inter'])

                    for _, r in df_plenus_mov.iterrows():
                        if pd.notnull(r['Grupo_Calc']):
                            grp = r['Grupo_Calc']
                            if grp not in saldo_aud_ple:
                                saldo_aud_ple[grp] = {'Entrada': 0, 'Saida': 0}
                            saldo_aud_ple[grp]['Entrada'] += r['entrada']
                            saldo_aud_ple[grp]['Saida'] += r['saida']

                todos_grupos = set(saldo_aud_sis.keys()) | set(saldo_aud_ple.keys())
                relatorio = []
                for g in todos_grupos:
                    s_ent = saldo_aud_sis.get(g, {'Entrada': 0})['Entrada']
                    s_sai = saldo_aud_sis.get(g, {'Saida': 0})['Saida']
                    s_liq = s_ent - s_sai
                    p_ent = saldo_aud_ple.get(g, {'Entrada': 0})['Entrada']
                    p_sai = saldo_aud_ple.get(g, {'Saida': 0})['Saida']
                    p_liq = p_ent - p_sai
                    diff = s_liq - p_liq

                    if any(abs(x) > 0.0001 for x in [s_ent, s_sai, s_liq, p_ent, p_sai, p_liq]):
                        relatorio.append({
                            "Grupo": g,
                            "Sis_Entrada": s_ent, "Sis_Saida": s_sai, "Sis_Liquido": s_liq,
                            "Ple_Entrada": p_ent, "Ple_Saida": p_sai, "Ple_Liquido": p_liq,
                            "Diferenca_Mov": diff
                        })

                df_rel = pd.DataFrame(relatorio)

                # Funcao de estilo manual sem matplotlib
                def style_difference(v):
                    if abs(v) < 0.01:
                        return 'background-color: #d4edda; color: #155724'  # Verde claro
                    elif v < 0:
                        return 'background-color: #f8d7da; color: #721c24'  # Vermelho claro
                    else:
                        return 'background-color: #cce5ff; color: #004085'  # Azul claro

                if not df_rel.empty:
                    render_filtered_table(df_rel, "aud_result", show_total=True)
                else:
                    st.info("Nenhuma movimentação encontrada neste período.")
