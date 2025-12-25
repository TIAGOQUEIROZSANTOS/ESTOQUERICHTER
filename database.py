import sqlite3
import pandas as pd
import json
import streamlit as st
from datetime import datetime, date

# --- CONFIGURAÇÃO ---
ARQUIVO_DB = "dados_sistema.db"

# --- FUNÇÕES DB AUXILIARES (NOVAS v177/v180) ---
def get_max_date_db(tabela, coluna_data):
    """Retorna a data máxima salva no banco para sugerir no calendário."""
    conn = sqlite3.connect(ARQUIVO_DB)
    c = conn.cursor()
    try:
        query = f"SELECT MAX({coluna_data}) FROM {tabela}"
        c.execute(query)
        res = c.fetchone()
        if res and res[0]:
            val_str = str(res[0])[:10]
            # Tenta formatos variados para robustez
            for fmt in ["%Y-%m-%d", "%d/%m/%Y"]:
                try:
                    return datetime.strptime(val_str, fmt).date()
                except:
                    pass
    except:
        pass
    finally:
        conn.close()
    return None

def get_smart_date_range(tabela, coluna_data):
    """
    Retorna (dt_ini, dt_fim) baseado na inteligência.
    """
    max_date = get_max_date_db(tabela, coluna_data)
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
    """Atualiza as datas da sessão para refletir o arquivo importado."""
    if dt_min and dt_max:
        st.session_state[f'{prefix}_dt_ini'] = dt_min
        st.session_state[f'{prefix}_dt_fim'] = dt_max

# --- BANCO DE DADOS (DB) ---
def init_db():
    conn = sqlite3.connect(ARQUIVO_DB)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS agrupamentos (item_original TEXT, nome_grupo TEXT, origem TEXT, categoria TEXT, PRIMARY KEY (item_original, origem))''')
    c.execute('''CREATE TABLE IF NOT EXISTS vinculos (grupo_plenus TEXT PRIMARY KEY, grupo_sisflora TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS sessoes (nome_sessao TEXT PRIMARY KEY, data_salvamento TEXT, dados_sisflora TEXT, dados_plenus TEXT)''')

    # Tabela de Estado da Navegação (V185)
    c.execute('''CREATE TABLE IF NOT EXISTS estado_sistema (chave TEXT PRIMARY KEY, valor TEXT)''')

    # Tabela SISTRANSF
    c.execute('''CREATE TABLE IF NOT EXISTS transf_historico (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        numero TEXT, data_realizacao TEXT, situacao TEXT,
        tipo_produto TEXT, produto TEXT, popular TEXT, essencia TEXT,
        volume REAL, unidade TEXT, arquivo_origem TEXT
    )''')

    # Tabela PLENUS
    c.execute('''CREATE TABLE IF NOT EXISTS plenus_historico (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        sku TEXT, produto TEXT, categoria TEXT,
        data_movimento TEXT, tipo_movimento TEXT,
        entrada REAL, saida REAL, saldo_apos REAL,
        saldo_anterior REAL,
        nota TEXT, serie TEXT, vlr_unit REAL, vlr_total REAL, arquivo_origem TEXT
    )''')
    # V189: Adiciona coluna saldo_anterior se não existir
    try:
        c.execute("ALTER TABLE plenus_historico ADD COLUMN saldo_anterior REAL")
    except:
        pass  # Coluna já existe
    # V193: Adiciona colunas vlr_unit e vlr_total se não existirem
    try:
        c.execute("ALTER TABLE plenus_historico ADD COLUMN vlr_unit REAL")
    except:
        pass  # Coluna já existe
    try:
        c.execute("ALTER TABLE plenus_historico ADD COLUMN vlr_total REAL")
    except:
        pass  # Coluna já existe

    # Tabela SISCONSUMO
    c.execute('''CREATE TABLE IF NOT EXISTS consumo_historico (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        data_consumo TEXT, produto TEXT, essencia TEXT,
        volume REAL, documento TEXT, arquivo_origem TEXT,
        dados_json TEXT
    )''')

    try:
        c.execute("ALTER TABLE consumo_historico ADD COLUMN dados_json TEXT")
    except:
        pass

    # Tabela SISFLORA SALDO
    c.execute('''CREATE TABLE IF NOT EXISTS sisflora_historico (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        data_referencia TEXT, produto TEXT, essencia TEXT, unidade TEXT,
        volume_disponivel REAL, codigo TEXT, cat_auto TEXT,
        arquivo_origem TEXT
    )''')

    try:
        c.execute("ALTER TABLE agrupamentos ADD COLUMN categoria TEXT")
    except:
        pass
    conn.commit()
    conn.close()

def get_db_connection():
    return sqlite3.connect(ARQUIVO_DB)

def excluir_periodo_tabela(tabela, col_data, dt_ini, dt_fim):
    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute(f"SELECT COUNT(*) FROM {tabela} WHERE {col_data} BETWEEN ? AND ?",
                  (dt_ini.strftime("%Y-%m-%d"), dt_fim.strftime("%Y-%m-%d")))
        count = c.fetchone()[0]
        if count > 0:
            c.execute(f"DELETE FROM {tabela} WHERE {col_data} BETWEEN ? AND ?",
                      (dt_ini.strftime("%Y-%m-%d"), dt_fim.strftime("%Y-%m-%d")))
            conn.commit()
        return count
    finally:
        conn.close()

# --- PERSISTÊNCIA DE ESTADO (V185 - via Banco de Dados) ---
def load_app_state():
    """Carrega o estado da navegação do banco SQLite."""
    conn = get_db_connection()
    try:
        c = conn.cursor()
        c.execute("SELECT chave, valor FROM estado_sistema")
        rows = c.fetchall()
        # Restaura chaves principais se não existirem na sessão
        for chave, valor in rows:
            if chave not in st.session_state:
                # Tenta converter para int (para indices) ou mantem string
                try:
                    if valor.isdigit():
                        st.session_state[chave] = int(valor)
                    else:
                        st.session_state[chave] = valor
                except:
                    st.session_state[chave] = valor
    except Exception as e:
        pass  # Tabela pode nao existir ainda na primeira execucao (sera criada no init_db)
    finally:
        conn.close()

def save_app_state():
    """Salva o estado atual das chaves de navegação no banco SQLite."""
    keys_to_save = [
        "menu_sel_idx",  # Índice do Menu Principal
        "nav_sis_183",  # Sub-menu Sisflora
        "nav_ple_183",  # Sub-menu Plenus
        # Filtros Persistentes
        "p_dt_ini", "p_dt_fim",
        "t_dt_ini", "t_dt_fim",
        "c_dt_ini", "c_dt_fim",
        "aud_dt_ini", "aud_dt_fim"
    ]

    data_to_save = []
    for k in keys_to_save:
        if k in st.session_state:
            val = st.session_state[k]
            val_str = ""
            if isinstance(val, (date, datetime)):
                val_str = val.strftime("%Y-%m-%d")
            else:
                val_str = str(val)
            data_to_save.append((k, val_str))

    if data_to_save:
        conn = get_db_connection()
        try:
            c = conn.cursor()
            c.executemany("INSERT OR REPLACE INTO estado_sistema (chave, valor) VALUES (?, ?)", data_to_save)
            conn.commit()
        except:
            pass
        finally:
            conn.close()

# --- FUNÇÕES DB S&P ---
@st.cache_data(ttl="1h")
def carregar_agrupamentos_db(origem):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT item_original, nome_grupo FROM agrupamentos WHERE origem = ?", (origem,))
    rows = c.fetchall()
    conn.close()
    return {row[0]: row[1] for row in rows}

def carregar_todos_agrupamentos_db():
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT * FROM agrupamentos", conn)
    except:
        df = pd.DataFrame()
    conn.close()
    return df

def get_categorias_dos_grupos(origem):
    conn = get_db_connection()
    try:
        df = pd.read_sql("SELECT nome_grupo, categoria FROM agrupamentos WHERE origem = ?", conn, params=(origem,))
        if df.empty:
            return {}
        return df.groupby('nome_grupo')['categoria'].agg(lambda x: x.mode()[0] if not x.mode().empty else "OUTROS").to_dict()
    finally:
        conn.close()

def salvar_agrupamento_db(itens, nome_grupo, origem, categoria_detectada):
    conn = get_db_connection()
    c = conn.cursor()
    try:
        data = [(it, nome_grupo, origem, categoria_detectada) for it in itens]
        c.executemany("INSERT OR REPLACE INTO agrupamentos (item_original, nome_grupo, origem, categoria) VALUES (?, ?, ?, ?)", data)
        conn.commit()
        st.toast(f"✅ Grupo Salvo: {nome_grupo}", icon="💾")
    except Exception as e:
        st.error(f"Erro DB: {e}")
    finally:
        conn.close()

def excluir_grupo_db(nome_grupo, origem):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("DELETE FROM agrupamentos WHERE nome_grupo = ? AND origem = ?", (nome_grupo, origem))
    conn.commit()
    conn.close()
    carregar_agrupamentos_db.clear()

def carregar_vinculos_db():
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT grupo_plenus, grupo_sisflora FROM vinculos")
    rows = c.fetchall()
    conn.close()
    return {row[0]: row[1] for row in rows}

def carregar_lista_grupos_db(origem):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT DISTINCT nome_grupo FROM agrupamentos WHERE origem = ? ORDER BY nome_grupo", (origem,))
    rows = c.fetchall()
    conn.close()
    return [r[0] for r in rows]

def salvar_vinculo_db(grupos_plenus_lista, grupo_sisflora):
    conn = get_db_connection()
    c = conn.cursor()
    try:
        data = [(gp, grupo_sisflora) for gp in grupos_plenus_lista]
        c.executemany("INSERT OR REPLACE INTO vinculos (grupo_plenus, grupo_sisflora) VALUES (?, ?)", data)
        conn.commit()
        st.toast(f"🔗 Vínculo criado!", icon="🔗")
    finally:
        conn.close()

def excluir_vinculo_db(grupo_plenus):
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("DELETE FROM vinculos WHERE grupo_plenus = ?", (grupo_plenus,))
    conn.commit()
    conn.close()

# --- FUNÇÕES DB INTELIGENTES (v180) ---
def check_dates_exist(tabela, col_data, datas_lista):
    """Verifica quais datas da lista já existem no banco."""
    conn = get_db_connection()
    datas_str = "', '".join([d.strftime("%Y-%m-%d") for d in datas_lista])
    query = f"SELECT DISTINCT {col_data} FROM {tabela} WHERE {col_data} IN ('{datas_str}')"
    try:
        df_exist = pd.read_sql(query, conn)
        return set(df_exist[col_data].tolist())
    except:
        return set()
    finally:
        conn.close()

def check_months_exist(tabela, col_data, datas_lista):
    """Verifica quais meses/anos da lista já existem no banco (YYYY-MM)."""
    conn = get_db_connection()
    # V191: Filtrar valores NaT/None antes de fazer strftime
    datas_validas = [d for d in datas_lista if pd.notna(d) and d is not None]
    meses_ano = set([d.strftime("%Y-%m") for d in datas_validas])
    # Busca todos os registros e extrai mês/ano
    query = f"SELECT DISTINCT {col_data} FROM {tabela} WHERE {col_data} IS NOT NULL"
    try:
        df_exist = pd.read_sql(query, conn)
        if not df_exist.empty:
            df_exist['mes_ano'] = pd.to_datetime(df_exist[col_data]).dt.strftime("%Y-%m")
            meses_existentes = set(df_exist['mes_ano'].unique())
            return meses_existentes.intersection(meses_ano)
        return set()
    except:
        return set()
    finally:
        conn.close()

def salvar_lote_smart(tabela, col_data, df, conn=None, por_mes=False):
    """
    Salva dados no banco, mas FILTRA registros cujas datas/meses já existem.
    Não sobrescreve, apenas preenche lacunas.

    Args:
        tabela: Nome da tabela
        col_data: Nome da coluna de data
        df: DataFrame para salvar
        conn: Conexão (opcional)
        por_mes: Se True, verifica por mês/ano. Se False, verifica por data específica.
    """
    if df.empty:
        return 0, 0

    if por_mes:
        # Verifica por mês/ano
        # V191: Filtrar valores NaT antes de converter para date
        dates_series = pd.to_datetime(df[col_data], errors='coerce')
        dates_validas = dates_series[dates_series.notna()].dt.date.unique()
        meses_existentes = check_months_exist(tabela, col_data, dates_validas)
        existing_count = len(meses_existentes)

        # Filtra DF - remove registros cujo mês/ano já existe
        df_check = df.copy()
        # V191: Converter para datetime e filtrar NaT antes de fazer strftime
        dates_series_check = pd.to_datetime(df_check[col_data], errors='coerce')
        # Criar coluna mes_ano apenas para linhas com data válida (não NaT)
        df_check['mes_ano'] = dates_series_check.apply(lambda x: x.strftime("%Y-%m") if pd.notna(x) else None)

        # Filtrar registros cujo mês/ano não existe ainda (e tem data válida)
        # IMPORTANTE: Linhas TOTAL podem ter data atribuída anteriormente, então devem ser incluídas
        df_to_save = df_check[(~df_check['mes_ano'].isin(meses_existentes)) & (df_check['mes_ano'].notna())].drop(columns=['mes_ano'])

        # DEBUG: Verificar quantos registros foram filtrados
        registros_filtrados = len(df_check) - len(df_to_save)
        registros_sem_data = len(df_check[df_check['mes_ano'].isna()])
        registros_mes_existente = len(df_check[df_check['mes_ano'].isin(meses_existentes)])

        # Log detalhado (apenas se houver registros filtrados)
        if registros_filtrados > 0 or registros_sem_data > 0:
            import logging
            logging.basicConfig(level=logging.INFO)
            logger = logging.getLogger(__name__)
            logger.info(f"salvar_lote_smart: {len(df_check)} registros totais, {len(df_to_save)} para salvar, {registros_sem_data} sem data, {registros_mes_existente} com mês existente")

        # V191: Converter coluna data_movimento para string se for Timestamp (antes de salvar)
        if col_data in df_to_save.columns and pd.api.types.is_datetime64_any_dtype(df_to_save[col_data]):
            df_to_save[col_data] = df_to_save[col_data].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)

        if df_to_save.empty:
            return 0, existing_count
    else:
        # Verifica por data específica (comportamento original)
        dates_in_df = pd.to_datetime(df[col_data]).dt.strftime("%Y-%m-%d").unique()
        dates_in_df_dt = [datetime.strptime(d, "%Y-%m-%d").date() for d in dates_in_df]

        existing = check_dates_exist(tabela, col_data, dates_in_df_dt)
        existing_count = len(existing)

        # Filtra DF
        df_check = df.copy()
        if pd.api.types.is_datetime64_any_dtype(df_check[col_data]):
            df_check[col_data] = df_check[col_data].dt.strftime("%Y-%m-%d")

        df_to_save = df_check[~df_check[col_data].isin(existing)]

        if df_to_save.empty:
            return 0, existing_count

    local_conn = conn if conn else get_db_connection()
    try:
        df_to_save.to_sql(tabela, local_conn, if_exists='append', index=False)
        if not conn:
            local_conn.commit()
        inseridos = len(df_to_save)
        return inseridos, existing_count
    finally:
        if not conn:
            local_conn.close()

# --- FUNÇÕES DB ESPECÍFICAS ---
def carregar_transf_filtrado_db(dt_ini, dt_fim, lista_filtros=None):
    conn = get_db_connection()
    query = "SELECT * FROM transf_historico WHERE substr(data_realizacao,1,10) BETWEEN ? AND ?"
    params = [dt_ini.strftime("%Y-%m-%d"), dt_fim.strftime("%Y-%m-%d")]

    mapa_cols = {
        "Número": "numero", "Situação": "situacao",
        "PRODUTO": "tipo_produto", "Produto": "produto", "Popular": "popular",
        "Essência": "essencia", "Unidade": "unidade"
    }

    if lista_filtros:
        for f in lista_filtros:
            col_db = mapa_cols.get(f['col'])
            valores = f['vals']
            if col_db and valores:
                placeholders = ','.join(['?'] * len(valores))
                query += f" AND {col_db} IN ({placeholders})"
                params.extend(valores)
    try:
        return pd.read_sql(query, conn, params=params)
    except:
        return pd.DataFrame()
    finally:
        conn.close()

def get_valores_unicos_coluna(coluna, tabela="transf_historico"):
    mapa_cols = {
        "Número": "numero", "Situação": "situacao",
        "PRODUTO": "tipo_produto", "Produto": "produto", "Popular": "popular",
        "Essência": "essencia", "Unidade": "unidade"
    }
    col_db = mapa_cols.get(coluna)
    if not col_db:
        return []

    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute(f"SELECT DISTINCT {col_db} FROM {tabela} ORDER BY {col_db}")
        rows = c.fetchall()
        return [str(r[0]) for r in rows if r[0] is not None and str(r[0]).strip() != ""]
    except:
        return []
    finally:
        conn.close()

def carregar_plenus_movimento_db(dt_ini, dt_fim):
    """
    Carrega movimentos PLENUS do banco de dados para um período.
    CORREÇÃO: Inclui TODAS as linhas TOTAL/ANTERIOR, mesmo que a data esteja fora do período.
    Isso garante que produtos sem movimentação no período apareçam com saldo anterior e total.
    """
    conn = get_db_connection()
    # Carregar movimentações normais dentro do período
    query_mov = """
        SELECT * FROM plenus_historico
        WHERE substr(data_movimento,1,10) BETWEEN ? AND ?
    """
    params_mov = [dt_ini.strftime("%Y-%m-%d"), dt_fim.strftime("%Y-%m-%d")]

    # Carregar TODAS as linhas TOTAL/ANTERIOR (independente da data)
    # Isso garante que produtos sem movimentação no período apareçam
    query_totais = """
        SELECT * FROM plenus_historico
        WHERE tipo_movimento IN ('TOTAL', 'TOTAL:', 'ANTERIOR', 'ANTERIOR:')
    """

    try:
        df_mov = pd.read_sql(query_mov, conn, params=params_mov)
        df_totais = pd.read_sql(query_totais, conn)

        # Combinar ambos, removendo duplicatas (se uma linha TOTAL/ANTERIOR já está no período, não duplicar)
        if df_mov.empty:
            return df_totais
        elif df_totais.empty:
            return df_mov
        else:
            # Remover duplicatas baseado em sku + tipo_movimento + data_movimento
            df_combined = pd.concat([df_mov, df_totais], ignore_index=True)
            # CORREÇÃO CRÍTICA: Para linhas TOTAL, remover duplicatas por SKU (apenas uma linha TOTAL por SKU)
            # Isso garante que não haja múltiplas linhas TOTAL do mesmo SKU sendo somadas
            if 'tipo_movimento' in df_combined.columns:
                # Separar linhas TOTAL das outras
                mask_total = df_combined['tipo_movimento'].astype(str).str.strip().str.upper().isin(['TOTAL', 'TOTAL:', 'TOTAL '])
                df_totais_combined = df_combined[mask_total].copy()
                df_outras = df_combined[~mask_total].copy()

                # Remover duplicatas de linhas TOTAL por SKU (manter apenas a mais recente por id)
                if not df_totais_combined.empty and 'sku' in df_totais_combined.columns:
                    if 'id' in df_totais_combined.columns:
                        # Ordenar por id descendente para manter a mais recente
                        df_totais_combined = df_totais_combined.sort_values(by='id', ascending=False, na_position='last')
                    # Remover duplicatas por SKU (apenas uma linha TOTAL por SKU)
                    df_totais_combined = df_totais_combined.drop_duplicates(subset=['sku'], keep='first')

                # Recombinar
                if not df_outras.empty and not df_totais_combined.empty:
                    df_combined = pd.concat([df_outras, df_totais_combined], ignore_index=True)
                elif not df_totais_combined.empty:
                    df_combined = df_totais_combined.copy()
                elif not df_outras.empty:
                    df_combined = df_outras.copy()

                # Remover outras duplicatas (movimentações com mesmo sku + tipo + data)
                if 'id' in df_combined.columns:
                    df_combined = df_combined.sort_values(by='id', ascending=False, na_position='last')
                df_combined = df_combined.drop_duplicates(subset=['sku', 'tipo_movimento', 'data_movimento'], keep='first')
            else:
                # Se não tem tipo_movimento, usar lógica antiga
                if 'id' in df_combined.columns:
                    df_combined = df_combined.sort_values(by='id', ascending=False, na_position='last')
                df_combined = df_combined.drop_duplicates(subset=['sku', 'tipo_movimento', 'data_movimento'], keep='first')

            # Reordenar por id ascendente para manter ordem original
            if 'id' in df_combined.columns:
                df_combined = df_combined.sort_values(by='id', ascending=True, na_position='last')
            return df_combined
    finally:
        conn.close()

def buscar_todos_skus_db():
    """
    Busca todos os SKUs únicos do banco de dados (com produto e categoria).
    Retorna um DataFrame com sku, produto, categoria únicos.
    V191: Pega todos os registros e agrupa por SKU, usando o último produto e categoria de cada SKU.
    """
    conn = get_db_connection()
    try:
        # Buscar todos os registros com SKU
        query = """
            SELECT sku, produto, categoria
            FROM plenus_historico
            WHERE sku IS NOT NULL
            ORDER BY sku, id DESC
        """
        df = pd.read_sql(query, conn)
        if df.empty:
            return pd.DataFrame(columns=['sku', 'produto', 'categoria'])

        # Agrupar por SKU e pegar o primeiro registro de cada (que é o mais recente devido ao ORDER BY id DESC)
        df_unique = df.groupby('sku', as_index=False).first()
        df_unique['produto'] = df_unique['produto'].fillna('')
        df_unique['categoria'] = df_unique['categoria'].fillna('')
        return df_unique[['sku', 'produto', 'categoria']]
    finally:
        conn.close()

def buscar_ultimo_saldo_antes_data_db(dt_antes, sku_list=None):
    """
    Busca o último saldo_apos de cada SKU antes de uma data específica.
    Retorna um dicionário {sku: ultimo_saldo_apos}
    Se sku_list for fornecido, busca apenas esses SKUs.
    """
    import pandas as pd
    conn = get_db_connection()
    try:
        dt_str = dt_antes.strftime("%Y-%m-%d")

        if sku_list and len(sku_list) > 0:
            # Busca apenas SKUs específicos
            placeholders = ','.join(['?' for _ in sku_list])
            query = f"""
                SELECT sku, saldo_apos, data_movimento, id
                FROM plenus_historico
                WHERE substr(data_movimento,1,10) < ? AND sku IN ({placeholders})
            """
            params = [dt_str] + list(sku_list)
        else:
            # Busca todos os SKUs
            query = """
                SELECT sku, saldo_apos, data_movimento, id
                FROM plenus_historico
                WHERE substr(data_movimento,1,10) < ?
            """
            params = [dt_str]

        df = pd.read_sql(query, conn, params=params)

        if df.empty:
            return {}

        # Ordenar por SKU, data (DESC) e id (DESC) para pegar o mais recente de cada SKU
        df['data_mov_dt'] = pd.to_datetime(df['data_movimento'], errors='coerce')
        df = df.sort_values(by=['sku', 'data_mov_dt', 'id'], ascending=[True, False, False], na_position='last')

        # Para cada SKU, pega o primeiro resultado (que é o mais recente)
        ultimos_saldos = {}
        for sku in df['sku'].unique():
            df_sku = df[df['sku'] == sku]
            if not df_sku.empty:
                ultimo_saldo = df_sku.iloc[0]['saldo_apos']
                if pd.notna(ultimo_saldo):
                    ultimos_saldos[sku] = float(ultimo_saldo)
                else:
                    ultimos_saldos[sku] = 0.0

        return ultimos_saldos
    finally:
        conn.close()

def carregar_consumo_filtrado_db(dt_ini, dt_fim):
    conn = get_db_connection()
    query = "SELECT * FROM consumo_historico WHERE substr(data_consumo,1,10) BETWEEN ? AND ?"
    params = [dt_ini.strftime("%Y-%m-%d"), dt_fim.strftime("%Y-%m-%d")]
    try:
        df = pd.read_sql(query, conn, params=params)
        if 'dados_json' in df.columns:
            json_series = df['dados_json'].dropna()
            json_series = json_series[json_series != ""]
            if not json_series.empty:
                try:
                    list_of_dicts = json_series.apply(lambda x: json.loads(x)).tolist()
                    df_expanded = pd.json_normalize(list_of_dicts)
                    cols_padrao = ['produto', 'essencia', 'volume']
                    for col in cols_padrao:
                        if col not in df_expanded.columns and col in df.columns:
                            df_expanded[col] = df[col].values
                    return df_expanded
                except:
                    return df
        return df
    finally:
        conn.close()

def salvar_lote_sisflora_db(df, data_ref, nome_arquivo):
    conn = get_db_connection()
    try:
        df_save = df.copy()
        df_save.rename(columns={
            "Produto": "produto", "Essencia": "essencia", "Unidade": "unidade",
            "Volume Disponivel": "volume_disponivel", "Codigo": "codigo", "Cat_Auto": "cat_auto"
        }, inplace=True)
        cols_db = ["produto", "essencia", "unidade", "volume_disponivel", "codigo", "cat_auto"]
        for c in cols_db:
            if c not in df_save.columns:
                df_save[c] = ""
        df_save = df_save[cols_db].copy()
        df_save["data_referencia"] = data_ref.strftime("%Y-%m-%d")
        df_save["arquivo_origem"] = nome_arquivo

        c = conn.cursor()
        c.execute("DELETE FROM sisflora_historico WHERE data_referencia = ?", (data_ref.strftime("%Y-%m-%d"),))
        conn.commit()
        df_save.to_sql('sisflora_historico', conn, if_exists='append', index=False)
        return True
    except Exception as e:
        st.error(f"Erro ao salvar Sisflora: {e}")
        return False
    finally:
        conn.close()

def carregar_sisflora_data_db(data_ref):
    conn = get_db_connection()
    query = "SELECT * FROM sisflora_historico WHERE data_referencia = ?"
    try:
        df = pd.read_sql(query, conn, params=(data_ref.strftime("%Y-%m-%d"),))
        if not df.empty:
            df.rename(columns={
                "produto": "Produto", "essencia": "Essencia", "unidade": "Unidade",
                "volume_disponivel": "Volume Disponivel", "codigo": "Codigo", "cat_auto": "Cat_Auto"
            }, inplace=True)
            df["Item_Completo"] = df.apply(lambda x: f"{x['Produto']} - {x['Essencia']}" if x['Essencia'] else x['Produto'], axis=1)
        return df
    finally:
        conn.close()

def get_datas_sisflora_disponiveis():
    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute("SELECT DISTINCT data_referencia FROM sisflora_historico ORDER BY data_referencia DESC")
        rows = c.fetchall()
        return [datetime.strptime(r[0], "%Y-%m-%d").date() for r in rows]
    except:
        return []
    finally:
        conn.close()

def excluir_sisflora_por_data(data_ref):
    conn = get_db_connection()
    c = conn.cursor()
    try:
        c.execute("DELETE FROM sisflora_historico WHERE data_referencia = ?", (data_ref.strftime("%Y-%m-%d"),))
        conn.commit()
        return True
    except:
        return False
    finally:
        conn.close()
