import streamlit as st
import pandas as pd
from datetime import date, datetime
import json
import os
from processamento import formatar_br
from database import save_app_state, salvar_lote_smart, update_session_dates

# --- FUNÇÕES UI V180 (FILTROS E ESTILO) ---
def render_filtered_table(df, key_prefix, show_total=True):
    """
    Renderiza tabela com filtros avançados e formatação rigorosa.
    v180: Exclui colunas IDs/Códigos da soma total.
    """
    if df.empty:
        st.info("Nenhum dado para exibir.")
        return

    # 1. Filtro Texto Global
    # IMPORTANTE: Usar key única para garantir que Streamlit detecte mudanças
    c1, c2 = st.columns([2, 1])
    txt_search = c1.text_input("🔎 Pesquisa Global:", key=f"txt_{key_prefix}", value="")

    # 2. Filtro Colunas/Categoria
    cols_filter = c2.multiselect("Filtrar por Coluna(s):", df.columns, key=f"cols_{key_prefix}")

    df_view = df.copy()

    # Aplica busca textual - usar valor direto do input
    if txt_search and str(txt_search).strip():
        mask = df_view.astype(str).apply(lambda x: x.str.contains(str(txt_search).strip(), case=False, na=False)).any(axis=1)
        df_view = df_view[mask]

    # Aplica filtro de colunas (mostra apenas essas colunas)
    if cols_filter:
        df_view = df_view[cols_filter]

    # 3. Totais (v180: Filtra colunas não somáveis)
    if show_total:
        numerics = df_view.select_dtypes(include=['float', 'int'])
        # Excluir colunas que parecem IDs, Códigos, Anos, etc.
        ignore_terms = ['id', 'sku', 'codigo', 'código', 'numero', 'número', 'nota', 'serie', 'série', 'ano', 'mes', 'dia']
        cols_to_sum = [c for c in numerics.columns if not any(term in c.lower() for term in ignore_terms)]

        if cols_to_sum:
            total_html = "<div style='display:flex; gap: 20px; flex-wrap: wrap; margin-bottom: 10px;'>"
            for col in cols_to_sum:
                val = df_view[col].sum()
                if abs(val) > 0.0001:
                    total_html += f"<div style='background:#e9ecef; padding:5px 10px; border-radius:4px;'><b>{col}:</b> {val:,.4f}".replace(",", "X").replace(".", ",").replace("X", ".") + "</div>"
            total_html += "</div>"
            st.markdown(total_html, unsafe_allow_html=True)

    # 4. Formatação Visual Rigorosa (Datas BR, 4 Casas)
    date_cols = []
    for col in df_view.columns:
        if pd.api.types.is_datetime64_any_dtype(df_view[col]):
            date_cols.append(col)
            df_view[col] = df_view[col].dt.strftime('%d/%m/%Y')
        elif df_view[col].astype(str).str.match(r'^\d{4}-\d{2}-\d{2}$').all():
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


# --- FUNÇÃO UI REUTILIZÁVEL PLENUS (v183 Refactor) ---
def render_plenus_dashboard(df_full, key_prefix="p_dash", allow_save=True):
    # --- Date Filter ---
    # Determine bounds
    min_d, max_d = date.today(), date.today()
    if 'data_movimento' in df_full.columns:
        try:
            dates = pd.to_datetime(df_full['data_movimento'], errors='coerce').dropna().dt.date
            if not dates.empty:
                min_d, max_d = dates.min(), dates.max()
        except:
            pass

    # Calendário primeiro (por cima da consulta)
    st.markdown("##### 🗓️ Filtrar Período")
    c_f1, c_f2 = st.columns([1, 1])
    d_ini_f = c_f1.date_input("Filtrar Data De:", value=min_d, key=f"{key_prefix}_filtro_p_ini", format="DD/MM/YYYY")
    d_fim_f = c_f2.date_input("Filtrar Data Até:", value=max_d, key=f"{key_prefix}_filtro_p_fim", format="DD/MM/YYYY")

    # Apply Filter de data primeiro
    # CORREÇÃO CRÍTICA: Manter TODAS as linhas TOTAL/ANTERIOR, independente da data
    # Isso garante que produtos como AMAPA (que só têm TOTAL/ANTERIOR) apareçam sempre
    df_view = df_full.copy()
    if 'data_movimento' in df_view.columns:
        df_view['dt_temp'] = pd.to_datetime(df_view['data_movimento'], errors='coerce').dt.date
        # IMPORTANTE: Manter TODAS as linhas TOTAL/ANTERIOR, mesmo que a data esteja fora do período
        # Também manter linhas sem data válida
        # Filtrar apenas movimentações normais por data
        if 'tipo' in df_view.columns:
            mask_tipo_especial = df_view['tipo'].astype(str).str.strip().str.upper().isin(['TOTAL', 'TOTAL:', 'ANTERIOR', 'ANTERIOR:'])
            # Manter: linhas TOTAL/ANTERIOR OU sem data OU dentro do período
            mask = mask_tipo_especial | (df_view['dt_temp'].isna()) | ((df_view['dt_temp'] >= d_ini_f) & (df_view['dt_temp'] <= d_fim_f))
        else:
            mask = (df_view['dt_temp'].isna()) | ((df_view['dt_temp'] >= d_ini_f) & (df_view['dt_temp'] <= d_fim_f))
        df_view = df_view[mask].drop(columns=['dt_temp'])

    # Filtros de Texto Global e Coluna (igual SISFLORA) - abaixo do calendário
    # CORREÇÃO: Usar diretamente o valor do input para filtrar (atualiza imediatamente ao digitar/Enter)
    c1, c2, c_btn2 = st.columns([2, 1, 1])

    # Inicializar valores do session_state se não existirem
    if f"txt_{key_prefix}" not in st.session_state:
        st.session_state[f"txt_{key_prefix}"] = ""
    if f"cols_{key_prefix}" not in st.session_state:
        st.session_state[f"cols_{key_prefix}"] = []

    # IMPORTANTE: Streamlit text_input com key única gerencia o valor automaticamente
    # Quando você digita e pressiona Enter, o Streamlit atualiza o valor na próxima execução
    # Não usar 'value=' para evitar conflitos - deixar o Streamlit gerenciar via key

    # Inicializar session_state se não existir
    if f"txt_{key_prefix}" not in st.session_state:
        st.session_state[f"txt_{key_prefix}"] = ""
    if f"cols_{key_prefix}" not in st.session_state:
        st.session_state[f"cols_{key_prefix}"] = []

    # Criar o input widget - usa o valor do session_state como valor inicial
    # Inicializar session_state se não existir
    if f"txt_{key_prefix}" not in st.session_state:
        st.session_state[f"txt_{key_prefix}"] = ""
    if f"cols_{key_prefix}" not in st.session_state:
        st.session_state[f"cols_{key_prefix}"] = []

    txt_search_input = c1.text_input("🔎 Pesquisa Global:", key=f"txt_{key_prefix}_input", value=st.session_state.get(f"txt_{key_prefix}", ""))
    cols_filter_input = c2.multiselect("Pesquisar em Coluna(s):", df_view.columns, key=f"cols_{key_prefix}_input", default=st.session_state.get(f"cols_{key_prefix}", []))

    # Botão Consultar (sincroniza com session_state principal e recarrega)
    # IMPORTANTE: Quando clicado, atualiza o session_state e faz rerun para aplicar filtros e recalcular saldo
    if c_btn2.button("🔍 Consultar", key=f"{key_prefix}_btn_consultar", type="primary", use_container_width=True):
        # Atualiza session_state com o valor atual do widget
        st.session_state[f"txt_{key_prefix}"] = str(txt_search_input).strip() if txt_search_input else ""
        st.session_state[f"cols_{key_prefix}"] = list(cols_filter_input) if cols_filter_input else []
        save_app_state()
        # Rerun faz o código rodar novamente, usando os novos valores do session_state
        # Isso garante que os filtros sejam aplicados e o saldo seja recalculado
        st.rerun()

    # CRÍTICO: Usar o valor do session_state (não do widget diretamente)
    # Isso garante que o filtro seja aplicado corretamente após clicar em "Consultar"
    # Quando o botão Consultar é clicado, o session_state é atualizado e o rerun faz este código rodar novamente
    # com os novos valores, aplicando os filtros e recalculando o saldo
    txt_search = st.session_state.get(f"txt_{key_prefix}", "").strip()
    cols_filter = st.session_state.get(f"cols_{key_prefix}", [])

    # Função auxiliar para normalizar SKU (remover zeros à esquerda)
    def normalizar_sku(sku_str):
        """Remove zeros à esquerda do SKU para comparação flexível"""
        if pd.isna(sku_str) or sku_str is None:
            return ""
        sku_str = str(sku_str).strip()
        # Remover zeros à esquerda, mas manter pelo menos um dígito
        sku_normalizado = sku_str.lstrip('0') if sku_str.lstrip('0') else sku_str
        return sku_normalizado

    # CORREÇÃO CRÍTICA: Separar linhas TOTAL/ANTERIOR ANTES de qualquer processamento
    # Isso garante que estejam disponíveis mesmo quando não há pesquisa
    if 'tipo' in df_view.columns and 'sku' in df_view.columns:
        # Separar linhas TOTAL/ANTERIOR das movimentações
        mask_tipo_especial = df_view['tipo'].astype(str).str.strip().str.upper().isin(['TOTAL', 'TOTAL:', 'ANTERIOR', 'ANTERIOR:'])
        df_movimentacoes = df_view[~mask_tipo_especial].copy()
        df_total_anterior = df_view[mask_tipo_especial].copy()
    else:
        df_movimentacoes = df_view.copy()
        df_total_anterior = pd.DataFrame()

    # Aplica pesquisa global (afeta linhas, não colunas)
    # Se cols_filter estiver preenchido, pesquisa apenas nessas colunas
    # Se não, pesquisa em todas as colunas
    # IMPORTANTE: Se há pesquisa, incluir também linhas TOTAL/ANTERIOR do mesmo SKU que passou no filtro
    if txt_search:

        # Aplicar pesquisa nas movimentações (sem TOTAL/ANTERIOR)
        skus_filtrados_mov = set()
        if not df_movimentacoes.empty:
            # Normalizar SKU para busca flexível
            txt_search_normalizado = normalizar_sku(txt_search)
            if 'sku' in df_movimentacoes.columns:
                df_movimentacoes['sku_normalizado'] = df_movimentacoes['sku'].apply(normalizar_sku)

            # Verificar se a coluna SKU está no filtro
            filtro_sku_especifico = cols_filter and 'sku' in cols_filter

            if filtro_sku_especifico:
                # Quando a coluna SKU está selecionada, fazer busca EXATA normalizada
                # Isso evita pegar outros números parecidos (ex: "1002" não pega "10020", "10021", etc)
                if 'sku_normalizado' in df_movimentacoes.columns and txt_search_normalizado:
                    # Busca exata no SKU normalizado (ignorando zeros à esquerda)
                    mask_sku_exato = df_movimentacoes['sku_normalizado'] == txt_search_normalizado
                    # Também verificar se o SKU original corresponde exatamente
                    mask_sku_original_exato = df_movimentacoes['sku'].astype(str).str.strip() == str(txt_search).strip()
                    mask_movimentacoes = mask_sku_exato | mask_sku_original_exato
                else:
                    # Se não conseguiu normalizar, buscar exato no SKU original
                    mask_movimentacoes = df_movimentacoes['sku'].astype(str).str.strip() == str(txt_search).strip()
            elif cols_filter:
                # Quando outras colunas estão selecionadas (não SKU), fazer busca parcial normalizada
                # Se SKU está nas colunas selecionadas, usar busca exata normalizada para ele
                if 'sku' in cols_filter and 'sku_normalizado' in df_movimentacoes.columns and txt_search_normalizado:
                    # Para SKU: busca exata normalizada
                    mask_sku_exato = df_movimentacoes['sku_normalizado'] == txt_search_normalizado
                    mask_sku_original_exato = df_movimentacoes['sku'].astype(str).str.strip() == str(txt_search).strip()
                    mask_sku = mask_sku_exato | mask_sku_original_exato

                    # Para outras colunas: busca parcial
                    outras_cols = [c for c in cols_filter if c != 'sku']
                    if outras_cols:
                        mask_outras = df_movimentacoes[outras_cols].astype(str).apply(lambda x: x.str.contains(txt_search, case=False, na=False)).any(axis=1)
                        mask_movimentacoes = mask_sku | mask_outras
                    else:
                        mask_movimentacoes = mask_sku
                else:
                    # Busca parcial nas colunas selecionadas
                    mask_movimentacoes = df_movimentacoes[cols_filter].astype(str).apply(lambda x: x.str.contains(txt_search, case=False, na=False)).any(axis=1)
            else:
                # Busca em todas as colunas: para SKU usar busca exata normalizada, para outras usar busca parcial
                if 'sku_normalizado' in df_movimentacoes.columns and txt_search_normalizado:
                    # Para SKU: busca exata normalizada
                    mask_sku_exato = df_movimentacoes['sku_normalizado'] == txt_search_normalizado
                    mask_sku_original_exato = df_movimentacoes['sku'].astype(str).str.strip() == str(txt_search).strip()
                    mask_sku = mask_sku_exato | mask_sku_original_exato

                    # Para outras colunas: busca parcial
                    outras_cols = [c for c in df_movimentacoes.columns if c not in ['sku', 'sku_normalizado']]
                    if outras_cols:
                        mask_outras = df_movimentacoes[outras_cols].astype(str).apply(lambda x: x.str.contains(txt_search, case=False, na=False)).any(axis=1)
                        mask_movimentacoes = mask_sku | mask_outras
                    else:
                        mask_movimentacoes = mask_sku
                else:
                    # Busca parcial em todas as colunas
                    mask_movimentacoes = df_movimentacoes.astype(str).apply(lambda x: x.str.contains(txt_search, case=False, na=False)).any(axis=1)

            # Remover coluna temporária se existir
            if 'sku_normalizado' in df_movimentacoes.columns:
                df_movimentacoes = df_movimentacoes.drop(columns=['sku_normalizado'])

            # Identificar SKUs que passaram no filtro (apenas das movimentações)
            if 'sku' in df_movimentacoes.columns:
                skus_filtrados_mov = set(df_movimentacoes[mask_movimentacoes]['sku'].unique())

        # IMPORTANTE: Também pesquisar diretamente nas linhas TOTAL/ANTERIOR
        # Isso permite encontrar produtos que só têm TOTAL/ANTERIOR (sem movimentações no período)
        if not df_total_anterior.empty:
            # Normalizar SKU para busca flexível
            txt_search_normalizado_ta = normalizar_sku(txt_search)
            if 'sku' in df_total_anterior.columns:
                df_total_anterior['sku_normalizado'] = df_total_anterior['sku'].apply(normalizar_sku)

            # Verificar se a coluna SKU está no filtro
            filtro_sku_especifico_ta = cols_filter and 'sku' in cols_filter

            if filtro_sku_especifico_ta:
                # Quando a coluna SKU está selecionada, fazer busca EXATA normalizada
                if 'sku_normalizado' in df_total_anterior.columns and txt_search_normalizado_ta:
                    # Busca exata no SKU normalizado (ignorando zeros à esquerda)
                    mask_sku_exato_ta = df_total_anterior['sku_normalizado'] == txt_search_normalizado_ta
                    # Também verificar se o SKU original corresponde exatamente
                    mask_sku_original_exato_ta = df_total_anterior['sku'].astype(str).str.strip() == str(txt_search).strip()
                    mask_total_anterior = mask_sku_exato_ta | mask_sku_original_exato_ta
                else:
                    # Se não conseguiu normalizar, buscar exato no SKU original
                    mask_total_anterior = df_total_anterior['sku'].astype(str).str.strip() == str(txt_search).strip()
            elif cols_filter:
                # Quando outras colunas estão selecionadas (não SKU), fazer busca parcial normalizada
                # Se SKU está nas colunas selecionadas, usar busca exata normalizada para ele
                if 'sku' in cols_filter and 'sku_normalizado' in df_total_anterior.columns and txt_search_normalizado_ta:
                    # Para SKU: busca exata normalizada
                    mask_sku_exato_ta = df_total_anterior['sku_normalizado'] == txt_search_normalizado_ta
                    mask_sku_original_exato_ta = df_total_anterior['sku'].astype(str).str.strip() == str(txt_search).strip()
                    mask_sku_ta = mask_sku_exato_ta | mask_sku_original_exato_ta

                    # Para outras colunas: busca parcial
                    outras_cols_ta = [c for c in cols_filter if c != 'sku']
                    if outras_cols_ta:
                        mask_outras_ta = df_total_anterior[outras_cols_ta].astype(str).apply(lambda x: x.str.contains(txt_search, case=False, na=False)).any(axis=1)
                        mask_total_anterior = mask_sku_ta | mask_outras_ta
                    else:
                        mask_total_anterior = mask_sku_ta
                else:
                    # Busca parcial nas colunas selecionadas
                    mask_total_anterior = df_total_anterior[cols_filter].astype(str).apply(lambda x: x.str.contains(txt_search, case=False, na=False)).any(axis=1)
            else:
                # Busca em todas as colunas: para SKU usar busca exata normalizada, para outras usar busca parcial
                if 'sku_normalizado' in df_total_anterior.columns and txt_search_normalizado_ta:
                    # Para SKU: busca exata normalizada
                    mask_sku_exato_ta = df_total_anterior['sku_normalizado'] == txt_search_normalizado_ta
                    mask_sku_original_exato_ta = df_total_anterior['sku'].astype(str).str.strip() == str(txt_search).strip()
                    mask_sku_ta = mask_sku_exato_ta | mask_sku_original_exato_ta

                    # Para outras colunas: busca parcial
                    outras_cols_ta = [c for c in df_total_anterior.columns if c not in ['sku', 'sku_normalizado']]
                    if outras_cols_ta:
                        mask_outras_ta = df_total_anterior[outras_cols_ta].astype(str).apply(lambda x: x.str.contains(txt_search, case=False, na=False)).any(axis=1)
                        mask_total_anterior = mask_sku_ta | mask_outras_ta
                    else:
                        mask_total_anterior = mask_sku_ta
                else:
                    # Busca parcial em todas as colunas
                    mask_total_anterior = df_total_anterior.astype(str).apply(lambda x: x.str.contains(txt_search, case=False, na=False)).any(axis=1)

            # Remover coluna temporária se existir
            if 'sku_normalizado' in df_total_anterior.columns:
                df_total_anterior = df_total_anterior.drop(columns=['sku_normalizado'])

            # Identificar SKUs que passaram no filtro nas linhas TOTAL/ANTERIOR
            if 'sku' in df_total_anterior.columns:
                skus_filtrados_total_anterior = set(df_total_anterior[mask_total_anterior]['sku'].unique())

        # Unir todos os SKUs filtrados (de movimentações E de TOTAL/ANTERIOR)
        skus_filtrados = skus_filtrados_mov | skus_filtrados_total_anterior
    else:
        # Quando não há pesquisa, não há SKUs filtrados (todos serão incluídos)
        skus_filtrados = set()

    # Construir df_view final:
    # 1. Incluir movimentações que passaram no filtro
    # 2. Incluir linhas TOTAL/ANTERIOR dos SKUs filtrados (tanto das movimentações quanto das próprias linhas TOTAL/ANTERIOR)
    # CORREÇÃO: Se não há pesquisa, incluir TODAS as linhas TOTAL/ANTERIOR (produtos sem movimentação devem aparecer)
    df_view_parts = []

    # Adicionar movimentações
    if not df_movimentacoes.empty:
        if txt_search:
            # Se há pesquisa, filtrar movimentações
            if mask_movimentacoes is not None:
                df_movimentacoes_filtradas = df_movimentacoes[mask_movimentacoes]
                df_view_parts.append(df_movimentacoes_filtradas)
        else:
            # Se NÃO há pesquisa, incluir TODAS as movimentações
            df_view_parts.append(df_movimentacoes)

    # Adicionar linhas TOTAL/ANTERIOR
    if not df_total_anterior.empty and 'sku' in df_total_anterior.columns:
        if txt_search:
            # Se há pesquisa, incluir apenas linhas TOTAL/ANTERIOR dos SKUs filtrados
            if skus_filtrados:
                df_total_anterior_filtrado = df_total_anterior[df_total_anterior['sku'].isin(skus_filtrados)]
                df_view_parts.append(df_total_anterior_filtrado)
        else:
            # CORREÇÃO CRÍTICA: Se NÃO há pesquisa, incluir TODAS as linhas TOTAL/ANTERIOR
            # Isso garante que produtos sem movimentação no período apareçam sempre
            df_view_parts.append(df_total_anterior)

    # Concatenar todas as partes
    if df_view_parts:
        df_view = pd.concat(df_view_parts, ignore_index=True)
    else:
        df_view = pd.DataFrame(columns=df_view.columns)

        # DEBUG: Verificar se linhas ANTERIOR estão presentes após busca
        if txt_search and 'tipo' in df_view.columns:
            linhas_anterior_apos_busca = df_view[df_view['tipo'].astype(str).str.strip().str.upper().isin(['ANTERIOR', 'ANTERIOR:'])]
            if not linhas_anterior_apos_busca.empty:
                st.info(f"🔍 DEBUG: {len(linhas_anterior_apos_busca)} linhas ANTERIOR encontradas após busca por '{txt_search}'")
            else:
                st.warning(f"⚠️ DEBUG: Nenhuma linha ANTERIOR encontrada após busca por '{txt_search}'. SKUs filtrados: {sorted(skus_filtrados)}")

    # V190: Ordenar por SKU primeiro, depois por tipo (ANTERIOR primeiro, TOTAL por último, movimentações no meio)
    # Guarda df_view completo ANTES de ordenar para usar nos cálculos e cores
    df_view_antes_ordenacao = df_view.copy()
    # IMPORTANTE: Garantir que a ordenação seja aplicada ANTES de qualquer processamento adicional
    if 'sku' in df_view.columns and not df_view.empty:
        # Criar coluna de ordem para tipo (ANTERIOR=1, movimentações=2, TOTAL=3)
        if 'tipo' in df_view.columns:
            def ordem_tipo(tipo_val):
                tipo_upper = str(tipo_val).upper().strip()
                if tipo_upper in ['ANTERIOR', 'ANTERIOR:']:
                    return 1
                elif tipo_upper in ['TOTAL', 'TOTAL:']:
                    return 3
                else:
                    return 2

            df_view['_ordem_tipo'] = df_view['tipo'].apply(ordem_tipo)

            # Ordenar: SKU -> ordem_tipo -> data_movimento -> id
            cols_sort = ['sku', '_ordem_tipo']
            if 'data_movimento' in df_view.columns:
                cols_sort.append('data_movimento')
            if 'id' in df_view.columns:
                cols_sort.append('id')

            # V190: Garantir ordenação correta e reset de índices
            # Criar lista ascending com o mesmo tamanho de cols_sort
            ascending_list = [True] * len(cols_sort)
            df_view = df_view.sort_values(by=cols_sort, ascending=ascending_list, kind='stable').reset_index(drop=True)
            df_view = df_view.drop(columns=['_ordem_tipo'])
        else:
            # Se não tem tipo (dados do histórico), ordena por SKU -> data -> id
            cols_sort = ['sku']
            if 'data_movimento' in df_view.columns:
                cols_sort.append('data_movimento')
            if 'id' in df_view.columns:
                cols_sort.append('id')
            # V190: Garantir ordenação correta e reset de índices
            df_view = df_view.sort_values(by=cols_sort, ascending=True, kind='stable').reset_index(drop=True)

    # V191: Saldo Total será calculado DEPOIS da tabela ser exibida (ver código abaixo)

    # DEBUG TEMPORÁRIO: Mostrar informações após filtros (ANTES de criar linhas TOTAL)
    with st.expander("🔍 DEBUG - Estado do df_view APÓS filtros (antes de criar linhas TOTAL)", expanded=False):
        st.write(f"**🔍 Filtro de pesquisa ativo:** '{txt_search}'")
        if not txt_search:
            st.warning("⚠️ **Filtro vazio!** Para testar, digite algo no campo de pesquisa e clique em 'Consultar'.")
        st.write(f"**📋 Colunas filtradas:** {cols_filter}")
        st.write(f"**📊 Linhas no df_view (após TODOS os filtros):** {len(df_view)}")

        if 'sku' in df_view.columns:
            skus_filtrados = set(df_view['sku'].unique())
            st.write(f"**🔢 SKUs únicos no df_view (filtrado):** {len(skus_filtrados)}")
            if txt_search:
                st.write(f"**📝 SKUs filtrados (primeiros 20):** {sorted(list(skus_filtrados))[:20]}")
            else:
                st.write(f"**📝 Todos os SKUs (sem filtro, primeiros 20):** {sorted(list(skus_filtrados))[:20]}")

        if 'tipo' in df_view.columns:
            mask_total_debug = df_view['tipo'].astype(str).str.strip().str.upper().isin(['TOTAL', 'TOTAL:', 'TOTAL '])
            linhas_total_debug = df_view[mask_total_debug].copy() if mask_total_debug.any() else pd.DataFrame()
            num_linhas_total = len(linhas_total_debug)
            st.write(f"**✅ Linhas TOTAL encontradas no df_view (APÓS filtros, ANTES de criar novas):** {num_linhas_total}")
            if not linhas_total_debug.empty and 'sku' in linhas_total_debug.columns:
                skus_com_total = sorted(linhas_total_debug['sku'].unique().tolist())
                st.write(f"**📋 SKUs com TOTAL (total: {len(skus_com_total)}, primeiros 20):** {skus_com_total[:20]}")
                if 'produto' in linhas_total_debug.columns and 'saldo' in linhas_total_debug.columns:
                    st.write(f"**💰 Primeiras 10 linhas TOTAL com saldo:**")
                    for idx, row in linhas_total_debug.head(10).iterrows():
                        st.write(f"  - SKU {row['sku']} | {row['produto']}: {formatar_br(row.get('saldo', 0))}")
            elif num_linhas_total == 0:
                st.info("ℹ️ Nenhuma linha TOTAL encontrada ainda (serão criadas após a correção sequencial).")

    # V191: O cálculo do total será feito DEPOIS da correção sequencial dos saldos (ver código abaixo)
    # st.metric temporariamente removido - será exibido depois da correção sequencial

    # Tabela (v189: Inclui saldo_anterior, nota, serie e valores sempre visíveis)
    cols_table = ['data', 'sku', 'produto', 'categoria', 'tipo', 'nota', 'serie', 'saldo_anterior', 'entrada', 'saida', 'saldo', 'vlr_unit', 'vlr_total']

    # V189: Garantir que saldo_anterior sempre existe (criar com 0 se não existir)
    if 'saldo_anterior' not in df_view.columns:
        df_view['saldo_anterior'] = 0.0
    else:
        # Preenche NaN com 0 (saldo anterior pode ser zero, mas sempre deve aparecer)
        df_view['saldo_anterior'] = df_view['saldo_anterior'].fillna(0.0)

    # CORREÇÃO: Recalcular saldo corretamente de forma SEQUENCIAL (saldo acumulado)
    # IMPORTANTE: Cada linha deve usar o saldo da linha ANTERIOR (do mesmo SKU) como saldo_anterior
    # Exemplo: se temos 3 movimentações do mesmo produto:
    #   Linha 1: saldo = saldo_anterior_inicial + entrada - saida
    #   Linha 2: saldo = saldo_linha1 + entrada - saida
    #   Linha 3: saldo = saldo_linha2 + entrada - saida
    # Variável para preservar linhas TOTAL de produtos sem movimentação
    df_totais_produtos_sem_mov = pd.DataFrame()

    if 'entrada' in df_view.columns and 'saida' in df_view.columns and 'saldo' in df_view.columns and 'sku' in df_view.columns:
        # IMPORTANTE: Separar linhas TOTAL antes do cálculo sequencial
        # As linhas TOTAL serão recriadas depois com os saldos corretos
        # MAS: Preservar linhas TOTAL de produtos que NÃO têm movimentações (com ou sem ANTERIOR)
        df_totais_originais = pd.DataFrame()
        if 'tipo' in df_view.columns and 'sku' in df_view.columns:
            mask_total = df_view['tipo'].astype(str).str.strip().str.upper().isin(['TOTAL', 'TOTAL:', 'TOTAL '])
            if mask_total.any():
                df_totais_originais = df_view[mask_total].copy()

                # Identificar produtos que só têm TOTAL/ANTERIOR (sem movimentações)
                # CORREÇÃO: Incluir produtos que só têm TOTAL (sem ANTERIOR também)
                mask_anterior = df_view['tipo'].astype(str).str.strip().str.upper().isin(['ANTERIOR', 'ANTERIOR:'])
                skus_com_total = set(df_totais_originais['sku'].dropna().unique())

                # Verificar quais SKUs têm movimentações (não são TOTAL nem ANTERIOR)
                mask_mov = ~mask_total & ~mask_anterior
                skus_com_mov = set(df_view[mask_mov]['sku'].dropna().unique())

                # Produtos que têm TOTAL mas NÃO têm movimentações (podem ter ou não ANTERIOR)
                skus_sem_mov_real = skus_com_total - skus_com_mov

                if skus_sem_mov_real:
                    # Preservar linhas TOTAL desses produtos (mesmo que não tenham ANTERIOR)
                    df_totais_produtos_sem_mov = df_totais_originais[df_totais_originais['sku'].isin(skus_sem_mov_real)].copy()
                    # Remover apenas linhas TOTAL de produtos com movimentações
                    df_view = df_view[~mask_total | df_view['sku'].isin(skus_sem_mov_real)].copy()
                else:
                    # Remover todas as linhas TOTAL
                    df_view = df_view[~mask_total].copy()

        # Converter para numérico
        df_view['entrada_num'] = pd.to_numeric(df_view['entrada'], errors='coerce').fillna(0.0)
        df_view['saida_num'] = pd.to_numeric(df_view['saida'], errors='coerce').fillna(0.0)
        df_view['saldo_anterior_num'] = pd.to_numeric(df_view['saldo_anterior'], errors='coerce').fillna(0.0)

        # Ordenar por SKU e data para calcular sequencialmente
        # IMPORTANTE: Guardar índice original ANTES de ordenar para mapear de volta depois
        df_view_temp = df_view.copy()
        # Criar coluna com índice original ANTES de qualquer ordenação
        # Guarda o índice original como valores na coluna '_idx_original'
        df_view_temp['_idx_original'] = df_view.index.values

        if 'data_movimento' in df_view_temp.columns:
            df_view_temp['data_mov_dt'] = pd.to_datetime(df_view_temp['data_movimento'], errors='coerce')
            cols_sort = ['sku', 'data_mov_dt']
            if 'id' in df_view_temp.columns:
                cols_sort.append('id')
            df_view_temp = df_view_temp.sort_values(by=cols_sort, ascending=True, na_position='last')
            df_view_temp = df_view_temp.drop(columns=['data_mov_dt'])
        elif 'id' in df_view_temp.columns:
            df_view_temp = df_view_temp.sort_values(by=['sku', 'id'], ascending=True, na_position='last')
        else:
            df_view_temp = df_view_temp.sort_values(by=['sku'], ascending=True, na_position='last')

        # Resetar índice para ter índice numérico sequencial (0, 1, 2...) para iterar facilmente
        # O índice original está guardado na coluna '_idx_original'
        df_view_temp = df_view_temp.reset_index(drop=True)

        # Calcular saldo acumulado sequencialmente por SKU
        ultimo_saldo_por_sku = {}  # {sku: ultimo_saldo}
        # Coluna para armazenar saldo_anterior atualizado (para exibição)
        df_view_temp['saldo_anterior_atualizado'] = df_view_temp['saldo_anterior_num'].copy()

        for idx in df_view_temp.index:
            sku_atual = df_view_temp.at[idx, 'sku'] if pd.notna(df_view_temp.at[idx, 'sku']) else None
            tipo_atual = None
            if 'tipo' in df_view_temp.columns:
                tipo_atual = str(df_view_temp.at[idx, 'tipo']).strip().upper() if pd.notna(df_view_temp.at[idx, 'tipo']) else ''

            # Se é linha ANTERIOR, guarda o saldo_anterior como último saldo
            if tipo_atual in ['ANTERIOR', 'ANTERIOR:']:
                saldo_ant = df_view_temp.at[idx, 'saldo_anterior_num']
                # AVISO: O saldo_anterior pode estar incorreto no HTML
                # Se houver primeira movimentação, podemos validar/ajustar baseado nela
                if sku_atual:
                    ultimo_saldo_por_sku[sku_atual] = saldo_ant
                df_view_temp.at[idx, 'saldo'] = saldo_ant  # Saldo ANTERIOR é o próprio saldo_anterior

            # Linhas TOTAL não devem estar aqui (já foram removidas antes do cálculo sequencial)
            # Se aparecer uma linha TOTAL aqui, é um erro - pular
            elif tipo_atual in ['TOTAL', 'TOTAL:']:
                # Não deveria chegar aqui, mas se chegar, pular
                pass

            # Se é movimentação, recalcula usando o último saldo do SKU
            else:
                if sku_atual and sku_atual in ultimo_saldo_por_sku:
                    saldo_ant_atual = ultimo_saldo_por_sku[sku_atual]
                else:
                    # Se não tem último saldo, usa o saldo_anterior da linha
                    saldo_ant_atual = df_view_temp.at[idx, 'saldo_anterior_num']

                # Atualizar saldo_anterior exibido para mostrar o saldo da linha anterior
                df_view_temp.at[idx, 'saldo_anterior_atualizado'] = saldo_ant_atual

                # Recalcula: saldo = saldo_anterior + entrada - saida
                entrada_val = df_view_temp.at[idx, 'entrada_num']
                saida_val = df_view_temp.at[idx, 'saida_num']
                novo_saldo = saldo_ant_atual + entrada_val - saida_val

                df_view_temp.at[idx, 'saldo'] = novo_saldo

                # Atualiza o último saldo deste SKU para a próxima linha
                if sku_atual:
                    ultimo_saldo_por_sku[sku_atual] = novo_saldo

        # Atualizar df_view com os saldos recalculados usando o índice original
        # Criar um mapeamento de índice original para novo saldo
        mapa_saldos = df_view_temp.set_index('_idx_original')['saldo'].to_dict()
        # Atualizar df_view usando o mapeamento
        # Se algum índice não estiver no mapa, manter o valor original
        saldos_atualizados = df_view.index.map(mapa_saldos)
        df_view.loc[saldos_atualizados.notna(), 'saldo'] = saldos_atualizados[saldos_atualizados.notna()]

        # Atualizar também o saldo_anterior exibido: mapear de volta
        mapa_saldo_ant = df_view_temp.set_index('_idx_original')['saldo_anterior_atualizado'].to_dict()
        saldo_ant_atualizado = df_view.index.map(mapa_saldo_ant)
        df_view.loc[saldo_ant_atualizado.notna(), 'saldo_anterior'] = saldo_ant_atualizado[saldo_ant_atualizado.notna()]

        df_view = df_view.drop(columns=['saldo_anterior_num', 'entrada_num', 'saida_num'])

    # V191: Criar linhas ANTERIOR e TOTAL para cada produto (se não existirem) DEPOIS da correção sequencial
    # CRÍTICO: Criar essas linhas DEPOIS de todos os filtros e correção sequencial para garantir que use apenas produtos visíveis
    # CORREÇÃO v191: Verificar por PRODUTO se tem linha ANTERIOR/TOTAL, não globalmente

    if 'saldo' in df_view.columns and not df_view.empty and 'sku' in df_view.columns:
        # Criar grupo por SKU+Produto para verificar por produto
        if 'produto' in df_view.columns:
            df_view['sku_produto'] = df_view['sku'].astype(str) + ' | ' + df_view['produto'].astype(str)
            grupo_col = 'sku_produto'
        else:
            grupo_col = 'sku'

        # Verificar quais produtos JÁ TÊM linhas ANTERIOR e TOTAL explícitas
        produtos_com_anterior = set()
        produtos_com_total = set()
        if 'tipo' in df_view.columns:
            mask_anterior_existente = df_view['tipo'].astype(str).str.strip().str.upper().isin(['ANTERIOR', 'ANTERIOR:'])
            mask_total_existente = df_view['tipo'].astype(str).str.strip().str.upper().isin(['TOTAL', 'TOTAL:', 'TOTAL '])
            if mask_anterior_existente.any():
                produtos_com_anterior = set(df_view[mask_anterior_existente][grupo_col].unique())
            if mask_total_existente.any():
                produtos_com_total = set(df_view[mask_total_existente][grupo_col].unique())

        # Obter lista de todos os produtos únicos no df_view (após filtros)
        produtos_unicos = set(df_view[grupo_col].unique())

        # V191: Remover linhas TOTAL existentes do df_view para recalcular entrada/saída corretamente
        # MAS: Manter linhas TOTAL de produtos que só têm ANTERIOR e TOTAL (sem movimentações)
        # IMPORTANTE: Produtos sem movimentação criados em app.py já têm TOTAL, então não devem ser removidos
        # CORREÇÃO: Garantir que apenas produtos visíveis (após filtros) sejam considerados
        if produtos_com_total and 'tipo' in df_view.columns:
            # CORREÇÃO: Filtrar produtos_com_total para incluir apenas produtos que estão no df_view (já filtrado)
            # Isso garante que apenas produtos visíveis após filtros sejam considerados
            produtos_com_total = produtos_com_total.intersection(produtos_unicos)

            # Identificar produtos que só têm ANTERIOR e TOTAL (sem movimentações)
            produtos_so_anterior_total = set()
            for produto in produtos_com_total:
                df_produto = df_view[df_view[grupo_col] == produto].copy()
                if 'tipo' in df_produto.columns:
                    tipos_produto = set(df_produto['tipo'].astype(str).str.strip().str.upper().unique())
                    # Se só tem ANTERIOR e TOTAL (sem movimentações)
                    if tipos_produto.issubset({'ANTERIOR', 'ANTERIOR:', 'TOTAL', 'TOTAL:', 'TOTAL '}):
                        produtos_so_anterior_total.add(produto)

            # Remover apenas linhas TOTAL de produtos que TÊM movimentações
            # CORREÇÃO: Garantir que apenas produtos que estão em produtos_com_total (já filtrado) sejam considerados
            produtos_com_movimentacoes = produtos_com_total - produtos_so_anterior_total
            if produtos_com_movimentacoes:
                # CORREÇÃO: Remover linhas TOTAL apenas de produtos que estão em produtos_com_movimentacoes
                # e que também estão em produtos_unicos (garantindo que são produtos visíveis)
                produtos_com_movimentacoes = produtos_com_movimentacoes.intersection(produtos_unicos)
                if produtos_com_movimentacoes:
                    mask_total_para_remover = (df_view['tipo'].astype(str).str.strip().str.upper().isin(['TOTAL', 'TOTAL:', 'TOTAL '])) & (df_view[grupo_col].isin(produtos_com_movimentacoes))
                    df_view = df_view[~mask_total_para_remover].copy()

        # Criar linhas ANTERIOR para produtos que NÃO TÊM
        produtos_sem_anterior = produtos_unicos - produtos_com_anterior
        # Inicializar df_calc_anterior como DataFrame vazio
        df_calc_anterior = pd.DataFrame()

        if produtos_sem_anterior:
            # IMPORTANTE: Verificar se há produtos que só têm TOTAL (sem movimentações e sem ANTERIOR)
            # Nesse caso, usar a linha TOTAL para criar a linha ANTERIOR
            produtos_so_total = set()
            for produto in produtos_sem_anterior:
                df_produto = df_view[df_view[grupo_col] == produto].copy()
                if 'tipo' in df_produto.columns:
                    tipos_produto = set(df_produto['tipo'].astype(str).str.strip().str.upper().unique())
                    # Se só tem TOTAL (sem movimentações e sem ANTERIOR)
                    if tipos_produto.issubset({'TOTAL', 'TOTAL:', 'TOTAL '}):
                        produtos_so_total.add(produto)

            # Para produtos que só têm TOTAL, criar linha ANTERIOR usando o saldo da linha TOTAL
            if produtos_so_total:
                for produto in produtos_so_total:
                    df_produto = df_view[df_view[grupo_col] == produto].copy()
                    mask_total_prod = df_produto['tipo'].astype(str).str.strip().str.upper().isin(['TOTAL', 'TOTAL:', 'TOTAL '])
                    if mask_total_prod.any():
                        linha_total_prod = df_produto[mask_total_prod].iloc[0]
                        # Criar linha ANTERIOR
                        linha_anterior = {}
                        for col in df_view.columns:
                            linha_anterior[col] = linha_total_prod[col] if col in linha_total_prod.index else None
                        linha_anterior['tipo'] = 'ANTERIOR'
                        linha_anterior['entrada'] = 0.0
                        linha_anterior['saida'] = 0.0
                        # O saldo da linha ANTERIOR é o saldo_anterior (que deve ser o saldo da linha TOTAL)
                        saldo_ant_valor = linha_total_prod.get('saldo_anterior', linha_total_prod.get('saldo', 0.0))
                        if pd.isna(saldo_ant_valor) or saldo_ant_valor is None:
                            saldo_ant_valor = float(linha_total_prod.get('saldo', 0.0))
                        else:
                            saldo_ant_valor = float(saldo_ant_valor)
                        linha_anterior['saldo'] = saldo_ant_valor
                        linha_anterior['saldo_anterior'] = saldo_ant_valor
                        linha_anterior['data_movimento'] = None
                        linha_anterior['data'] = None
                        # Adicionar ao df_view
                        df_view = pd.concat([df_view, pd.DataFrame([linha_anterior])], ignore_index=True)
                        produtos_com_anterior.add(produto)  # Marcar como tendo ANTERIOR agora

            # Remover produtos que já têm ANTERIOR da lista de produtos sem ANTERIOR
            produtos_sem_anterior = produtos_sem_anterior - produtos_com_anterior

            # CORREÇÃO: Criar linhas TOTAL para produtos que só têm ANTERIOR (sem movimentações)
            # Esses produtos precisam ter linha TOTAL criada para aparecerem corretamente
            produtos_so_anterior = set()
            for produto in produtos_com_anterior:
                df_produto = df_view[df_view[grupo_col] == produto].copy()
                if 'tipo' in df_produto.columns:
                    tipos_produto = set(df_produto['tipo'].astype(str).str.strip().str.upper().unique())
                    # Se só tem ANTERIOR (sem movimentações e sem TOTAL)
                    if tipos_produto.issubset({'ANTERIOR', 'ANTERIOR:'}):
                        produtos_so_anterior.add(produto)

            # Criar linhas TOTAL para produtos que só têm ANTERIOR
            if produtos_so_anterior:
                linhas_total_anterior = []
                for produto in produtos_so_anterior:
                    df_produto = df_view[df_view[grupo_col] == produto].copy()
                    mask_anterior_prod = df_produto['tipo'].astype(str).str.strip().str.upper().isin(['ANTERIOR', 'ANTERIOR:'])
                    if mask_anterior_prod.any():
                        linha_anterior_prod = df_produto[mask_anterior_prod].iloc[0]
                        # Criar linha TOTAL baseada na linha ANTERIOR
                        linha_total_anterior = {}
                        for col in df_view.columns:
                            linha_total_anterior[col] = linha_anterior_prod[col] if col in linha_anterior_prod.index else None
                        linha_total_anterior['tipo'] = 'TOTAL'
                        linha_total_anterior['entrada'] = 0.0
                        linha_total_anterior['saida'] = 0.0
                        # O saldo da linha TOTAL é o mesmo da linha ANTERIOR (sem movimentações)
                        saldo_total_valor = linha_anterior_prod.get('saldo', 0.0)
                        if pd.isna(saldo_total_valor) or saldo_total_valor is None:
                            saldo_total_valor = 0.0
                        else:
                            saldo_total_valor = float(saldo_total_valor)
                        linha_total_anterior['saldo'] = saldo_total_valor
                        linha_total_anterior['saldo_anterior'] = saldo_total_valor
                        linha_total_anterior['data_movimento'] = None
                        linha_total_anterior['data'] = None
                        linhas_total_anterior.append(linha_total_anterior)

                # Adicionar linhas TOTAL ao df_view
                if linhas_total_anterior:
                    df_totais_anterior_append = pd.DataFrame(linhas_total_anterior)
                    df_view = pd.concat([df_view, df_totais_anterior_append], ignore_index=True)
                    # Marcar esses produtos como tendo TOTAL agora
                    produtos_com_total.update(produtos_so_anterior)

            # Pegar a primeira movimentação de cada produto que NÃO TEM linha ANTERIOR para criar linha ANTERIOR
            if produtos_sem_anterior:
                df_calc_anterior = df_view[df_view[grupo_col].isin(produtos_sem_anterior)].copy()
                # Excluir linhas TOTAL se houver
                if 'tipo' in df_calc_anterior.columns:
                    mask_total = df_calc_anterior['tipo'].astype(str).str.strip().str.upper().isin(['TOTAL', 'TOTAL:'])
                    df_calc_anterior = df_calc_anterior[~mask_total].copy()

        if not df_calc_anterior.empty and produtos_sem_anterior:
                # Usar grupo_col já definido (sku_produto ou sku)
                # Ordenar para pegar a primeira linha de cada produto
                if 'data_movimento' in df_calc_anterior.columns:
                    df_calc_anterior['data_mov_dt'] = pd.to_datetime(df_calc_anterior['data_movimento'], errors='coerce')
                    cols_ord = [grupo_col, 'data_mov_dt']
                    if 'id' in df_calc_anterior.columns:
                        cols_ord.append('id')
                    df_calc_anterior = df_calc_anterior.sort_values(by=cols_ord, ascending=True, na_position='last')
                    df_calc_anterior = df_calc_anterior.drop(columns=['data_mov_dt'])
                elif 'id' in df_calc_anterior.columns:
                    df_calc_anterior = df_calc_anterior.sort_values(by=[grupo_col, 'id'], ascending=True, na_position='last')

                # Pegar primeira linha de cada produto (que tem o saldo_anterior inicial)
                primeiras_linhas = df_calc_anterior.groupby(grupo_col, sort=False).first().reset_index()

                # Criar linhas ANTERIOR
                linhas_anterior = []
                for idx, row in primeiras_linhas.iterrows():
                    # Converter Series para dict preservando todos os valores
                    linha_anterior = {}
                    for col in df_calc_anterior.columns:
                        linha_anterior[col] = row[col] if col in row.index else None

                    linha_anterior['tipo'] = 'ANTERIOR'
                    linha_anterior['entrada'] = 0.0
                    linha_anterior['saida'] = 0.0
                    # O saldo da linha ANTERIOR é o saldo_anterior da primeira movimentação
                    saldo_ant_valor = linha_anterior.get('saldo_anterior', 0.0)
                    if pd.isna(saldo_ant_valor) or saldo_ant_valor is None:
                        saldo_ant_valor = 0.0
                    else:
                        saldo_ant_valor = float(saldo_ant_valor)
                    linha_anterior['saldo'] = saldo_ant_valor
                    linha_anterior['saldo_anterior'] = saldo_ant_valor  # ANTERIOR não tem saldo anterior (é o inicial)
                    linha_anterior['data_movimento'] = None  # ANTERIOR não tem data
                    linha_anterior['data'] = None  # ANTERIOR não tem data
                    linhas_anterior.append(linha_anterior)

                # Adicionar linhas ANTERIOR ao df_view
                if linhas_anterior:
                    df_anteriores_append = pd.DataFrame(linhas_anterior)
                    df_view = pd.concat([df_view, df_anteriores_append], ignore_index=True)

        # V191: Criar/Recriar linhas TOTAL para TODOS os produtos (para recalcular entrada/saída corretamente)
        # Pegar o último saldo de cada produto (df_view já não tem mais linhas TOTAL antigas de produtos com movimentações)
        # IMPORTANTE: Produtos que só têm ANTERIOR e TOTAL já têm suas linhas TOTAL preservadas acima
        df_calc_totais = df_view.copy()
        # Excluir linhas ANTERIOR se houver
        if 'tipo' in df_calc_totais.columns:
            mask_anterior = df_calc_totais['tipo'].astype(str).str.strip().str.upper().isin(['ANTERIOR', 'ANTERIOR:'])
            df_calc_totais = df_calc_totais[~mask_anterior].copy()

        # V191: Produtos sem movimentação já têm linhas TOTAL criadas em app.py
        # Não precisamos criar novamente aqui para evitar duplicação
        # Se df_calc_totais estiver vazio, significa que só há linhas ANTERIOR e TOTAL
        # (que já foram criadas corretamente em app.py)

        if not df_calc_totais.empty:
            # V191: Verificar se df_calc_totais contém apenas linhas TOTAL (produtos sem movimentação)
            # Se sim, não criar novas linhas TOTAL (já existem e foram preservadas)
            if 'tipo' in df_calc_totais.columns:
                tipos_calc = set(df_calc_totais['tipo'].astype(str).str.strip().str.upper().unique())
                # Se só tem TOTAL (sem movimentações reais), não criar novas linhas TOTAL
                if tipos_calc.issubset({'TOTAL', 'TOTAL:', 'TOTAL ', '', 'NAN', 'NONE'}):
                    # df_calc_totais contém apenas linhas TOTAL preservadas, não criar novas
                    pass
                else:
                    # Há movimentações reais, então criar/recriar linhas TOTAL
                    # Usar grupo_col já definido (sku_produto ou sku)
                    # Ordenar para pegar o último saldo de cada produto
                    if 'data_movimento' in df_calc_totais.columns:
                        df_calc_totais['data_mov_dt'] = pd.to_datetime(df_calc_totais['data_movimento'], errors='coerce')
                        cols_ord = [grupo_col, 'data_mov_dt']
                        if 'id' in df_calc_totais.columns:
                            cols_ord.append('id')
                        df_calc_totais = df_calc_totais.sort_values(by=cols_ord, ascending=True, na_position='last')
                        df_calc_totais = df_calc_totais.drop(columns=['data_mov_dt'])
                    elif 'id' in df_calc_totais.columns:
                        df_calc_totais = df_calc_totais.sort_values(by=[grupo_col, 'id'], ascending=True, na_position='last')

                    # V191: Excluir linhas TOTAL existentes de df_calc_totais para pegar apenas movimentações
                    df_calc_totais_sem_total = df_calc_totais[~df_calc_totais['tipo'].astype(str).str.strip().str.upper().isin(['TOTAL', 'TOTAL:', 'TOTAL '])].copy()

                    # CORREÇÃO CRÍTICA: Filtrar df_calc_totais_sem_total para incluir apenas produtos visíveis (após filtros)
                    # Isso garante que apenas linhas TOTAL dos produtos filtrados sejam criadas
                    if grupo_col in df_calc_totais_sem_total.columns:
                        df_calc_totais_sem_total = df_calc_totais_sem_total[df_calc_totais_sem_total[grupo_col].isin(produtos_unicos)].copy()

                    if not df_calc_totais_sem_total.empty:
                        # Pegar última linha de cada produto VISÍVEL (que tem o saldo final após correção sequencial)
                        # IMPORTANTE: groupby().last() pega a última linha baseada na ordem atual (já ordenada por grupo_col e data)
                        ultimas_linhas = df_calc_totais_sem_total.groupby(grupo_col, sort=False).last().reset_index()

                        # Criar linhas TOTAL para cada produto VISÍVEL após filtros
                        linhas_total = []
                        for idx, row in ultimas_linhas.iterrows():
                            # Identificar o produto (SKU ou SKU+Produto)
                            sku_produto_atual = row[grupo_col] if grupo_col in row.index else row.get('sku', '')

                            # V191: Somar entrada e saida APENAS das movimentações do período (excluir ANTERIOR e TOTAL)
                            # IMPORTANTE: Usar df_calc_totais_sem_total que JÁ exclui linhas ANTERIOR e TOTAL
                            movimentos_produto = df_calc_totais_sem_total[df_calc_totais_sem_total[grupo_col] == sku_produto_atual].copy()

                            entrada_total = 0.0
                            saida_total = 0.0
                            if not movimentos_produto.empty and 'entrada' in movimentos_produto.columns and 'saida' in movimentos_produto.columns:
                                entrada_total = pd.to_numeric(movimentos_produto['entrada'], errors='coerce').fillna(0.0).sum()
                                saida_total = pd.to_numeric(movimentos_produto['saida'], errors='coerce').fillna(0.0).sum()

                            # Converter Series para dict preservando todos os valores
                            linha_total = {}
                            for col in df_calc_totais_sem_total.columns:
                                linha_total[col] = row[col] if col in row.index else None

                            linha_total['tipo'] = 'TOTAL'
                            linha_total['entrada'] = float(entrada_total)  # V191: Soma das entradas do período
                            linha_total['saida'] = float(saida_total)  # V191: Soma das saídas do período
                            linha_total['saldo_anterior'] = 0.0
                            # V191: O saldo da linha TOTAL é o último saldo calculado sequencialmente (já está correto)
                            saldo_valor = linha_total.get('saldo', 0.0)
                            if pd.isna(saldo_valor) or saldo_valor is None:
                                saldo_valor = 0.0
                            else:
                                saldo_valor = float(saldo_valor)
                            linha_total['saldo'] = saldo_valor
                            # Usar a última data do produto como data da linha TOTAL
                            linha_total['data_movimento'] = row['data_movimento'] if 'data_movimento' in row.index and pd.notna(row.get('data_movimento')) else None
                            linha_total['data'] = row['data'] if 'data' in row.index and pd.notna(row.get('data')) else None
                            linhas_total.append(linha_total)

                        # Adicionar linhas TOTAL ao df_view
                        if linhas_total:
                            df_totais_append = pd.DataFrame(linhas_total)
                            df_view = pd.concat([df_view, df_totais_append], ignore_index=True)

                            # CORREÇÃO: Filtrar df_totais_produtos_sem_mov para excluir produtos que já têm TOTAL criada
                            # Isso evita duplicatas
                            if not df_totais_produtos_sem_mov.empty:
                                # Criar grupo_col em df_totais_produtos_sem_mov se não existir
                                if grupo_col not in df_totais_produtos_sem_mov.columns:
                                    if 'produto' in df_totais_produtos_sem_mov.columns:
                                        df_totais_produtos_sem_mov['sku_produto'] = df_totais_produtos_sem_mov['sku'].astype(str) + ' | ' + df_totais_produtos_sem_mov['produto'].astype(str)
                                        grupo_col_temp = 'sku_produto'
                                    else:
                                        grupo_col_temp = 'sku'
                                else:
                                    grupo_col_temp = grupo_col

                                # Identificar produtos que já têm TOTAL criada
                                produtos_com_total_criada = set(df_totais_append[grupo_col].unique())

                                # Filtrar df_totais_produtos_sem_mov para excluir esses produtos
                                df_totais_produtos_sem_mov_filtrado = df_totais_produtos_sem_mov[~df_totais_produtos_sem_mov[grupo_col_temp].isin(produtos_com_total_criada)].copy()

                                # Adicionar apenas linhas TOTAL de produtos que NÃO tiveram TOTAL criada
                                if not df_totais_produtos_sem_mov_filtrado.empty:
                                    df_view = pd.concat([df_view, df_totais_produtos_sem_mov_filtrado], ignore_index=True)
                        else:
                            # Se não criou nenhuma linha TOTAL, adicionar todas as preservadas
                            if not df_totais_produtos_sem_mov.empty:
                                df_view = pd.concat([df_view, df_totais_produtos_sem_mov], ignore_index=True)

        # CORREÇÃO FINAL: Garantir que produtos que só têm ANTERIOR também tenham TOTAL
        # Isso garante que apareçam corretamente na primeira renderização
        # IMPORTANTE: Fazer isso ANTES da ordenação final para que as linhas sejam ordenadas juntas
        if 'tipo' in df_view.columns and 'sku' in df_view.columns and not df_view.empty:
            # Recriar grupo_col se necessário
            if 'produto' in df_view.columns:
                if 'sku_produto' not in df_view.columns:
                    df_view['sku_produto'] = df_view['sku'].astype(str) + ' | ' + df_view['produto'].astype(str)
                grupo_col_final = 'sku_produto'
            else:
                grupo_col_final = 'sku'

            # Identificar produtos que têm ANTERIOR mas não têm TOTAL
            mask_anterior_final_check = df_view['tipo'].astype(str).str.strip().str.upper().isin(['ANTERIOR', 'ANTERIOR:'])
            mask_total_final_check = df_view['tipo'].astype(str).str.strip().str.upper().isin(['TOTAL', 'TOTAL:', 'TOTAL '])
            mask_mov_final_check = ~mask_anterior_final_check & ~mask_total_final_check

            if mask_anterior_final_check.any():
                produtos_com_anterior_final_check = set(df_view[mask_anterior_final_check][grupo_col_final].unique())
                produtos_com_total_final_check = set(df_view[mask_total_final_check][grupo_col_final].unique()) if mask_total_final_check.any() else set()
                produtos_com_mov_final_check = set(df_view[mask_mov_final_check][grupo_col_final].unique()) if mask_mov_final_check.any() else set()

                # Produtos que têm ANTERIOR mas não têm TOTAL nem movimentações
                produtos_sem_total_final = produtos_com_anterior_final_check - produtos_com_total_final_check - produtos_com_mov_final_check

                if produtos_sem_total_final:
                    linhas_total_final_check = []
                    for produto in produtos_sem_total_final:
                        df_produto_final = df_view[df_view[grupo_col_final] == produto].copy()
                        mask_anterior_prod_final_check = df_produto_final['tipo'].astype(str).str.strip().str.upper().isin(['ANTERIOR', 'ANTERIOR:'])
                        if mask_anterior_prod_final_check.any():
                            linha_anterior_prod_final_check = df_produto_final[mask_anterior_prod_final_check].iloc[0]
                            # Criar linha TOTAL baseada na linha ANTERIOR
                            linha_total_final_check = {}
                            for col in df_view.columns:
                                if col != 'sku_produto':  # Não copiar sku_produto, será recriado se necessário
                                    linha_total_final_check[col] = linha_anterior_prod_final_check[col] if col in linha_anterior_prod_final_check.index else None
                            linha_total_final_check['tipo'] = 'TOTAL'
                            linha_total_final_check['entrada'] = 0.0
                            linha_total_final_check['saida'] = 0.0
                            # O saldo da linha TOTAL é o mesmo da linha ANTERIOR (sem movimentações)
                            saldo_total_final_check = linha_anterior_prod_final_check.get('saldo', 0.0)
                            if pd.isna(saldo_total_final_check) or saldo_total_final_check is None:
                                saldo_total_final_check = 0.0
                            else:
                                saldo_total_final_check = float(saldo_total_final_check)
                            linha_total_final_check['saldo'] = saldo_total_final_check
                            linha_total_final_check['saldo_anterior'] = saldo_total_final_check
                            linha_total_final_check['data_movimento'] = None
                            linha_total_final_check['data'] = None
                            linhas_total_final_check.append(linha_total_final_check)

                    # Adicionar linhas TOTAL ao df_view
                    if linhas_total_final_check:
                        df_totais_final_check_append = pd.DataFrame(linhas_total_final_check)
                        # Recriar sku_produto se necessário
                        if 'sku_produto' in df_view.columns and 'sku_produto' not in df_totais_final_check_append.columns:
                            if 'produto' in df_totais_final_check_append.columns:
                                df_totais_final_check_append['sku_produto'] = df_totais_final_check_append['sku'].astype(str) + ' | ' + df_totais_final_check_append['produto'].astype(str)
                        df_view = pd.concat([df_view, df_totais_final_check_append], ignore_index=True)
                        # NÃO reordenar aqui - será feito na ordenação final após todas as operações

        # CORREÇÃO: Remover duplicatas de linhas TOTAL por SKU
        # Garantir que cada produto tenha apenas UMA linha TOTAL
        if 'tipo' in df_view.columns and 'sku' in df_view.columns:
            mask_total = df_view['tipo'].astype(str).str.strip().str.upper().isin(['TOTAL', 'TOTAL:', 'TOTAL '])
            if mask_total.any():
                # Separar linhas TOTAL das outras
                df_totais_duplicados = df_view[mask_total].copy()
                df_view_sem_total = df_view[~mask_total].copy()

                # Remover duplicatas de linhas TOTAL, mantendo apenas a última (ou a primeira, dependendo da lógica)
                # Usar drop_duplicates com subset=['sku'] ou ['sku', 'produto'] se existir
                if 'produto' in df_totais_duplicados.columns:
                    # Se tem produto, usar sku+produto para identificar duplicatas
                    df_totais_unicos = df_totais_duplicados.drop_duplicates(subset=['sku', 'produto'], keep='last')
                else:
                    # Se não tem produto, usar apenas sku
                    df_totais_unicos = df_totais_duplicados.drop_duplicates(subset=['sku'], keep='last')

                # Recombinar
                if not df_view_sem_total.empty and not df_totais_unicos.empty:
                    df_view = pd.concat([df_view_sem_total, df_totais_unicos], ignore_index=True)
                elif not df_totais_unicos.empty:
                    df_view = df_totais_unicos.copy()
                elif not df_view_sem_total.empty:
                    df_view = df_view_sem_total.copy()

        # ORDENAÇÃO FINAL: Garantir que linhas do mesmo produto fiquem sempre juntas
        # ANTERIOR primeiro, movimentações no meio, TOTAL por último
        # IMPORTANTE: Fazer isso DEPOIS de todas as criações e remoções de duplicatas
        if 'tipo' in df_view.columns and 'sku' in df_view.columns and not df_view.empty:
            def ordem_tipo_final_completa(tipo_val):
                tipo_upper = str(tipo_val).upper().strip()
                if tipo_upper in ['ANTERIOR', 'ANTERIOR:']:
                    return 1
                elif tipo_upper in ['TOTAL', 'TOTAL:', 'TOTAL ']:
                    return 3
                else:
                    return 2
            df_view['_ordem_tipo_final_completa'] = df_view['tipo'].apply(ordem_tipo_final_completa)
            cols_sort_final_completa = ['sku', '_ordem_tipo_final_completa']
            if 'data_movimento' in df_view.columns:
                df_view['data_mov_dt_final_completa'] = pd.to_datetime(df_view['data_movimento'], errors='coerce').fillna(pd.Timestamp('1900-01-01'))
                cols_sort_final_completa.append('data_mov_dt_final_completa')
            if 'id' in df_view.columns:
                cols_sort_final_completa.append('id')
            # Ordenar de forma estável para manter a ordem relativa
            df_view = df_view.sort_values(by=cols_sort_final_completa, ascending=True, kind='stable').reset_index(drop=True)
            # Limpar colunas temporárias
            cols_to_drop = ['_ordem_tipo_final_completa']
            if 'data_mov_dt_final_completa' in df_view.columns:
                cols_to_drop.append('data_mov_dt_final_completa')
            df_view = df_view.drop(columns=cols_to_drop)

    # Calcular Saldo Total ANTES da tabela - método simples: última linha de cada SKU visível
    vol_total = 0.0
    if 'saldo' in df_view.columns and not df_view.empty and 'sku' in df_view.columns:
        # Usar df_view que já está filtrado e processado
        df_calc = df_view.copy()

        # Converter saldo para numérico
        df_calc['saldo'] = pd.to_numeric(df_calc['saldo'], errors='coerce').fillna(0.0)

        # Ordenar por SKU e data/id para pegar a última linha de cada SKU
        if 'data_movimento' in df_calc.columns:
            df_calc['data_mov_dt'] = pd.to_datetime(df_calc['data_movimento'], errors='coerce')
            cols_ord = ['sku', 'data_mov_dt']
            if 'id' in df_calc.columns:
                cols_ord.append('id')
            df_calc = df_calc.sort_values(by=cols_ord, ascending=True, na_position='last')
            df_calc = df_calc.drop(columns=['data_mov_dt'])
        elif 'id' in df_calc.columns:
            df_calc = df_calc.sort_values(by=['sku', 'id'], ascending=True, na_position='last')
        else:
            df_calc = df_calc.sort_values(by=['sku'], ascending=True, na_position='last')

        # Pegar a última linha de cada SKU visível (após filtros)
        ultimas_linhas = df_calc.groupby('sku', sort=False).last().reset_index()

        # Somar os saldos das últimas linhas
        if not ultimas_linhas.empty and 'saldo' in ultimas_linhas.columns:
            vol_total = float(ultimas_linhas['saldo'].sum())

    # Exibe o total ANTES da tabela
    # Calcular quantidade de SKUs únicos listados
    qtd_skus = 0
    if 'sku' in df_view.columns and not df_view.empty:
        qtd_skus = len(df_view['sku'].unique())

    # Exibir métricas lado a lado
    col1, col2 = st.columns(2)
    with col1:
        st.metric("📊 Saldo Total (soma do saldo da última linha de cada SKU visível)", formatar_br(vol_total))
    with col2:
        st.metric("🔢 Quantidade de Produtos (SKUs) Listados", f"{qtd_skus}")

    st.caption("ℹ️ Calculado a partir da última linha de cada produto visível após filtros")
    st.divider()

    # V189: saldo_anterior SEMPRE deve aparecer (mesmo que seja 0,000 para todos)

    # Formatação incluindo saldo_anterior (v189: sempre mostra, mesmo que seja 0)
    def fmt_saldo_ant(x):
        # Sempre formata, mesmo se for 0 (zero é um valor válido para saldo anterior)
        return formatar_br(float(x) if pd.notna(x) else 0.0)

    # Formatação monetária (2 casas decimais) para valores unitário e total
    def fmt_monetario(x):
        if pd.isna(x) or x == 0:
            return "0,00"
        return f"{float(x):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

    # SEMPRE mostra todas as colunas da tabela (não filtra colunas da exibição)
    df_para_exibicao = df_view.copy()

    # CORREÇÃO: Adicionar linha de TOTAL no topo da tabela que soma os saldos das linhas TOTAL de cada SKU
    if 'saldo' in df_para_exibicao.columns and not df_para_exibicao.empty and 'sku' in df_para_exibicao.columns:
        # Calcular total: usar linha TOTAL de cada SKU, ou última movimentação se não houver TOTAL
        df_calc_total = df_para_exibicao.copy()
        df_calc_total['saldo'] = pd.to_numeric(df_calc_total['saldo'], errors='coerce').fillna(0.0)

        # IMPORTANTE: Verificar se há múltiplas categorias no DataFrame
        # Se houver, significa que o filtro de categoria pode não ter sido aplicado corretamente
        categorias_no_df = set()
        if 'Cat_Auto' in df_calc_total.columns:
            # Pegar categorias apenas de linhas de movimentação (não TOTAL/ANTERIOR)
            if 'tipo' in df_calc_total.columns:
                mask_tipo_especial = df_calc_total['tipo'].astype(str).str.strip().str.upper().isin(['TOTAL', 'TOTAL:', 'ANTERIOR', 'ANTERIOR:'])
                df_mov_cat = df_calc_total[~mask_tipo_especial]
                categorias_no_df = set(df_mov_cat['Cat_Auto'].dropna().unique())
            else:
                categorias_no_df = set(df_calc_total['Cat_Auto'].dropna().unique())

        # IMPORTANTE: Excluir linhas ANTERIOR do cálculo (não devem ser somadas no total)
        # Separar linhas TOTAL, ANTERIOR e movimentações
        if 'tipo' in df_calc_total.columns:
            mask_total = df_calc_total['tipo'].astype(str).str.strip().str.upper().isin(['TOTAL', 'TOTAL:', 'TOTAL '])
            mask_anterior = df_calc_total['tipo'].astype(str).str.strip().str.upper().isin(['ANTERIOR', 'ANTERIOR:'])
            df_totais = df_calc_total[mask_total].copy()
            # Movimentações = tudo que não é TOTAL nem ANTERIOR
            df_movimentacoes = df_calc_total[~mask_total & ~mask_anterior].copy()
        else:
            df_totais = pd.DataFrame()
            df_movimentacoes = df_calc_total.copy()

        # Para cada SKU, pegar linha TOTAL se existir, senão última movimentação
        # IMPORTANTE: Cada SKU deve ser contado apenas UMA vez
        # SIMPLIFICAÇÃO: Não verificar categoria aqui - o filtro já foi aplicado no app.py
        # Se o SKU está no df_calc_total, significa que já passou pelo filtro de categoria
        saldos_por_sku = {}
        skus_unicos = df_calc_total['sku'].dropna().unique()

        for sku in skus_unicos:
            # Pegar categoria do SKU apenas para log (não para filtrar)
            cat_sku = None
            df_sku_check = df_calc_total[df_calc_total['sku'] == sku]
            if not df_sku_check.empty:
                # Pegar categoria de qualquer linha
                for idx, row in df_sku_check.iterrows():
                    cat_sku = row.get('Cat_Auto', None)
                    if pd.notna(cat_sku):
                        break
                if cat_sku is None or pd.isna(cat_sku):
                    cat_sku = df_sku_check.iloc[0].get('Cat_Auto', None)

            # Primeiro tentar pegar linha TOTAL (prioridade)
            # IMPORTANTE: Incluir mesmo se saldo for zero (produtos com saldo zero devem aparecer)
            if not df_totais.empty:
                linhas_total_sku = df_totais[df_totais['sku'] == sku]
                if not linhas_total_sku.empty:
                    # Usar a primeira linha TOTAL encontrada (deve ter apenas uma)
                    saldo_valor = float(linhas_total_sku.iloc[0]['saldo'])
                    # CORREÇÃO: Incluir mesmo se saldo for zero (pd.notna verifica se não é NaN, zero é válido)
                    if pd.notna(saldo_valor):
                        saldos_por_sku[sku] = {
                            'saldo': saldo_valor,  # Pode ser zero, isso é válido
                            'categoria': cat_sku,
                            'fonte': 'TOTAL'
                        }
                    continue

            # Se não tem TOTAL, pegar última movimentação (excluindo ANTERIOR)
            # IMPORTANTE: Incluir mesmo se saldo for zero
            if not df_movimentacoes.empty:
                mov_sku = df_movimentacoes[df_movimentacoes['sku'] == sku]
                if not mov_sku.empty:
                    # Ordenar por data/id para pegar a última
                    if 'data_movimento' in mov_sku.columns:
                        mov_sku['data_mov_dt'] = pd.to_datetime(mov_sku['data_movimento'], errors='coerce')
                        mov_sku = mov_sku.sort_values(by='data_mov_dt', ascending=True, na_position='last')
                        mov_sku = mov_sku.drop(columns=['data_mov_dt'])
                    elif 'id' in mov_sku.columns:
                        mov_sku = mov_sku.sort_values(by='id', ascending=True, na_position='last')
                    saldo_valor = float(mov_sku.iloc[-1]['saldo'])
                    # CORREÇÃO: Incluir mesmo se saldo for zero (pd.notna verifica se não é NaN, zero é válido)
                    if pd.notna(saldo_valor):
                        saldos_por_sku[sku] = {
                            'saldo': saldo_valor,  # Pode ser zero, isso é válido
                            'categoria': cat_sku,
                            'fonte': 'ULTIMA_MOVIMENTACAO'
                        }

        # Calcular total dos saldos (apenas uma vez por SKU)
        # Agora saldos_por_sku é um dict com dicts, então precisamos extrair os valores
        total_saldo = sum(v['saldo'] if isinstance(v, dict) else v for v in saldos_por_sku.values()) if saldos_por_sku else 0.0

        # GERAR LOG ESTRUTURADO PARA ANÁLISE AUTOMÁTICA
        # Verificar quais SKUs têm movimentação no período (tipo != TOTAL e != ANTERIOR)
        skus_com_movimento = set()
        skus_sem_movimento = set()
        if 'tipo' in df_para_exibicao.columns and 'sku' in df_para_exibicao.columns:
            for sku in df_para_exibicao['sku'].dropna().unique():
                df_sku = df_para_exibicao[df_para_exibicao['sku'] == sku]
                tipos_sku = df_sku['tipo'].astype(str).str.strip().str.upper().unique()
                # Se tem apenas TOTAL e/ou ANTERIOR, não tem movimentação no período
                if all(t in ['TOTAL', 'TOTAL:', 'ANTERIOR', 'ANTERIOR:'] for t in tipos_sku):
                    skus_sem_movimento.add(sku)
                else:
                    skus_com_movimento.add(sku)

        log_data = {
            "timestamp": datetime.now().isoformat(),
            "estatisticas_gerais": {
                "total_skus_calculo": len(saldos_por_sku),
                "total_linhas_df_para_exibicao": len(df_para_exibicao),
                "total_saldo_calculado": float(total_saldo),
                "total_saldo_formatado": formatar_br(total_saldo),
                "skus_com_movimento_no_periodo": len(skus_com_movimento),
                "skus_sem_movimento_no_periodo": len(skus_sem_movimento)
            },
            "df_full_antes_filtros": {},
            "df_para_exibicao_apos_filtros": {},
            "skus_no_calculo": {"total": 0, "skus": []},
            "skus_filtrados": {"total_toras": 0, "skus_toras": []},
            "produtos_categorizacao_incorreta": [],
            "detalhes_por_categoria": {},
            "skus_com_movimento": list(skus_com_movimento),
            "skus_sem_movimento": list(skus_sem_movimento)
        }

        # Coletar dados do df_full (antes dos filtros)
        if 'sku' in df_full.columns and 'Cat_Auto' in df_full.columns:
            skus_full = df_full['sku'].dropna().unique()
            log_data["df_full_antes_filtros"]["total_skus"] = len(skus_full)

            categorias_full = {}
            skus_por_categoria_full = {}
            for sku in skus_full:
                df_sku_full = df_full[df_full['sku'] == sku]
                if not df_sku_full.empty:
                    cat = df_sku_full.iloc[0].get('Cat_Auto', 'SEM CATEGORIA')
                    produto = df_sku_full.iloc[0].get('produto', '')
                    if cat not in categorias_full:
                        categorias_full[cat] = 0
                        skus_por_categoria_full[cat] = []
                    categorias_full[cat] += 1
                    skus_por_categoria_full[cat].append({
                        "sku": str(sku),
                        "produto": str(produto)
                    })

            log_data["df_full_antes_filtros"]["skus_por_categoria"] = categorias_full
            log_data["df_full_antes_filtros"]["detalhes_skus"] = skus_por_categoria_full

        # Coletar dados do df_para_exibicao (após filtros)
        if 'sku' in df_para_exibicao.columns:
            skus_unicos_df = df_para_exibicao['sku'].dropna().unique()
            log_data["df_para_exibicao_apos_filtros"]["total_skus"] = len(skus_unicos_df)

            # LOG DETALHADO: SKUs exibidos para o usuário com saldos (M3)
            log_data["skus_exibidos_usuario"] = {}
            for sku in skus_unicos_df:
                df_sku_exib = df_para_exibicao[df_para_exibicao['sku'] == sku].copy()
                if not df_sku_exib.empty:
                    # Tentar pegar saldo da linha TOTAL
                    if 'tipo' in df_sku_exib.columns:
                        mask_total = df_sku_exib['tipo'].astype(str).str.strip().str.upper().isin(['TOTAL', 'TOTAL:'])
                        if mask_total.any():
                            saldo_valor = float(df_sku_exib[mask_total].iloc[0]['saldo'])
                        else:
                            # Pegar último saldo
                            if 'data_movimento' in df_sku_exib.columns:
                                df_sku_exib['data_dt'] = pd.to_datetime(df_sku_exib['data_movimento'], errors='coerce')
                                df_sku_exib = df_sku_exib.sort_values(by='data_dt', ascending=False)
                            saldo_valor = float(df_sku_exib.iloc[0]['saldo'])
                    else:
                        # Se não tem tipo, pegar último saldo
                        if 'data_movimento' in df_sku_exib.columns:
                            df_sku_exib['data_dt'] = pd.to_datetime(df_sku_exib['data_movimento'], errors='coerce')
                            df_sku_exib = df_sku_exib.sort_values(by='data_dt', ascending=False)
                        saldo_valor = float(df_sku_exib.iloc[0]['saldo'])

                    produto = df_sku_exib.iloc[0].get('produto', '')
                    categoria = df_sku_exib.iloc[0].get('categoria', '')
                    cat_auto = df_sku_exib.iloc[0].get('Cat_Auto', '')

                    log_data["skus_exibidos_usuario"][str(sku)] = {
                        "sku": str(sku),
                        "produto": str(produto),
                        "categoria": str(categoria),
                        "cat_auto": str(cat_auto),
                        "saldo_m3": float(saldo_valor),
                        "saldo_m3_formatado": formatar_br(saldo_valor),
                        "no_calculo_total": str(sku) in saldos_por_sku
                    }

        # Coletar SKUs no cálculo com detalhes individuais
        log_data["saldos_por_sku_detalhado"] = {}
        if saldos_por_sku:
            if 'Cat_Auto' in df_para_exibicao.columns:
                categorias_skus = {}
                for sku, dados_saldo in saldos_por_sku.items():
                    # Extrair saldo (pode ser dict ou float)
                    if isinstance(dados_saldo, dict):
                        saldo = dados_saldo['saldo']
                        cat = dados_saldo.get('categoria', None)
                        fonte = dados_saldo.get('fonte', 'DESCONHECIDA')
                    else:
                        saldo = dados_saldo
                        cat = None
                        fonte = 'DESCONHECIDA'

                    df_sku = df_para_exibicao[df_para_exibicao['sku'] == sku]
                    if not df_sku.empty:
                        # Se não tem categoria do dict, pegar do DataFrame
                        if cat is None or pd.isna(cat):
                            cat = df_sku.iloc[0].get('Cat_Auto', 'SEM CATEGORIA')
                        produto = df_sku.iloc[0].get('produto', '')

                        # Adicionar ao log detalhado
                        log_data["saldos_por_sku_detalhado"][str(sku)] = {
                            "sku": str(sku),
                            "produto": str(produto),
                            "categoria": str(cat),
                            "saldo": float(saldo),
                            "saldo_formatado": formatar_br(saldo),
                            "fonte": fonte
                        }

                        if cat not in categorias_skus:
                            categorias_skus[cat] = {
                                "total_skus": 0,
                                "total_saldo": 0.0,
                                "skus": []
                            }
                        categorias_skus[cat]["total_skus"] += 1
                        categorias_skus[cat]["total_saldo"] += float(saldo)
                        categorias_skus[cat]["skus"].append({
                            "sku": str(sku),
                            "produto": str(produto),
                            "saldo": float(saldo),
                            "fonte": fonte
                        })

                log_data["detalhes_por_categoria"] = categorias_skus

                # Log de categorias presentes no cálculo
                log_data["categorias_no_calculo"] = {}
                for cat, dados in categorias_skus.items():
                    log_data["categorias_no_calculo"][cat] = {
                        "total_skus": dados["total_skus"],
                        "total_saldo": dados["total_saldo"],
                        "total_saldo_formatado": formatar_br(dados["total_saldo"])
                    }

                # SKUs no DataFrame mas não no cálculo
                skus_no_df = set(df_para_exibicao['sku'].dropna().unique())
                skus_no_calc = set(saldos_por_sku.keys())
                skus_faltando = skus_no_df - skus_no_calc
                if skus_faltando:
                    log_data["skus_no_calculo"]["total"] = len(skus_faltando)
                    log_data["skus_no_calculo"]["skus"] = []
                    for sku in sorted(skus_faltando):
                        df_sku = df_para_exibicao[df_para_exibicao['sku'] == sku]
                        if not df_sku.empty:
                            produto = df_sku.iloc[0].get('produto', '')
                            cat = df_sku.iloc[0].get('Cat_Auto', 'SEM CATEGORIA')
                            log_data["skus_no_calculo"]["skus"].append({
                                "sku": str(sku),
                                "produto": str(produto),
                                "categoria": str(cat)
                            })

                # SKUs filtrados (estão no df_full mas não no df_para_exibicao)
                if 'sku' in df_full.columns and 'Cat_Auto' in df_full.columns:
                    skus_full_set = set(df_full['sku'].dropna().unique())
                    skus_para_exib_set = set(df_para_exibicao['sku'].dropna().unique())
                    skus_filtrados = skus_full_set - skus_para_exib_set

                    skus_toras_filtrados = []
                    for sku in skus_filtrados:
                        df_sku_full = df_full[df_full['sku'] == sku]
                        if not df_sku_full.empty:
                            cat = df_sku_full.iloc[0].get('Cat_Auto', 'SEM CATEGORIA')
                            if cat == 'TORAS':
                                produto = df_sku_full.iloc[0].get('produto', '')
                                skus_toras_filtrados.append({
                                    "sku": str(sku),
                                    "produto": str(produto)
                                })

                    if skus_toras_filtrados:
                        log_data["skus_filtrados"]["total_toras"] = len(skus_toras_filtrados)
                        log_data["skus_filtrados"]["skus_toras"] = skus_toras_filtrados

                # Produtos com categorização incorreta
                if 'sku' in df_full.columns and 'produto' in df_full.columns and 'Cat_Auto' in df_full.columns:
                    produtos_nao_toras = []
                    for sku in df_full['sku'].dropna().unique():
                        df_sku_full = df_full[df_full['sku'] == sku]
                        if not df_sku_full.empty:
                            produto = str(df_sku_full.iloc[0].get('produto', '')).upper()
                            cat = df_sku_full.iloc[0].get('Cat_Auto', 'SEM CATEGORIA')
                            if ('TORO' in produto or 'TORA' in produto) and cat != 'TORAS':
                                produtos_nao_toras.append({
                                    "sku": str(sku),
                                    "produto": str(df_sku_full.iloc[0].get('produto', '')),
                                    "categoria_atual": str(cat)
                                })

                    if produtos_nao_toras:
                        log_data["produtos_categorizacao_incorreta"] = produtos_nao_toras

        # Salvar log em arquivo JSON
        log_dir = "logs"
        if not os.path.exists(log_dir):
            os.makedirs(log_dir)

        log_filename = os.path.join(log_dir, f"plenus_debug_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        with open(log_filename, 'w', encoding='utf-8') as f:
            json.dump(log_data, f, indent=2, ensure_ascii=False)

        # Mostrar apenas um resumo simples na tela
        with st.expander("🔍 DEBUG - Log Gerado", expanded=False):
            st.success(f"✅ Log estruturado salvo em: `{log_filename}`")
            st.write(f"**📊 Total calculado:** {formatar_br(total_saldo)}")
            if log_data.get("detalhes_por_categoria"):
                st.write("**📋 Resumo por categoria:**")
                for cat, dados in sorted(log_data["detalhes_por_categoria"].items()):
                    st.write(f"  - **{cat}**: {dados['total_skus']} SKUs, Total: {formatar_br(dados['total_saldo'])}")
            if log_data.get("skus_filtrados", {}).get("total_toras", 0) > 0:
                st.warning(f"⚠️ {log_data['skus_filtrados']['total_toras']} SKUs de TORAS foram filtrados")
            if log_data.get("produtos_categorizacao_incorreta"):
                st.error(f"❌ {len(log_data['produtos_categorizacao_incorreta'])} produtos com categorização incorreta")
            if log_data.get("estatisticas_gerais", {}).get("skus_sem_movimento_no_periodo", 0) > 0:
                st.info(f"ℹ️ {log_data['estatisticas_gerais']['skus_sem_movimento_no_periodo']} SKUs sem movimentação no período (incluídos com saldo anterior)")

        # Calcular totais de entrada e saída APENAS dos SKUs que estão em saldos_por_sku
        # IMPORTANTE: Somar apenas dos SKUs que foram incluídos no cálculo do saldo
        total_entrada = 0.0
        total_saida = 0.0

        # Usar apenas os SKUs que estão em saldos_por_sku (os que foram incluídos no cálculo)
        skus_no_calculo = set(saldos_por_sku.keys())

        if not df_totais.empty and skus_no_calculo:
            # Filtrar linhas TOTAL apenas dos SKUs que estão no cálculo
            df_totais_filtrado = df_totais[df_totais['sku'].isin(skus_no_calculo)]
            if not df_totais_filtrado.empty:
                total_entrada = float(pd.to_numeric(df_totais_filtrado['entrada'], errors='coerce').fillna(0.0).sum())
                total_saida = float(pd.to_numeric(df_totais_filtrado['saida'], errors='coerce').fillna(0.0).sum())

        # Se não tem linhas TOTAL suficientes, somar das movimentações dos SKUs no cálculo
        if total_entrada == 0.0 and total_saida == 0.0 and not df_movimentacoes.empty and skus_no_calculo:
            df_mov_filtrado = df_movimentacoes[df_movimentacoes['sku'].isin(skus_no_calculo)]
            if not df_mov_filtrado.empty:
                total_entrada = float(pd.to_numeric(df_mov_filtrado['entrada'], errors='coerce').fillna(0.0).sum())
                total_saida = float(pd.to_numeric(df_mov_filtrado['saida'], errors='coerce').fillna(0.0).sum())

        # Criar linha de TOTAL
        linha_total = {}
        for col in df_para_exibicao.columns:
            if col == 'tipo':
                linha_total[col] = 'TOTAL GERAL'
            elif col == 'saldo':
                linha_total[col] = total_saldo
            elif col == 'entrada':
                linha_total[col] = total_entrada
            elif col == 'saida':
                linha_total[col] = total_saida
            elif col in ['saldo_anterior', 'vlr_unit', 'vlr_total']:
                # Somar valores numéricos se existirem
                if col in df_calc_total.columns:
                    linha_total[col] = pd.to_numeric(df_calc_total[col], errors='coerce').fillna(0.0).sum()
                else:
                    linha_total[col] = 0.0
            else:
                linha_total[col] = ''

        # Criar DataFrame com a linha de total
        df_linha_total = pd.DataFrame([linha_total])

        # Adicionar no topo do DataFrame
        df_para_exibicao = pd.concat([df_linha_total, df_para_exibicao], ignore_index=True)

    cols_exist = [c for c in cols_table if c in df_para_exibicao.columns]

    format_dict = {
        'entrada': formatar_br,
        'saida': formatar_br,
        'saldo': formatar_br
    }
    if 'saldo_anterior' in cols_exist:
        format_dict['saldo_anterior'] = fmt_saldo_ant
    if 'vlr_unit' in cols_exist:
        format_dict['vlr_unit'] = fmt_monetario
    if 'vlr_total' in cols_exist:
        format_dict['vlr_total'] = fmt_monetario

    # V191: Aplicar cores nas linhas - TOTAL em vermelho (destacado)
    styled_df = df_para_exibicao[cols_exist].style.format(format_dict)

    # Aplicar cores condicionalmente
    if 'tipo' in df_para_exibicao.columns:
        def aplicar_cores(row):
            tipo_val = str(row.get('tipo', '')).strip().upper() if pd.notna(row.get('tipo')) else ''
            if tipo_val in ['TOTAL GERAL', 'TOTAL', 'TOTAL:']:
                # TOTAL GERAL em azul destacado (linha de totalização no topo)
                if tipo_val == 'TOTAL GERAL':
                    return ['background-color: #cce5ff; color: #000000; font-weight: bold'] * len(row)
                # TOTAL de produto em vermelho
                return ['background-color: #ffcccc; color: #000000; font-weight: bold'] * len(row)
            elif tipo_val in ['ANTERIOR', 'ANTERIOR:']:
                # ANTERIOR em verde claro (início do produto)
                return ['background-color: #ccffcc; color: #000000; font-weight: bold'] * len(row)
            return [''] * len(row)

        styled_df = styled_df.apply(aplicar_cores, axis=1)

    st.dataframe(styled_df, use_container_width=True, height=600)

    # Save Button (Only if allowed)
    if allow_save:
        st.divider()
        st.info("Para auditoria, é necessário salvar os movimentos no banco de dados.")
        if st.button("💾 Salvar Filtrados no Banco (v191)", key=f"{key_prefix}_btn_save"):
            # Prepare for DB
            df_save = df_view.copy()
            # Rename cols if necessary
            rename_map = {}
            if 'tipo' in df_save.columns:
                rename_map['tipo'] = 'tipo_movimento'
            if 'saldo' in df_save.columns:
                rename_map['saldo'] = 'saldo_apos'
            df_save.rename(columns=rename_map, inplace=True)

            # V190: Salvar também linhas TOTAL (tipo='TOTAL' ou 'TOTAL:') mesmo sem data_movimento
            # Isso permite que ao carregar do histórico, possamos usar os totais explícitos
            # Para linhas TOTAL sem data, usar a data do último movimento do mesmo SKU ou data máxima do período
            if 'data_movimento' in df_save.columns and 'tipo_movimento' in df_save.columns:
                # Separar linhas com data (movimentações) e sem data (TOTAL/ANTERIOR)
                df_com_data = df_save[df_save['data_movimento'].notna()].copy()
                df_sem_data = df_save[df_save['data_movimento'].isna()].copy()

                # Se há linhas sem data que são TOTAIS, precisamos atribuir uma data para salvar
                if not df_sem_data.empty:
                    mask_total = df_sem_data['tipo_movimento'].astype(str).str.strip().str.upper().isin(['TOTAL', 'TOTAL:'])
                    df_totais_sem_data = df_sem_data[mask_total].copy()

                    if not df_totais_sem_data.empty:
                        # Para cada linha TOTAL sem data, usar a data do último movimento do mesmo SKU
                        df_totais_com_data = df_totais_sem_data.copy()

                        # Se não há linhas com data, não podemos atribuir data às linhas TOTAL
                        # Nesse caso, essas linhas serão perdidas (mas isso não deveria acontecer normalmente)
                        if df_com_data.empty:
                            # Não há movimentações - linhas TOTAL sem data não podem ser salvas
                            # Isso será registrado no log
                            pass
                        else:
                            # Há linhas com data - usar lógica original
                            for idx, row in df_totais_com_data.iterrows():
                                sku_match = row['sku'] if 'sku' in row else None
                                if sku_match and 'sku' in df_com_data.columns:
                                    # Pega a última data do mesmo SKU (filtrando valores NaT)
                                    movimentos_sku = df_com_data[df_com_data['sku'] == sku_match]
                                    if not movimentos_sku.empty:
                                        # Converter para datetime se necessário e filtrar NaT
                                        datas_sku = pd.to_datetime(movimentos_sku['data_movimento'], errors='coerce')
                                        datas_validas = datas_sku[datas_sku.notna()]
                                        if not datas_validas.empty:
                                            ultima_data = datas_validas.max()
                                            # V191: Converter Timestamp para string no formato YYYY-MM-DD
                                            df_totais_com_data.at[idx, 'data_movimento'] = ultima_data.strftime('%Y-%m-%d') if pd.notna(ultima_data) else None
                                        else:
                                            # Se não tem data válida, usar a data máxima de todo o df_com_data
                                            todas_datas = pd.to_datetime(df_com_data['data_movimento'], errors='coerce')
                                            datas_validas_todas = todas_datas[todas_datas.notna()]
                                            if not datas_validas_todas.empty:
                                                ultima_data_geral = datas_validas_todas.max()
                                                # V191: Converter Timestamp para string no formato YYYY-MM-DD
                                                df_totais_com_data.at[idx, 'data_movimento'] = ultima_data_geral.strftime('%Y-%m-%d') if pd.notna(ultima_data_geral) else None
                                    else:
                                        # SKU não tem movimentações - usar data máxima de todo o df_com_data
                                        todas_datas = pd.to_datetime(df_com_data['data_movimento'], errors='coerce')
                                        datas_validas_todas = todas_datas[todas_datas.notna()]
                                        if not datas_validas_todas.empty:
                                            ultima_data_geral = datas_validas_todas.max()
                                            df_totais_com_data.at[idx, 'data_movimento'] = ultima_data_geral.strftime('%Y-%m-%d') if pd.notna(ultima_data_geral) else None
                                else:
                                    # SKU não encontrado - usar data máxima de todo o df_com_data
                                    todas_datas = pd.to_datetime(df_com_data['data_movimento'], errors='coerce')
                                    datas_validas_todas = todas_datas[todas_datas.notna()]
                                    if not datas_validas_todas.empty:
                                        ultima_data_geral = datas_validas_todas.max()
                                        df_totais_com_data.at[idx, 'data_movimento'] = ultima_data_geral.strftime('%Y-%m-%d') if pd.notna(ultima_data_geral) else None

                    # Remover ANTERIOR sem data (não precisamos salvar)
                    mask_anterior = df_sem_data['tipo_movimento'].astype(str).str.strip().str.upper().isin(['ANTERIOR', 'ANTERIOR:'])
                    df_anteriores = df_sem_data[mask_anterior]

                    # Juntar tudo: movimentações + TOTAIS com data atribuída
                    if 'df_totais_com_data' in locals() and not df_totais_com_data.empty:
                        df_save = pd.concat([df_com_data, df_totais_com_data], ignore_index=True)
                    else:
                        df_save = df_com_data
                else:
                    # Se não tem linhas sem data, só usar as com data
                    df_save = df_com_data

            # V189: Keep only columns that exist in DB (incluindo saldo_anterior agora)
            cols_db_plenus = ['sku', 'produto', 'categoria', 'data_movimento', 'tipo_movimento',
                              'entrada', 'saida', 'saldo_apos', 'saldo_anterior', 'nota', 'serie', 'arquivo_origem']
            # V189: Garantir que saldo_anterior existe (preencher com 0 se não existir)
            if 'saldo_anterior' not in df_save.columns:
                df_save['saldo_anterior'] = 0.0
            else:
                df_save['saldo_anterior'] = df_save['saldo_anterior'].fillna(0.0)
            cols_to_save = [c for c in cols_db_plenus if c in df_save.columns]
            df_save = df_save[cols_to_save]

            # V191: Converter coluna data_movimento para string (formato YYYY-MM-DD) se for Timestamp
            if 'data_movimento' in df_save.columns:
                if pd.api.types.is_datetime64_any_dtype(df_save['data_movimento']):
                    df_save['data_movimento'] = df_save['data_movimento'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notna(x) else None)

            # LOG DETALHADO ANTES DE SALVAR - para investigar produtos não salvos
            skus_unicos_list = sorted(df_save['sku'].dropna().unique().tolist()) if 'sku' in df_save.columns else []
            meses_no_df_list = sorted(pd.to_datetime(df_save['data_movimento'], errors='coerce').dt.strftime('%Y-%m').dropna().unique().tolist()) if 'data_movimento' in df_save.columns else []

            # Detalhar SKUs por mês
            skus_por_mes = {}
            if 'sku' in df_save.columns and 'data_movimento' in df_save.columns:
                df_save['mes_ano'] = pd.to_datetime(df_save['data_movimento'], errors='coerce').dt.strftime('%Y-%m')
                for mes in meses_no_df_list:
                    df_mes = df_save[df_save['mes_ano'] == mes]
                    skus_por_mes[mes] = sorted(df_mes['sku'].dropna().unique().tolist())
                df_save = df_save.drop(columns=['mes_ano'])

            log_save_data = {
                "timestamp": datetime.now().isoformat(),
                "antes_salvar": {
                    "total_registros": len(df_save),
                    "total_skus_unicos": len(skus_unicos_list),
                    "skus_unicos": skus_unicos_list,
                    "tipos_movimento": df_save['tipo_movimento'].value_counts().to_dict() if 'tipo_movimento' in df_save.columns else {},
                    "meses_no_df": meses_no_df_list,
                    "skus_por_mes": skus_por_mes
                }
            }

            # V190: Salvar verificando por MÊS (não sobrescreve, só preenche meses faltantes)
            inseridos, meses_existentes = salvar_lote_smart('plenus_historico', 'data_movimento', df_save, por_mes=True)

            # Verificar registros que não foram salvos (antes de chamar salvar_lote_smart)
            df_save_antes = df_save.copy()
            if 'data_movimento' in df_save_antes.columns and 'sku' in df_save_antes.columns:
                df_save_antes['mes_ano'] = pd.to_datetime(df_save_antes['data_movimento'], errors='coerce').dt.strftime('%Y-%m')
                registros_sem_data = df_save_antes[df_save_antes['mes_ano'].isna()]
                if not registros_sem_data.empty:
                    log_save_data["registros_nao_salvos"] = {
                        "total_sem_data": len(registros_sem_data),
                        "tipos_sem_data": registros_sem_data['tipo_movimento'].value_counts().to_dict() if 'tipo_movimento' in registros_sem_data.columns else {},
                        "skus_sem_data": sorted(registros_sem_data['sku'].dropna().unique().tolist())
                    }
                df_save_antes = df_save_antes.drop(columns=['mes_ano'])

            # Verificar quais SKUs foram filtrados (não salvos porque mês já existe)
            skus_filtrados_por_mes = {}
            if meses_existentes > 0 and 'sku' in df_save.columns and 'data_movimento' in df_save.columns:
                df_save['mes_ano'] = pd.to_datetime(df_save['data_movimento'], errors='coerce').dt.strftime('%Y-%m')
                meses_existentes_set = set()
                # Buscar meses existentes no banco
                from database import get_db_connection, check_months_exist
                conn = get_db_connection()
                try:
                    query = f"SELECT DISTINCT data_movimento FROM plenus_historico WHERE data_movimento IS NOT NULL"
                    df_exist = pd.read_sql(query, conn)
                    if not df_exist.empty:
                        meses_existentes_set = set(pd.to_datetime(df_exist['data_movimento']).dt.strftime('%Y-%m').unique())
                finally:
                    conn.close()

                for mes in meses_existentes_set:
                    if mes in meses_no_df_list:
                        df_mes = df_save[df_save['mes_ano'] == mes]
                        skus_filtrados_por_mes[mes] = sorted(df_mes['sku'].dropna().unique().tolist())
                df_save = df_save.drop(columns=['mes_ano'])

            # Calcular diferença entre esperado e inserido
            registros_nao_inseridos = len(df_save) - inseridos

            # LOG APÓS SALVAR
            log_save_data["depois_salvar"] = {
                "registros_inseridos": inseridos,
                "meses_existentes": meses_existentes,
                "total_registros_esperados": len(df_save),
                "registros_nao_inseridos": registros_nao_inseridos,
                "skus_filtrados_por_mes_existente": skus_filtrados_por_mes
            }

            # Se há registros não inseridos, investigar o motivo
            if registros_nao_inseridos > 0:
                log_save_data["depois_salvar"]["aviso"] = f"⚠️ {registros_nao_inseridos} registros não foram inseridos! Investigar motivo."

            # Verificar quais SKUs foram salvos (consultar banco após salvar)
            if inseridos > 0 and 'sku' in df_save.columns:
                # Buscar SKUs únicos que foram salvos (com data_movimento válida)
                df_save_com_data = df_save[df_save['data_movimento'].notna()].copy()
                if not df_save_com_data.empty:
                    meses_salvos = pd.to_datetime(df_save_com_data['data_movimento'], errors='coerce').dt.strftime('%Y-%m').dropna().unique()
                    # Buscar no banco quais SKUs existem para esses meses
                    from database import get_db_connection
                    conn = get_db_connection()
                    try:
                        meses_placeholders = ','.join(['?'] * len(meses_salvos))
                        query = f"""
                            SELECT DISTINCT sku, produto, categoria
                            FROM plenus_historico
                            WHERE substr(data_movimento, 1, 7) IN ({meses_placeholders})
                        """
                        df_skus_salvos = pd.read_sql(query, conn, params=meses_salvos.tolist())
                        log_save_data["depois_salvar"]["skus_salvos_no_banco"] = sorted(df_skus_salvos['sku'].dropna().unique().tolist()) if not df_skus_salvos.empty else []
                        log_save_data["depois_salvar"]["total_skus_salvos"] = len(log_save_data["depois_salvar"]["skus_salvos_no_banco"])
                    except Exception as e:
                        log_save_data["depois_salvar"]["erro_ao_verificar"] = str(e)
                    finally:
                        conn.close()

            # Salvar log em arquivo
            log_dir = "logs"
            if not os.path.exists(log_dir):
                os.makedirs(log_dir)
            log_filename = os.path.join(log_dir, f"plenus_save_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
            with open(log_filename, 'w', encoding='utf-8') as f:
                json.dump(log_save_data, f, indent=2, ensure_ascii=False)

            # Mostrar resumo na tela
            if log_save_data["antes_salvar"]["total_skus_unicos"] > 0:
                st.info(f"📊 **Log de salvamento:** {log_save_data['antes_salvar']['total_skus_unicos']} SKUs únicos preparados para salvar | {inseridos} registros inseridos | Log salvo em: `{log_filename}`")

            # Atualizar datas da sessão apenas se houver novos registros
            if inseridos > 0:
                # V191: Filtrar valores NaT antes de calcular min/max
                dates_series = pd.to_datetime(df_save['data_movimento'], errors='coerce')
                dates_validas = dates_series[dates_series.notna()].dt.date
                if not dates_validas.empty:
                    update_session_dates('p', dates_validas.min(), dates_validas.max())

            # V190: Mensagens de feedback melhoradas
            if inseridos > 0:
                st.success(f"✅ {inseridos} registro(s) gravado(s) com sucesso no banco de dados!")
                if meses_existentes > 0:
                    st.info(f"ℹ️ {meses_existentes} mês(es) já existiam no banco e foram ignorados. Para sobrescrever, exclua o período antes de importar.")
            elif meses_existentes > 0:
                st.warning(f"⚠️ Todos os meses deste período já existem no banco. Nada foi salvo. Para sobrescrever, use 'Gerenciar / Excluir' para remover o período antes de importar.")
            else:
                st.info("ℹ️ Nenhum registro novo para salvar.")
