# Como testar a nova funcionalidade

## O que foi adicionado:

Na seção **"Carregar do Histórico"** do módulo **SALDO SISFLORA**, foi adicionado um filtro de categoria logo acima da tabela "Dados Carregados na Memória".

## Como ficará visualmente:

```
┌─────────────────────────────────────────────┐
│ Dados Carregados na Memória:                │
├─────────────────────────────────────────────┤
│ Filtrar por Categoria (Cat_Auto):          │
│ [▼ Multiselect]                            │
│   ☑ TODOS                                  │
│   ☐ TORAS                                  │
│   ☐ SERRADAS                               │
│   ☐ BENEFICIADAS                           │
│   ☐ OUTROS                                 │
├─────────────────────────────────────────────┤
│ 🔎 Pesquisa Global: [_________]            │
│ Filtrar por Coluna(s): [Multiselect]      │
├─────────────────────────────────────────────┤
│ [Tabela com dados filtrados]               │
└─────────────────────────────────────────────┘
```

## Funcionalidades:

1. **Multiselect de Categorias**: Permite selecionar uma ou múltiplas categorias
2. **Opção "TODOS"**: Quando selecionado, mostra todas as categorias (comportamento padrão)
3. **Filtro dinâmico**: Filtra os dados antes de exibir na tabela
4. **Integração**: Funciona junto com os filtros existentes (pesquisa global e filtro de colunas)

## Como testar:

1. Execute: `streamlit run app.py`
2. Navegue até: **1. SALDO SISFLORA**
3. Selecione: **Carregar do Histórico**
4. Carregue uma data do histórico
5. Você verá o novo filtro de categoria acima da tabela
6. Teste selecionando diferentes categorias

## Exemplo de uso:

- **Mostrar todas**: Selecione apenas "TODOS" (ou deixe como padrão)
- **Filtrar por uma categoria**: Desmarque "TODOS" e selecione apenas "TORAS"
- **Filtrar por múltiplas**: Desmarque "TODOS" e selecione "TORAS" e "SERRADAS"
