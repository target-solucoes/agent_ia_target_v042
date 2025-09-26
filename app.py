"""
Aplicação Streamlit Refatorada - Target AI Agent v0.51
Reduzido de 1468 para aproximadamente 300 linhas
"""

import streamlit as st
import pandas as pd
import json
import time
import sys
from typing import Dict, Optional

sys.path.append("src")

# Importar módulos refatorados
from src.utils.data_loaders import load_parquet_data, initialize_agent
from src.utils.formatters import format_context_for_display, format_sql_query
from src.filters.filter_manager import (
    filter_user_friendly_context,
    create_enhanced_filter_manager
)
from src.filters.json_filter_manager import get_json_filter_manager
from src.visualization.plotly_charts import render_plotly_visualization

# Page configuration
st.set_page_config(page_title="Agente IA Target v0.51", page_icon="🤖", layout="wide")


def main():
    """Função principal da aplicação"""
    # Initialize CSS and header
    _setup_page_styling()
    _render_header()

    # Load data and initialize agent
    df, data_error = load_parquet_data()
    if data_error:
        st.error(f"❌ {data_error}")
        st.stop()

    agent, df_agent, agent_error = initialize_agent()
    if agent_error:
        st.error(f"❌ {agent_error}")
        st.stop()

    # Main application interface
    _render_main_interface(agent, df)

    # Company footer
    _render_footer()


def _setup_page_styling():
    """Configura CSS personalizado com design moderno e minimalista"""
    st.markdown("""
    <style>
    .main > div {
        padding-top: 1rem;
    }

    /* Header Styling */
    .header-container {
        background: linear-gradient(135deg, #1a2332 0%, #2d3e50 100%);
        padding: 2rem 0;
        margin: -1rem -1rem 2rem -1rem;
        border-radius: 0 0 20px 20px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.1);
        text-align: center;
    }

    .app-title {
        color: white !important;
        font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, sans-serif;
        font-size: 2.5rem;
        font-weight: 300;
        margin: 0;
        letter-spacing: 2px;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
    }

    .app-subtitle {
        color: white !important;
        font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, sans-serif;
        font-size: 1rem;
        font-weight: 300;
        margin: 0.5rem 0 0 0;
        letter-spacing: 1px;
        opacity: 0.95;
        text-shadow: 1px 1px 2px rgba(0,0,0,0.3);
    }

    .app-description {
        color: rgba(255,255,255,0.8);
        font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, sans-serif;
        font-size: 0.9rem;
        font-weight: 300;
        margin: 1rem 0 0 0;
        max-width: 600px;
        margin-left: auto;
        margin-right: auto;
        line-height: 1.5;
    }

    .feature-icons {
        display: flex;
        justify-content: center;
        gap: 2rem;
        margin-top: 1.5rem;
        flex-wrap: wrap;
    }

    .feature-item {
        display: flex;
        flex-direction: column;
        align-items: center;
        color: rgba(255,255,255,0.7);
        font-size: 0.8rem;
        font-weight: 300;
    }

    .feature-icon {
        font-size: 1.5rem;
        margin-bottom: 0.5rem;
        opacity: 0.8;
    }

    /* Chat Container Styling */
    .chat-main-container {
        display: flex;
        flex-direction: column;
        margin: 2rem 0;
    }

    .chat-messages-container {
        padding: 1rem;
        margin-bottom: 1rem;
        border-radius: 15px;
        border: 1px solid var(--secondary-background-color);
    }

    .chat-input-container {
        padding: 1.5rem 0;
        margin-top: 1rem;
        border-top: 1px solid var(--secondary-background-color);
    }

    /* Chat Message Styling - Dark mode friendly */
    .stChatMessage {
        border-radius: 15px;
        padding: 1.2rem;
        margin: 0.8rem 0;
        box-shadow: 0 2px 10px rgba(0,0,0,0.08);
        border: 1px solid var(--secondary-background-color);
    }

    .stChatMessage[data-testid="user-message"] {
        background: linear-gradient(135deg, #e74c3c 0%, #c0392b 100%) !important;
        color: white !important;
        margin-left: 2rem;
    }

    .stChatMessage[data-testid="assistant-message"] {
        border-left: 4px solid #e74c3c;
        margin-right: 2rem;
    }

    /* Chat Input Styling - Dark mode friendly */
    .stChatInputContainer {
        border: none !important;
        padding: 0 !important;
        margin: 0 !important;
        background: transparent;
    }

    .stChatInput > div {
        border-radius: 25px !important;
        border: 2px solid #e74c3c !important;
    }

    .stChatInput input {
        border: none !important;
        font-size: 1rem !important;
        padding: 1rem 1.5rem !important;
    }

    /* Welcome message styling - Dark mode friendly */
    .welcome-message {
        text-align: center;
        padding: 3rem 2rem;
        font-style: italic;
        border-radius: 15px;
        margin: 2rem 0;
        border: 2px dashed var(--secondary-background-color);
    }

    .welcome-message h3 {
        color: #e74c3c;
        margin-bottom: 1rem;
    }

    /* Button Styling */
    .stButton > button {
        background: linear-gradient(135deg, #6c757d 0%, #5a6268 100%);
        color: white;
        border: none;
        border-radius: 20px;
        padding: 0.4rem 0.8rem;
        font-weight: 400;
        font-size: 0.85rem;
        transition: all 0.3s ease;
        box-shadow: 0 2px 8px rgba(108, 117, 125, 0.3);
    }

    .stButton > button:hover {
        background: linear-gradient(135deg, #5a6268 0%, #495057 100%);
        transform: translateY(-1px);
        box-shadow: 0 3px 12px rgba(108, 117, 125, 0.4);
    }

    .stButton > button:active {
        transform: translateY(0px);
    }

    /* Debug mode toggle styling */
    .stToggle > div {
        background-color: transparent !important;
    }

    .stToggle > div > div {
        background-color: #f0f0f0 !important;
        border-radius: 20px !important;
    }

    .stToggle > div > div[data-checked="true"] {
        background-color: #e74c3c !important;
    }

    /* Debug section styling */
    .debug-section {
        background-color: rgba(231, 76, 60, 0.05);
        border: 1px solid rgba(231, 76, 60, 0.2);
        border-radius: 10px;
        padding: 1rem;
        margin-top: 1rem;
    }

    .debug-title {
        color: #e74c3c;
        font-weight: bold;
        margin-bottom: 0.5rem;
    }

    /* Filter Management Styling */
    .filter-checkbox {
        margin-right: 0.5rem !important;
        margin-bottom: 0.5rem !important;
    }

    .filter-item {
        display: flex;
        align-items: center;
        margin-bottom: 0.5rem;
        padding: 0.25rem 0;
    }

    .filter-text {
        flex: 1;
        margin-left: 0.5rem;
    }

    .disabled-filter {
        opacity: 0.6;
        text-decoration: line-through;
        color: var(--text-color-light, #666);
    }

    .enabled-filter {
        opacity: 1;
        text-decoration: none;
    }

    /* Sidebar filter management styling */
    .stSidebar .stCheckbox {
        margin-bottom: 0.25rem !important;
    }

    .stSidebar .stCheckbox > div {
        margin-bottom: 0 !important;
    }

    .filter-category-header {
        font-weight: bold;
        margin-top: 1rem;
        margin-bottom: 0.5rem;
        border-bottom: 1px solid var(--secondary-background-color);
        padding-bottom: 0.25rem;
    }

    .filter-status-info {
        font-size: 0.85rem;
        font-style: italic;
        padding: 0.5rem;
        margin-top: 0.5rem;
        border-radius: 5px;
        background: var(--secondary-background-color);
    }

    .reactivate-button {
        width: 100% !important;
        margin-top: 0.5rem !important;
    }

    /* Modern minimalist enhancements */
    .stApp {
        background-color: var(--background-color);
    }

    /* Enhanced sidebar styling - Light Mode */
    .stSidebar {
        background: #ffffff !important;
        border-right: 1px solid #e0e0e0;
    }

    .stSidebar > div {
        padding-top: 2rem;
    }

    /* Sidebar title styling */
    .stSidebar h2 {
        color: #2c3e50 !important;
        font-size: 1.1rem !important;
        font-weight: 600 !important;
        margin-bottom: 1rem !important;
        padding-bottom: 0.5rem !important;
        border-bottom: 2px solid #e74c3c !important;
    }

    /* Sidebar text styling for better readability */
    .stSidebar p,
    .stSidebar div,
    .stSidebar span {
        color: #2c3e50 !important;
        font-weight: 500 !important;
    }

    .stSidebar strong {
        color: #1a202c !important;
        font-weight: 700 !important;
    }

    .stSidebar .stMarkdown,
    .stSidebar .stMarkdown p,
    .stSidebar .stMarkdown div {
        color: #2c3e50 !important;
    }

    /* Sidebar checkbox and toggle styling */
    .stSidebar .stCheckbox label,
    .stSidebar .stToggle label {
        color: #2c3e50 !important;
        font-weight: 500 !important;
    }

    /* Sidebar expander styling */
    .stSidebar .stExpander summary {
        color: #2c3e50 !important;
        font-weight: 600 !important;
    }

    /* Enhanced expander styling */
    .stExpander {
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        margin: 0.5rem 0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }

    .stExpander > div > div {
        background-color: #f8f9fa;
        border-radius: 8px;
    }

    /* Code block improvements */
    .stCodeBlock {
        background-color: #f8f9fa;
        border: 1px solid #e0e0e0;
        border-radius: 8px;
        font-family: 'Consolas', 'Monaco', 'Courier New', monospace;
    }

    /* Metric improvements */
    .stMetric {
        background-color: rgba(255,255,255,0.8);
        border-radius: 10px;
        padding: 1rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        border: 1px solid #e0e0e0;
    }

    /* Success message styling */
    .stSuccess {
        background-color: rgba(46, 204, 113, 0.1);
        border: 1px solid #2ecc71;
        border-radius: 8px;
        color: #27ae60;
    }

    /* Warning message styling */
    .stWarning {
        background-color: rgba(241, 196, 15, 0.1);
        border: 1px solid #f1c40f;
        border-radius: 8px;
        color: #f39c12;
    }

    /* Error message styling */
    .stError {
        background-color: rgba(231, 76, 60, 0.1);
        border: 1px solid #e74c3c;
        border-radius: 8px;
        color: #c0392b;
    }

    /* Modern card styling for containers */
    .card-container {
        background: white;
        border-radius: 12px;
        padding: 1.5rem;
        margin: 1rem 0;
        box-shadow: 0 2px 12px rgba(0,0,0,0.08);
        border: 1px solid #e0e0e0;
    }

    /* Responsive adjustments */
    @media (max-width: 768px) {
        .app-title {
            font-size: 2rem;
        }
        .feature-icons {
            gap: 1rem;
        }
        .header-container {
            padding: 1.5rem 1rem;
        }
        .stChatMessage[data-testid="user-message"] {
            margin-left: 0.5rem;
        }
        .stChatMessage[data-testid="assistant-message"] {
            margin-right: 0.5rem;
        }
    }

    /* Sidebar separator styling */
    .stSidebar hr {
        border-color: #e0e0e0 !important;
        margin: 1rem 0 !important;
    }

    /* Universal sidebar text color override */
    .stSidebar,
    .stSidebar *,
    .stSidebar div,
    .stSidebar p,
    .stSidebar span,
    .stSidebar label,
    .stSidebar em {
        color: #2c3e50 !important;
    }

    .stSidebar div[data-testid="stMarkdownContainer"],
    .stSidebar div[data-testid="stMarkdownContainer"] *,
    .stSidebar .stMarkdown,
    .stSidebar .stMarkdown * {
        color: #2c3e50 !important;
    }

    /* Dark mode improvements */
    @media (prefers-color-scheme: dark) {
        .stSidebar {
            background: linear-gradient(180deg, #2c3e50 0%, #34495e 100%) !important;
        }

        .stSidebar,
        .stSidebar *,
        .stSidebar div,
        .stSidebar p,
        .stSidebar span,
        .stSidebar label,
        .stSidebar em,
        .stSidebar h2 {
            color: #ecf0f1 !important;
        }

        .stSidebar strong {
            color: #ffffff !important;
        }

        .stSidebar div[data-testid="stMarkdownContainer"],
        .stSidebar div[data-testid="stMarkdownContainer"] *,
        .stSidebar .stMarkdown,
        .stSidebar .stMarkdown * {
            color: #ecf0f1 !important;
        }

        .stSidebar h2 {
            border-bottom: 2px solid #e74c3c !important;
        }

        .card-container {
            background: #34495e;
            border-color: #4a5a70;
        }
    }
    </style>
    """, unsafe_allow_html=True)


def _render_header():
    """Renderiza cabeçalho da aplicação com design profissional"""
    # Import selected_model from config
    try:
        from src.config.model_config import SELECTED_MODEL as selected_model
    except ImportError:
        selected_model = "gpt-4"  # Fallback

    # Enhanced Professional Header
    st.markdown(
        f"""
        <div class="header-container">
            <h1 class="app-title">🤖 AGENTE IA TARGET v0.51</h1>
            <p class="app-subtitle">INTELIGÊNCIA ARTIFICIAL PARA ANÁLISE DE DADOS</p>
            <p class="app-description">
                Converse naturalmente com seus dados comerciais. Faça perguntas em linguagem natural
                e obtenha insights precisos através de análise inteligente.<br>
                <small style="opacity: 0.7;">Modelo: {selected_model}</small>
            </p>
            <div class="feature-icons">
                <div class="feature-item">
                    <div class="feature-icon">💬</div>
                    <span>Chat Natural</span>
                </div>
                <div class="feature-item">
                    <div class="feature-icon">📊</div>
                    <span>Análise Rápida</span>
                </div>
                <div class="feature-item">
                    <div class="feature-icon">🎯</div>
                    <span>Insights Precisos</span>
                </div>
                <div class="feature-item">
                    <div class="feature-icon">🚀</div>
                    <span>Resultados Instantâneos</span>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def _render_main_interface(agent, df):
    """Renderiza interface principal com layout otimizado"""
    # Sidebar configuration
    with st.sidebar:
        _render_sidebar(df)

    # Main content area with improved layout
    main_col1, main_col2, main_col3 = st.columns([0.5, 4, 0.5])

    with main_col2:
        # Main chat interface
        _render_chat_interface(agent)


def _render_sidebar(df):
    """Renderiza sidebar com informações e filtros"""
    st.markdown("## 📊 Informações do Dataset")
    st.markdown(f"**Registros:** {len(df):,}")
    st.markdown(f"**Período:** {df['Data'].min().strftime('%Y-%m-%d')} a {df['Data'].max().strftime('%Y-%m-%d')}")

    # Debug toggle
    debug_mode = st.toggle(
        "🔧 Modo Debug",
        value=st.session_state.get('debug_mode', False),
        help="Mostra queries SQL e informações técnicas"
    )
    st.session_state.debug_mode = debug_mode

    st.markdown("---")

    # Enhanced Filter management with new JSON system
    if 'last_context' in st.session_state and st.session_state.last_context:
        user_context = filter_user_friendly_context(st.session_state.last_context)
        create_enhanced_filter_manager(user_context, show_suggestions=True)

        # Mostrar resumo dos filtros ativos usando novo sistema
        try:
            json_manager = get_json_filter_manager(df)
            json_manager.sincronizar_com_contexto_agente(user_context)
            summary = json_manager.obter_resumo_filtros_ativos()
            if summary != "Nenhum filtro ativo":
                st.markdown(f"📊 *{summary}*")
        except Exception:
            # Fallback para sistema antigo se necessário
            pass
    else:
        create_enhanced_filter_manager({}, show_suggestions=False)


def _render_chat_interface(agent):
    """Renderiza interface de chat principal"""
    # Initialize chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []
        st.session_state.last_context = {}

        # Add welcome message as first assistant message
        welcome_msg = """👋 Olá! Sou o **Agente IA Target**, seu assistente para análise de dados comerciais.

Estou aqui para ajudá-lo a explorar e entender seus dados através de conversas naturais. Você pode me fazer perguntas como:
- "Quais são os produtos mais vendidos?"
- "Mostre o faturamento por região"
- "Analise as tendências de vendas"

Como posso ajudá-lo hoje?"""
        st.session_state.messages.append(
            {"role": "assistant", "content": welcome_msg}
        )

    # Delete Chat button
    col1, col2 = st.columns([1, 4])
    with col1:
        if st.button("🗑️ Limpar", type="secondary"):
            # Clear all session state related to chat
            st.session_state.messages = []
            if "session_user_id" in st.session_state:
                del st.session_state.session_user_id

            # CORREÇÃO: Limpar cache do agente
            if "cached_agent" in st.session_state:
                del st.session_state.cached_agent
            if "cached_df_agent" in st.session_state:
                del st.session_state.cached_df_agent

            # Clear agent persistent context
            if agent is not None:
                if hasattr(agent, 'clear_persistent_context'):
                    agent.clear_persistent_context()
                elif hasattr(agent, 'persistent_context'):
                    agent.persistent_context = {}
            # Clear disabled filters
            if 'disabled_filters' in st.session_state:
                st.session_state.disabled_filters.clear()
            if 'last_context' in st.session_state:
                del st.session_state.last_context

            # Clear JSON filter manager state
            from src.filters.json_filter_manager import reset_json_filter_manager
            reset_json_filter_manager()
            # Force app rerun to refresh everything
            st.rerun()

    st.markdown("<br>", unsafe_allow_html=True)

    # Display chat history
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            if message["role"] == "assistant":
                _render_assistant_message(message)
            else:
                st.markdown(message["content"])

    # Chat input
    if prompt := st.chat_input("💬 Faça sua pergunta sobre os dados comerciais..."):
        _handle_user_input(prompt, agent)


def _render_assistant_message(message):
    """Renderiza mensagem do assistente com visualizações"""
    # Render main content
    st.markdown(message["content"])

    # Render visualization if available
    if "visualization_data" in message and message["visualization_data"]:
        render_plotly_visualization(message["visualization_data"])

    # Render debug info if available
    if "debug_info" in message and message["debug_info"] and st.session_state.get('debug_mode', False):
        _render_debug_info(message["debug_info"], message.get("context"))


def _render_debug_info(debug_info, message_context=None):
    """Renderiza informações de debug"""
    with st.expander("🔧 Informações de Debug", expanded=False):

        # SQL Queries
        if "sql_queries" in debug_info and debug_info["sql_queries"]:
            st.markdown("### 📝 Queries SQL Executadas")
            for i, query in enumerate(debug_info["sql_queries"], 1):
                st.markdown(f"**Query {i}:**")
                st.code(format_sql_query(query), language="sql")


        # Show JSON Filter Structure from processed response
        st.markdown("### 🎯 Filtros JSON Detectados")
        if message_context:
            filter_json = _build_intelligent_filter_json(message_context)
            # CORREÇÃO: Sempre exibir JSON, mesmo quando vazio
            if filter_json and any(v for category in filter_json.values() for v in (category.values() if isinstance(category, dict) else []) if v is not None and v != [] and v != ""):
                st.json(filter_json)
            else:
                st.json({"message": "Nenhum filtro detectado no contexto", "context_fields": list(message_context.keys()) if message_context else []})
        else:
            st.json({"message": "Contexto vazio - nenhum filtro ativo"})

        # String Normalizations
        if "string_normalizations" in debug_info and debug_info["string_normalizations"]:
            st.markdown("### 🔤 Normalizações de String")
            for norm in debug_info["string_normalizations"]:
                st.markdown(f"- **{norm['column']}**: '{norm['original_value']}' → '{norm['normalized_value']}'")

        # Response timing
        if "response_time" in debug_info:
            st.markdown(f"### ⏱️ Tempo de Resposta: {debug_info['response_time']:.2f}s")


def _sincronizar_contexto_agente(agent):
    """
    Sincroniza contexto entre session_state e agent de forma robusta
    """
    # Se há contexto no session_state mas o agente está vazio, restaurar
    if (st.session_state.get('last_context') and
        (not hasattr(agent, 'persistent_context') or not agent.persistent_context)):
        agent.persistent_context = st.session_state.last_context.copy()
        return True
    return False


def _handle_user_input(prompt, agent):
    """Processa entrada do usuário com novo sistema JSON de filtros"""
    # Reset do flag de rerun para permitir nova atualização
    st.session_state.rerun_triggered = False

    # CORREÇÃO: Sincronizar contexto antes de processar
    context_restored = _sincronizar_contexto_agente(agent)
    if context_restored and st.session_state.get('debug_mode', False):
        st.info(f"🔄 Contexto sincronizado antes do processamento: {agent.persistent_context}")

    # Add user message to chat history
    st.session_state.messages.append({"role": "user", "content": prompt})

    # Display user message
    with st.chat_message("user"):
        st.markdown(prompt)

    # Process agent response
    with st.chat_message("assistant"):
        with st.spinner("🤖 Analisando..."):
            start_time = time.time()

            # Aplicar filtros desabilitados ao contexto antes de enviar para o agente
            current_context = getattr(agent, 'persistent_context', {}) if hasattr(agent, 'persistent_context') else {}
            disabled_filters = getattr(st.session_state, 'disabled_filters', set())

            if disabled_filters and current_context:
                from src.filters.json_filter_manager import get_json_filter_manager
                df_dataset = getattr(agent, 'df_normalized', None)
                if df_dataset is not None:
                    json_manager = get_json_filter_manager(df_dataset)
                    current_context = json_manager.aplicar_filtros_desabilitados(current_context, disabled_filters)
                    if hasattr(agent, 'persistent_context'):
                        agent.persistent_context = current_context

            try:
                # Clear execution state if needed
                if hasattr(agent, 'clear_execution_state'):
                    agent.clear_execution_state()

                # Get agent response
                response = agent.run(prompt)
                response_time = time.time() - start_time

                # Process response content
                response_content = str(response.content) if hasattr(response, 'content') else str(response)

                # Extract context and debug info
                context = {}
                debug_info = {"response_time": response_time}
                visualization_data = None

                if hasattr(agent, 'debug_info'):
                    debug_info.update(agent.debug_info)

                    # CORREÇÃO CRÍTICA: Extrair filtros ANTES de limpar debug_info
                    # Processar filtros usando APENAS as queries SQL
                    try:
                        df_dataset = getattr(agent, 'df_normalized', None)
                        # Usar debug_info local que contém as queries (não agent.debug_info)
                        if df_dataset is not None and 'sql_queries' in debug_info:
                            from src.filters.json_filter_manager import processar_filtros_apenas_sql

                            sql_queries = debug_info.get('sql_queries', [])
                            if sql_queries:
                                # Extrair filtros das queries SQL
                                updated_context_temp, filter_changes = processar_filtros_apenas_sql(
                                    sql_queries, {}, df_dataset
                                )
                                # Salvar resultado para uso posterior
                                debug_info['extracted_filters'] = updated_context_temp
                                debug_info['filter_changes'] = filter_changes
                    except Exception as e:
                        debug_info['filter_extraction_error'] = str(e)

                    agent.debug_info.clear()  # Clear for next query

                if hasattr(agent, 'persistent_context'):
                    context = agent.persistent_context.copy()

                    # CORREÇÃO CRÍTICA: Sempre detectar e restaurar contexto (não apenas em debug)
                    if 'last_agent_id' in st.session_state:
                        if st.session_state.last_agent_id != id(agent):
                            # AGENTE FOI RECRIADO - RESTAURAR CONTEXTO AUTOMATICAMENTE
                            if st.session_state.get('last_context'):
                                agent.persistent_context = st.session_state.last_context.copy()
                                context = agent.persistent_context.copy()
                                # Log apenas em debug mode
                                if st.session_state.get('debug_mode', False):
                                    st.warning(f"⚠️ AGENTE RECRIADO! Contexto restaurado: {context}")
                    st.session_state.last_agent_id = id(agent)

                    # DEBUG: Log contexto inicial do agente apenas em debug mode
                    if st.session_state.get('debug_mode', False):
                        st.info(f"🔍 Contexto INICIAL do agente: {context}")
                        st.info(f"🔍 Agent ID: {id(agent)}")
                        if hasattr(agent, '_creation_time'):
                            st.info(f"🔍 Agent criado em: {agent._creation_time}")
                        st.info(f"🔍 Session state keys: {list(st.session_state.keys())}")
                        st.info(f"🔍 Last context in session: {st.session_state.get('last_context', 'NONE')}")

                # SISTEMA LIMPO: Extrair filtros APENAS das queries SQL
                try:
                    df_dataset = getattr(agent, 'df_normalized', None)
                    if df_dataset is not None:
                        from src.filters.json_filter_manager import processar_filtros_apenas_sql

                        # DEBUG: Log contexto antes do processamento
                        if st.session_state.get('debug_mode', False):
                            st.info(f"🔍 **ANTES** do processamento:")
                            st.info(f"  - Contexto: {context}")
                            st.info(f"  - Total de filtros: {len(context)} campos")
                            st.info(f"  - SQL queries disponíveis: {debug_info.get('sql_queries', [])}")

                        # USAR FILTROS JÁ EXTRAÍDOS (antes da limpeza do debug_info)
                        if 'extracted_filters' in debug_info:
                            extracted_context = debug_info['extracted_filters']
                            filter_changes = debug_info.get('filter_changes', [])
                            # Merge contexto existente com filtros extraídos
                            updated_context = context.copy()
                            updated_context.update(extracted_context)
                        else:
                            # Fallback: nenhum filtro foi extraído
                            updated_context = context
                            filter_changes = ["INFO: Nenhum filtro extraído das queries SQL"]

                        # DEBUG: Log resultado do processamento
                        if st.session_state.get('debug_mode', False):
                            st.info(f"🔍 **DEPOIS** do processamento:")
                            st.info(f"  - Contexto atualizado: {updated_context}")
                            st.info(f"  - Total de filtros: {len(updated_context)} campos")
                            st.info(f"  - Queries processadas: {len(debug_info.get('sql_queries', []))}")

                            # Análise de diferenças
                            filtros_adicionados = set(updated_context.keys()) - set(context.keys())
                            filtros_removidos = set(context.keys()) - set(updated_context.keys())
                            filtros_modificados = {k for k in context.keys() & updated_context.keys()
                                                 if context[k] != updated_context[k]}

                            if filtros_adicionados:
                                st.success(f"➕ Filtros adicionados: {filtros_adicionados}")
                            if filtros_removidos:
                                st.error(f"➖ Filtros removidos: {filtros_removidos}")
                            if filtros_modificados:
                                st.warning(f"🔄 Filtros modificados: {filtros_modificados}")

                        # Sempre atualizar contexto após processamento
                        context = updated_context

                        # Sempre atualizar contexto persistente do agente
                        if hasattr(agent, 'update_persistent_context'):
                            agent.update_persistent_context(updated_context)
                        elif hasattr(agent, 'persistent_context'):
                            agent.persistent_context = updated_context

                        # Mostrar filtros extraídos se há mudanças
                        if filter_changes and any(not change.startswith("INFO:") for change in filter_changes):
                            with st.expander("🔍 Extração de Filtros das Queries SQL", expanded=True):
                                st.markdown("**Processamento realizado:**")
                                for change in filter_changes:
                                    if change.startswith("SQL:"):
                                        st.success(change)  # Sucesso na extração SQL
                                    elif change.startswith("  +"):
                                        st.markdown(f"- {change}")  # Detalhes dos filtros
                                    elif change.startswith("ℹ️"):
                                        st.info(change)  # Informações
                                    else:
                                        st.markdown(f"- {change}")

                except Exception as e:
                    # Se houver erro na extração, continuar normalmente sem fallback
                    if st.session_state.get('debug_mode', False):
                        st.error(f"❌ **ERRO** no processamento de filtros SQL:")
                        st.error(f"  - Exceção: {str(e)}")
                        st.error(f"  - Tipo: {type(e).__name__}")
                        st.error(f"  - Debug info disponível: {hasattr(agent, 'debug_info') and bool(agent.debug_info)}")
                        import traceback
                        st.error(f"  - Stack trace: {traceback.format_exc()}")
                    else:
                        st.warning(f"⚠️ Erro no processamento de filtros SQL: {str(e)}")

                    # Em caso de erro, manter contexto atual (não usar fallback)
                    pass

                # Extract visualization data if DuckDB tool has results
                if hasattr(agent, 'tools'):
                    for tool in agent.tools:
                        if hasattr(tool, 'last_result_df') and tool.last_result_df is not None:
                            df_result = tool.last_result_df
                            if not df_result.empty and len(df_result) <= 20:
                                visualization_data = _prepare_visualization_data(df_result)
                            break

                # Display response
                st.markdown(response_content)

                # Display visualization
                if visualization_data:
                    render_plotly_visualization(visualization_data)

                # Display response time
                st.markdown(f"⏱️ *Tempo de resposta: {response_time:.2f}s*")

                # Store message with all metadata
                assistant_message = {
                    "role": "assistant",
                    "content": response_content,
                    "context": context,
                    "debug_info": debug_info
                }

                if visualization_data:
                    assistant_message["visualization_data"] = visualization_data

                st.session_state.messages.append(assistant_message)

                # ATUALIZAÇÃO IMEDIATA DA SIDEBAR: Detectar se o contexto mudou
                previous_context = st.session_state.get('last_context', {})
                context_changed = context != previous_context

                st.session_state.last_context = context

                # DEBUG: Log estado final do contexto
                if st.session_state.get('debug_mode', False):
                    st.info(f"🔍 CONTEXTO FINAL salvo na sessão: {st.session_state.last_context}")
                    st.info(f"🔍 Agent context final: {getattr(agent, 'persistent_context', 'NONE')}")
                    st.info(f"🔍 Context changed: {context_changed}")

                # Display debug info if enabled
                if debug_info and st.session_state.get('debug_mode', False):
                    _render_debug_info(debug_info, context)

                # FORÇAR ATUALIZAÇÃO VISUAL IMEDIATA da sidebar se contexto mudou
                if context_changed and context:
                    # Marcar que filtros foram atualizados para trigger rerun
                    st.session_state.filters_just_updated = True
                    # Fazer rerun apenas uma vez para atualizar sidebar
                    if not st.session_state.get('rerun_triggered', False):
                        st.session_state.rerun_triggered = True
                        st.rerun()

            except Exception as e:
                error_msg = f"❌ **Erro:** {str(e)}"
                st.error(error_msg)
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": error_msg,
                    "context": {},
                    "debug_info": {"error": str(e), "response_time": time.time() - start_time}
                })



def _prepare_visualization_data(df_result):
    """Prepara dados para visualização automática"""
    if df_result.empty or len(df_result.columns) < 2:
        return None

    # Tentar identificar colunas de valor e rótulo
    numeric_cols = df_result.select_dtypes(include=['number']).columns
    text_cols = df_result.select_dtypes(include=['object', 'string']).columns

    if len(numeric_cols) >= 1 and len(text_cols) >= 1:
        value_col = numeric_cols[0]
        label_col = text_cols[0]

        # Detectar se é dados temporais
        is_temporal = any(
            keyword in label_col.lower()
            for keyword in ['data', 'mes', 'ano', 'periodo', 'date', 'month', 'year']
        )

        chart_type = 'line_chart' if is_temporal else 'bar_chart'

        # Preparar dados no formato esperado
        chart_data = df_result[[label_col, value_col]].copy()
        chart_data.columns = ['label' if not is_temporal else 'date', 'value']

        return {
            'type': chart_type,
            'data': chart_data,
            'has_data': True,
            'config': {
                'title': f'Top {len(chart_data)} Resultados',
                'value_format': 'currency' if 'valor' in value_col.lower() else 'number',
                'is_categorical_id': chart_data['label' if not is_temporal else 'date'].dtype == 'object'
            }
        }

    return None


def _render_footer():
    """Renderiza footer com logotipo da empresa"""
    # Footer Target Data Experience
    st.markdown("---")
    st.markdown("<br>", unsafe_allow_html=True)

    # Create footer with logo
    footer_col1, footer_col2, footer_col3 = st.columns([1, 2, 1])

    with footer_col2:
        # Company footer with modern styling
        st.markdown(
            """
            <div style="text-align: center; background: linear-gradient(135deg, #1a2332 0%, #2d3e50 100%);
                        padding: 30px; border-radius: 15px; margin: 20px 0; display: flex;
                        flex-direction: column; align-items: center; justify-content: center;">
                <div style="color: white; font-family: 'Arial', sans-serif; font-weight: 300;
                           letter-spacing: 6px; margin: 0; font-size: 24px;">T A R G E T</div>
                <div style="color: #e74c3c; font-family: 'Arial', sans-serif; font-weight: 300;
                          letter-spacing: 3px; margin: 8px 0 0 0; font-size: 12px;">D A T A &nbsp; E X P E R I E N C E</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    st.markdown("<br>", unsafe_allow_html=True)


def _build_intelligent_filter_json(message_context: Dict) -> Dict:
    """
    Constrói JSON de filtros de forma inteligente preservando granularidade temporal.

    Args:
        message_context: Contexto da mensagem processada

    Returns:
        Dict com estrutura JSON preservando granularidade de mês/ano
    """
    filter_json = {
        "periodo": {},
        "regiao": {
            "UF_Cliente": message_context.get("UF_Cliente"),
            "Municipio_Cliente": message_context.get("Municipio_Cliente")
        },
        "cliente": {
            "Cod_Cliente": message_context.get("Cod_Cliente"),
            "Cod_Segmento_Cliente": message_context.get("Cod_Segmento_Cliente")
        },
        "produto": {
            "Cod_Familia_Produto": message_context.get("Cod_Familia_Produto"),
            "Cod_Grupo_Produto": message_context.get("Cod_Grupo_Produto"),
            "Cod_Linha_Produto": message_context.get("Cod_Linha_Produto"),
            "Des_Linha_Produto": message_context.get("Des_Linha_Produto")
        },
        "representante": {
            "Cod_Vendedor": message_context.get("Cod_Vendedor"),
            "Cod_Regiao_Vendedor": message_context.get("Cod_Regiao_Vendedor")
        }
    }

    # LÓGICA INTELIGENTE PARA PERÍODO - PRESERVAR GRANULARIDADE
    try:
        from src.text_normalizer import normalizer

        # CORREÇÃO CRÍTICA: Verificar se há estrutura inicio/fim (novo formato)
        if message_context.get("inicio") and message_context.get("fim"):
            # Novo formato estruturado já está correto
            filter_json["periodo"] = {
                "inicio": message_context["inicio"],
                "fim": message_context["fim"]
            }

        # Verificar se há ranges de data no contexto (formato antigo)
        elif message_context.get("Data_>=") and message_context.get("Data_<"):
            data_inicio = message_context["Data_>="]
            data_fim = message_context["Data_<"]

            # NOVO: Converter ranges para estrutura mês/ano se possível
            structured_data = _convert_range_to_structured(data_inicio, data_fim)
            if structured_data:
                filter_json["periodo"] = structured_data
            else:
                # Fallback para ranges originais
                filter_json["periodo"] = {
                    "Data_>=": data_inicio,
                    "Data_<": data_fim
                }

        # Se há apenas Data simples, tentar preservar como estava
        elif message_context.get("Data"):
            filter_json["periodo"]["Data"] = message_context["Data"]

        # CORREÇÃO: Verificar se há campos de mês/ano diretos
        elif message_context.get("mes") and message_context.get("ano"):
            filter_json["periodo"] = {
                "mes": message_context["mes"],
                "ano": message_context["ano"]
            }

        # Se não há info temporal, deixar vazio
        else:
            # Período vazio será removido pelo _clean_empty_fields
            pass

    except Exception as e:
        # Fallback para formato antigo se algo der errado
        filter_json["periodo"] = {
            "error": f"Erro ao processar período: {str(e)}",
            "Data": message_context.get("Data"),
            "Data_>=": message_context.get("Data_>="),
            "Data_<": message_context.get("Data_<"),
            "inicio": message_context.get("inicio"),
            "fim": message_context.get("fim")
        }

    # Limpar campos vazios
    filter_json = _clean_empty_fields(filter_json)

    return filter_json


def _convert_range_to_structured(data_inicio: str, data_fim: str) -> Optional[Dict]:
    """
    Converte ranges de data para estrutura mês/ano se aplicável.

    Args:
        data_inicio: Data início (YYYY-MM-DD)
        data_fim: Data fim (YYYY-MM-DD)

    Returns:
        Dict com estrutura mês/ano ou None se não aplicável
    """
    try:
        from datetime import datetime, timedelta

        start = datetime.strptime(data_inicio, '%Y-%m-%d')
        end = datetime.strptime(data_fim, '%Y-%m-%d')

        # Verificar se é um mês específico
        if (start.day == 1 and
            end.day == 1 and
            start.year == end.year and
            end.month == start.month + 1):

            return {
                "mes": f"{start.month:02d}",
                "ano": str(start.year)
            }

        # Verificar se é intervalo de meses no mesmo ano
        elif (start.day == 1 and
              end.day == 1 and
              start.year == end.year and
              end.month > start.month):

            return {
                "inicio": {
                    "mes": f"{start.month:02d}",
                    "ano": str(start.year)
                },
                "fim": {
                    "mes": f"{end.month - 1:02d}",  # end é exclusivo
                    "ano": str(start.year)
                }
            }

        # Verificar se é ano completo
        elif (start.month == 1 and start.day == 1 and
              end.month == 1 and end.day == 1 and
              end.year == start.year + 1):

            return {"ano": str(start.year)}

    except Exception:
        pass

    return None


def _clean_empty_fields(data: Dict) -> Dict:
    """
    Remove campos None ou vazios do dicionário recursivamente.

    Args:
        data: Dicionário para limpar

    Returns:
        Dicionário limpo
    """
    if isinstance(data, dict):
        cleaned = {}
        for key, value in data.items():
            if isinstance(value, dict):
                cleaned_value = _clean_empty_fields(value)
                if cleaned_value:  # Só adicionar se dict não estiver vazio
                    cleaned[key] = cleaned_value
            elif value is not None and value != [] and value != "":
                cleaned[key] = value
        return cleaned
    else:
        return data


if __name__ == "__main__":
    main()
