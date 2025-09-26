"""
Carregadores de dados e inicializadores de agente
"""

import streamlit as st
import pandas as pd
import uuid
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from config.model_config import DATA_CONFIG
from chatbot_agents import create_agent


@st.cache_data
def load_parquet_data():
    """Carrega arquivo Parquet com tratamento robusto de codificação"""
    data_path = DATA_CONFIG["data_path"]

    # Method 1: Try direct pandas loading
    try:
        with st.spinner("🔄 Carregando dados..."):
            df = pd.read_parquet(data_path)

            # Process string columns for encoding issues
            string_cols = df.select_dtypes(include=["object"]).columns
            for col in string_cols:
                try:
                    # Convert to string and clean encoding
                    original_values = df[col].fillna("")
                    cleaned_values = []

                    for val in original_values:
                        if isinstance(val, bytes):
                            # Handle bytes
                            try:
                                cleaned_val = val.decode("utf-8", errors="replace")
                            except:
                                cleaned_val = str(val)
                        else:
                            # Handle strings with potential encoding issues
                            cleaned_val = (
                                str(val)
                                .encode("utf-8", errors="ignore")
                                .decode("utf-8")
                            )
                        cleaned_values.append(cleaned_val)

                    df[col] = cleaned_values

                except Exception as col_error:
                    # If column processing fails, keep original
                    st.warning(
                        f"⚠️ Mantendo coluna {col} original devido a: {col_error}"
                    )
                    continue

            return df, None

    except Exception as e:
        return None, f"Erro ao carregar dados: {str(e)}"


def initialize_agent():
    """
    Inicializa o agente DuckDB configurado com memória temporária baseada em sessão
    CORREÇÃO: Removido cache para garantir sincronização de contexto
    """
    try:
        # Gerar um ID único para a sessão do Streamlit se não existir
        if "session_user_id" not in st.session_state:
            st.session_state.session_user_id = str(uuid.uuid4())

        # CORREÇÃO: Versão do prompt para invalidar cache quando necessário
        import hashlib
        import os
        prompt_file = os.path.join(os.path.dirname(__file__), '..', 'prompts', 'chatbot_prompt.py')
        try:
            with open(prompt_file, 'r', encoding='utf-8') as f:
                prompt_content = f.read()
            prompt_hash = hashlib.md5(prompt_content.encode()).hexdigest()[:8]
        except:
            prompt_hash = "unknown"

        # Verificar se já existe um agente na sessão e se o prompt não mudou
        if ("cached_agent" in st.session_state and "cached_df_agent" in st.session_state and
            st.session_state.get('cached_prompt_hash') == prompt_hash):

            agent = st.session_state.cached_agent
            df_agent = st.session_state.cached_df_agent

            # SEMPRE sincronizar contexto do session_state para o agente
            if st.session_state.get('last_context'):
                agent.persistent_context = st.session_state.last_context.copy()
                if st.session_state.get('debug_mode', False):
                    print(f"🔄 CONTEXTO SINCRONIZADO com agente existente: {agent.persistent_context}")

            return agent, df_agent, None

        # Criar novo agente se não existe ou prompt mudou
        import time
        current_time = time.time()

        agent, df_agent = create_agent(session_user_id=st.session_state.session_user_id)

        # Marcar o agente com timestamp
        agent._creation_time = current_time

        # SEMPRE restaurar contexto do session_state se disponível
        if st.session_state.get('last_context'):
            agent.persistent_context = st.session_state.last_context.copy()
            print(f"🔄 CONTEXTO RESTAURADO na criação do agente: {agent.persistent_context}")

        # Cache na sessão com hash do prompt
        st.session_state.cached_agent = agent
        st.session_state.cached_df_agent = df_agent
        st.session_state.cached_prompt_hash = prompt_hash

        # Log criação
        if st.session_state.get('debug_mode', False):
            print(f"🔍 NOVO AGENTE CRIADO em {current_time} - ID: {id(agent)} - Prompt hash: {prompt_hash}")

        return agent, df_agent, None
    except Exception as e:
        return None, None, f"Erro ao inicializar agente: {str(e)}"