"""
Configurações de modelo e API
"""

import os
from dotenv import load_dotenv

load_dotenv()

# Configuração do modelo OpenAI
SELECTED_MODEL = "gpt-5-nano-2025-08-07"

# Configuração da API
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

# Configurações de dados
DATA_CONFIG = {
    "data_path": "data/raw/DadosComercial_resumido_v02.parquet",
    "alias_mapping_path": "data/mappings/alias.yaml"
}