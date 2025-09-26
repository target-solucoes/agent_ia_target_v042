"""
Configurações do agente e hierarquia de colunas
"""

# HIERARQUIA DE COLUNAS: Define níveis hierárquicos para filtros inteligentes
# IMPORTANTE: Filtros só se conflitam dentro da MESMA categoria hierárquica
COLUMN_HIERARCHY = {
    'cliente': [
        'Cod_Cliente',        # Mais específico (cliente individual)
        'Cod_Segmento_Cliente'  # Segmento do cliente (mais amplo)
    ],
    'regiao': [
        'Municipio_Cliente',  # Cidade do cliente (mais específico)
        'UF_Cliente'          # Estado do cliente (mais amplo)
    ],
    'produto': [
        'Cod_Produto',        # Produto específico
        'Cod_Familia_Produto', # Família do produto
        'Cod_Grupo_Produto',  # Grupo do produto
        'Cod_Linha_Produto',  # Linha do produto
        'Des_Linha_Produto'   # Descrição da linha (mais amplo)
    ],
    'vendedor': [
        'Cod_Vendedor',       # Vendedor específico
        'Cod_Regiao_Vendedor' # Região do vendedor (mais amplo)
    ]
}

# Configurações do agente
AGENT_CONFIG = {
    "show_tool_calls": False,  # Será overridden por debug_mode
    "markdown": True,
    "run_code": True,
    "pip_install": False
}