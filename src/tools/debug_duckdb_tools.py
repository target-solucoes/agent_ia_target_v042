"""
Ferramenta DuckDB Otimizada com normalização automática de strings e captura de contexto
"""

from agno.tools.duckdb import DuckDbTools
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
# from parsers.sql_context_parser import extract_where_clause_context  # Removido - agora usando sistema JSON
import pandas as pd
import re


class DebugDuckDbTools(DuckDbTools):
    """
    Classe customizada de DuckDbTools que aplica normalização automática
    de strings e captura contexto das queries SQL
    """

    def __init__(self, debug_info_ref=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.debug_info_ref = debug_info_ref
        self.last_result_df = None  # Armazenar último DataFrame resultado

    def _normalize_query_strings(self, query: str) -> str:
        """Aplica normalização LOWER() automaticamente a todas as comparações de strings na query"""

        # Pattern para detectar comparações de strings: coluna = 'valor', coluna LIKE 'valor', etc.
        # Captura: operador de comparação, nome da coluna, operador, valor entre aspas
        patterns = [
            # Igualdade: WHERE coluna = 'valor'
            (r"(\w+)\s*(=)\s*'([^']*)'", r"LOWER(\1) \2 '\3'"),
            # LIKE: WHERE coluna LIKE 'valor'
            (r"(\w+)\s+(LIKE)\s+'([^']*)'", r"LOWER(\1) \2 '\3'"),
            # Igualdade com aspas duplas: WHERE coluna = "valor"
            (r"(\w+)\s*(=)\s*\"([^\"]*)\"", r"LOWER(\1) \2 '\3'"),
            # LIKE com aspas duplas: WHERE coluna LIKE "valor"
            (r"(\w+)\s+(LIKE)\s+\"([^\"]*)\"", r"LOWER(\1) \2 '\3'"),
        ]

        normalized_query = query
        applied_normalizations = []

        for pattern, replacement in patterns:
            # Encontrar todas as correspondências
            matches = re.finditer(pattern, normalized_query, re.IGNORECASE)

            for match in matches:
                column = match.group(1)
                operator = match.group(2)
                value = match.group(3)

                # Converter o valor para lowercase também
                normalized_value = value.lower()

                # Aplicar a substituição com LOWER() na coluna e valor normalizado
                old_text = match.group(0)
                new_text = f"LOWER({column}) {operator} '{normalized_value}'"

                normalized_query = normalized_query.replace(old_text, new_text)
                applied_normalizations.append({
                    "column": column,
                    "operator": operator,
                    "original_value": value,
                    "normalized_value": normalized_value
                })

        # Log das normalizações aplicadas para debug
        if applied_normalizations and self.debug_info_ref and hasattr(self.debug_info_ref, "debug_info"):
            if "string_normalizations" not in self.debug_info_ref.debug_info:
                self.debug_info_ref.debug_info["string_normalizations"] = []
            self.debug_info_ref.debug_info["string_normalizations"].extend(applied_normalizations)

        return normalized_query

    def run_query(self, query: str) -> str:
        """Override do método run_query com normalização automática de strings e captura de contexto"""

        # APLICAR NORMALIZAÇÃO AUTOMÁTICA de todas as strings na query
        normalized_query = self._normalize_query_strings(query)

        # Executar a query normalizada
        result = super().run_query(normalized_query)

        # CAPTURAR DADOS DO RESULTADO para visualização
        try:
            # Tentar executar novamente a query para capturar DataFrame
            if hasattr(self, 'connection') and self.connection:
                df_result = self.connection.execute(normalized_query).df()
                if not df_result.empty:
                    self.last_result_df = df_result
        except Exception as e:
            # Se falhar, tentar extrair dados do resultado textual
            self.last_result_df = self._parse_result_to_dataframe(result)

        # Debug info e context extraction
        if self.debug_info_ref is not None and hasattr(
            self.debug_info_ref, "debug_info"
        ):
            if "sql_queries" not in self.debug_info_ref.debug_info:
                self.debug_info_ref.debug_info["sql_queries"] = []
            if "query_contexts" not in self.debug_info_ref.debug_info:
                self.debug_info_ref.debug_info["query_contexts"] = []

            # Usar a query normalizada final
            clean_query = normalized_query.strip()
            if (
                clean_query
                and clean_query not in self.debug_info_ref.debug_info["sql_queries"]
            ):
                self.debug_info_ref.debug_info["sql_queries"].append(clean_query)

                # SEMPRE extrair contexto, mesmo que vazio
                # context = extract_where_clause_context(clean_query)  # Removido - agora usando sistema JSON
                context = {}  # Placeholder - filtros agora extraídos via JSON response
                # Adicionar contexto mesmo se vazio (para garantir que sempre apareça)
                self.debug_info_ref.debug_info["query_contexts"].append(context if context else {})

        return result

    def _parse_result_to_dataframe(self, result_text):
        """Converte resultado textual em DataFrame quando possível"""
        try:
            # Procurar por padrões de tabela no resultado
            lines = result_text.split('\n')
            data_rows = []

            # Procurar por linhas que parecem dados tabulares
            for line in lines:
                line = line.strip()
                if '|' in line or '\t' in line:
                    # Possível linha de dados
                    if '|' in line:
                        cells = [cell.strip() for cell in line.split('|') if cell.strip()]
                    else:
                        cells = [cell.strip() for cell in line.split('\t') if cell.strip()]

                    if len(cells) >= 2:
                        # Tentar converter última célula para número
                        try:
                            value = float(cells[-1].replace(',', '').replace('$', ''))
                            data_rows.append({
                                'label': cells[0],
                                'value': value
                            })
                        except:
                            continue

            return pd.DataFrame(data_rows) if data_rows else None
        except:
            return None