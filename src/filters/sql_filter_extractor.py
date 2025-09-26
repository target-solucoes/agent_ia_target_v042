"""
Extrator de Filtros baseado em SQL - Versão Refatorada
Extrai filtros diretamente das cláusulas WHERE das queries SQL geradas,
mapeando para estrutura JSON esperada pelo sistema de filtros.
"""

import re
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
import copy


class SQLFilterExtractor:
    """
    Extrator que analisa queries SQL para gerar filtros em formato JSON
    """

    def __init__(self, df_dataset: Optional[pd.DataFrame] = None):
        """
        Inicializa o extrator com dataset para validação opcional

        Args:
            df_dataset: DataFrame com dados para validação de valores
        """
        self.df_dataset = df_dataset

        # Mapeamento de colunas SQL para categorias JSON
        self.column_mapping = {
            # Período
            'Data': 'periodo',
            'Data_>=': 'periodo',
            'Data_<': 'periodo',
            'Data_<=': 'periodo',
            'Data_>': 'periodo',
            'periodo': 'periodo',
            'mes': 'periodo',
            'ano': 'periodo',

            # Região
            'UF_Cliente': 'regiao',
            'Municipio_Cliente': 'regiao',
            'cidade': 'regiao',
            'estado': 'regiao',

            # Cliente
            'Cod_Cliente': 'cliente',
            'Cod_Segmento_Cliente': 'cliente',
            'cliente': 'cliente',

            # Produto
            'Cod_Familia_Produto': 'produto',
            'Cod_Grupo_Produto': 'produto',
            'Cod_Linha_Produto': 'produto',
            'Des_Linha_Produto': 'produto',
            'produto': 'produto',
            'familia': 'produto',
            'grupo': 'produto',
            'linha': 'produto',

            # Representante
            'Cod_Vendedor': 'representante',
            'Cod_Regiao_Vendedor': 'representante',
            'vendedor': 'representante',
            'representante': 'representante'
        }

    def extract_filters_from_sql(self, sql_query: str) -> Dict:
        """
        Extrai filtros de uma query SQL e retorna em formato JSON estruturado

        Args:
            sql_query: Query SQL para analisar

        Returns:
            Dict com filtros estruturados no formato JSON esperado
        """
        try:
            # Normalizar query
            normalized_query = re.sub(r'\s+', ' ', sql_query.strip())

            # Extrair cláusula WHERE
            where_context = self._extract_where_conditions(normalized_query)

            if not where_context:
                return self._get_empty_filter_structure()

            # Mapear para estrutura JSON
            json_filters = self._map_sql_to_json(where_context)

            return json_filters

        except Exception as e:
            return {"error": f"Erro ao extrair filtros do SQL: {str(e)}"}

    def _extract_where_conditions(self, sql_query: str) -> Dict:
        """
        Extrai condições da cláusula WHERE de forma robusta

        Args:
            sql_query: Query SQL normalizada

        Returns:
            Dict com condições extraídas
        """
        # Encontrar cláusula WHERE
        where_match = re.search(
            r'\bWHERE\b(.*?)(?:\bGROUP BY\b|\bORDER BY\b|\bHAVING\b|\bLIMIT\b|$)',
            sql_query, re.IGNORECASE
        )

        if not where_match:
            return {}

        where_clause = where_match.group(1).strip()
        conditions = {}

        # Padrões para extrair diferentes tipos de condições
        patterns = [
            # Igualdade com LOWER(): LOWER(coluna) = 'valor'
            (r"LOWER\((\w+)\)\s*=\s*'([^']*)'", 'equality_lower'),

            # Igualdade simples: coluna = 'valor'
            (r"(?<!LOWER\()(\w+)\s*=\s*'([^']*)'", 'equality'),

            # Igualdade com aspas duplas
            (r"(?:LOWER\()?(\w+)\)?\s*=\s*\"([^\"]*)\"", 'equality'),

            # LIKE com LOWER(): LOWER(coluna) LIKE 'valor'
            (r"LOWER\((\w+)\)\s+LIKE\s+'([^']*)'", 'like_lower'),

            # LIKE simples: coluna LIKE 'valor'
            (r"(?<!LOWER\()(\w+)\s+LIKE\s+'([^']*)'", 'like'),

            # Comparações com datas: coluna >= DATE '2024-01-01' ou coluna >= '2024-01-01'
            (r"(\w+)\s*([><=!]+)\s*(?:DATE\s*)?'([^']*)'", 'comparison'),

            # IN: coluna IN (valores)
            (r"(?:LOWER\()?(\w+)\)?\s+IN\s*\(([^)]+)\)", 'in_values'),

            # BETWEEN: coluna BETWEEN valor1 AND valor2
            (r"(\w+)\s+BETWEEN\s+([^\s]+)\s+AND\s+([^\s]+)", 'between')
        ]

        for pattern, condition_type in patterns:
            matches = re.finditer(pattern, where_clause, re.IGNORECASE)

            for match in matches:
                if condition_type in ['equality', 'equality_lower', 'like', 'like_lower']:
                    column = match.group(1)
                    value = match.group(2)

                    # Para LOWER(), converter valor também para uppercase no resultado
                    if condition_type.endswith('_lower'):
                        value = value.upper()

                    conditions[column] = value

                elif condition_type == 'comparison':
                    column = match.group(1)
                    operator = match.group(2)
                    value = match.group(3)

                    # Limpar valor de prefixos DATE
                    cleaned_value = value.strip('\'"')
                    if cleaned_value.upper().startswith('DATE'):
                        cleaned_value = cleaned_value[4:].strip('\'"')

                    conditions[f"{column}_{operator}"] = cleaned_value

                elif condition_type == 'in_values':
                    column = match.group(1)
                    values_str = match.group(2)

                    # Extrair valores individuais
                    values = re.findall(r"'([^']*)'|\"([^\"]*)\"", values_str)
                    if values:
                        clean_values = [v[0] or v[1] for v in values]
                        # Para campos UF, converter para maiúsculo
                        if column.lower() == 'uf_cliente':
                            clean_values = [v.upper() for v in clean_values]
                        conditions[column] = clean_values if len(clean_values) > 1 else clean_values[0]

                elif condition_type == 'between':
                    column = match.group(1)
                    start_val = match.group(2).strip('\'"')
                    end_val = match.group(3).strip('\'"')

                    conditions[f"{column}_>="] = start_val
                    conditions[f"{column}_<="] = end_val

        return conditions

    def _map_sql_to_json(self, sql_conditions: Dict) -> Dict:
        """
        Mapeia condições SQL para estrutura JSON de filtros

        Args:
            sql_conditions: Condições extraídas do SQL

        Returns:
            Dict com estrutura JSON de filtros
        """
        json_structure = self._get_empty_filter_structure()

        # Processar condições temporais primeiro (mais complexas)
        temporal_conditions = {}
        other_conditions = {}

        for key, value in sql_conditions.items():
            if any(temporal_key in key.lower() for temporal_key in ['data', 'periodo', 'mes', 'ano']):
                temporal_conditions[key] = value
            else:
                other_conditions[key] = value

        # Processar condições temporais
        if temporal_conditions:
            json_structure['periodo'] = self._process_temporal_conditions(temporal_conditions)

        # Processar outras condições
        for sql_column, value in other_conditions.items():
            # Remover sufixos de operador para mapear coluna
            base_column = re.sub(r'_[><=!]+$', '', sql_column)

            if base_column in self.column_mapping:
                category = self.column_mapping[base_column]

                if category in json_structure:
                    # Determinar campo específico na categoria
                    target_field = self._determine_target_field(base_column, category)

                    if target_field:
                        # Processar valor baseado no tipo
                        processed_value = self._process_field_value(value, target_field)
                        json_structure[category][target_field] = processed_value

        # Limpar campos vazios/nulos
        json_structure = self._clean_empty_fields(json_structure)

        return json_structure

    def _process_temporal_conditions(self, temporal_conditions: Dict) -> Dict:
        """
        Processa condições temporais para formato mês/ano

        Args:
            temporal_conditions: Condições relacionadas a data/período

        Returns:
            Dict com estrutura temporal formatada
        """
        periodo_result = {}

        # Verificar se temos range de datas
        date_gte = None
        date_lt = None
        date_lte = None

        for key, value in temporal_conditions.items():
            if 'Data_>=' in key:
                date_gte = value
            elif 'Data_<' in key:
                date_lt = value
            elif 'Data_<=' in key:
                date_lte = value
            elif key == 'Data':
                # Data específica
                periodo_result = self._convert_date_to_month_year(value)
                return periodo_result

        # Processar range de datas
        if date_gte and (date_lt or date_lte):
            end_date = date_lt or date_lte
            is_exclusive_end = bool(date_lt)  # Data_< é exclusivo

            periodo_result = self._convert_date_range_to_structured(
                date_gte, end_date, is_exclusive_end
            )
        elif date_gte:
            # Apenas data início
            start_info = self._convert_date_to_month_year(date_gte)
            if start_info:
                periodo_result['inicio'] = start_info
        elif date_lt or date_lte:
            # Apenas data fim
            end_date = date_lt or date_lte
            end_info = self._convert_date_to_month_year(end_date)
            if end_info:
                periodo_result['fim'] = end_info

        return periodo_result

    def _convert_date_to_month_year(self, date_str: str) -> Dict:
        """
        Converte string de data para formato mês/ano

        Args:
            date_str: String de data (YYYY-MM-DD)

        Returns:
            Dict com mes e ano
        """
        try:
            if isinstance(date_str, str) and len(date_str) >= 10:
                date_obj = datetime.strptime(date_str[:10], '%Y-%m-%d')
                return {
                    "mes": f"{date_obj.month:02d}",
                    "ano": str(date_obj.year)
                }
        except ValueError:
            pass

        return {}

    def _convert_date_range_to_structured(self, start_date: str, end_date: str, is_exclusive_end: bool) -> Dict:
        """
        Converte range de datas para estrutura início/fim

        Args:
            start_date: Data de início
            end_date: Data de fim
            is_exclusive_end: Se a data fim é exclusiva (Data_<)

        Returns:
            Dict com estrutura início/fim ou mês/ano único
        """
        try:
            start_obj = datetime.strptime(start_date[:10], '%Y-%m-%d')
            end_obj = datetime.strptime(end_date[:10], '%Y-%m-%d')

            # Para data exclusiva, ajustar para o dia anterior
            if is_exclusive_end:
                end_obj = end_obj - timedelta(days=1)

            # Verificar se é mês específico
            if (start_obj.day == 1 and
                start_obj.year == end_obj.year and
                start_obj.month == end_obj.month and
                end_obj.day >= 28):  # Final do mês

                return {
                    "mes": f"{start_obj.month:02d}",
                    "ano": str(start_obj.year)
                }

            # Verificar se é intervalo de meses
            elif start_obj.day == 1 and start_obj.year == end_obj.year:
                return {
                    "inicio": {
                        "mes": f"{start_obj.month:02d}",
                        "ano": str(start_obj.year)
                    },
                    "fim": {
                        "mes": f"{end_obj.month:02d}",
                        "ano": str(end_obj.year)
                    }
                }

            # Intervalo genérico
            else:
                return {
                    "inicio": self._convert_date_to_month_year(start_date),
                    "fim": self._convert_date_to_month_year(end_date)
                }

        except ValueError:
            # Fallback para formato simples
            return {
                "Data_>=": start_date,
                "Data_<" if is_exclusive_end else "Data_<=": end_date
            }

    def _determine_target_field(self, sql_column: str, category: str) -> Optional[str]:
        """
        Determina o campo específico na categoria baseado na coluna SQL

        Args:
            sql_column: Nome da coluna SQL
            category: Categoria do filtro

        Returns:
            Nome do campo específico ou None
        """
        # Mapeamentos diretos
        direct_mapping = {
            'UF_Cliente': 'UF_Cliente',
            'Municipio_Cliente': 'Municipio_Cliente',
            'Cod_Cliente': 'Cod_Cliente',
            'Cod_Segmento_Cliente': 'Cod_Segmento_Cliente',
            'Cod_Familia_Produto': 'Cod_Familia_Produto',
            'Cod_Grupo_Produto': 'Cod_Grupo_Produto',
            'Cod_Linha_Produto': 'Cod_Linha_Produto',
            'Des_Linha_Produto': 'Des_Linha_Produto',
            'Cod_Vendedor': 'Cod_Vendedor',
            'Cod_Regiao_Vendedor': 'Cod_Regiao_Vendedor'
        }

        if sql_column in direct_mapping:
            return direct_mapping[sql_column]

        # Mapeamentos por categoria
        if category == 'regiao':
            if 'uf' in sql_column.lower() or 'estado' in sql_column.lower():
                return 'UF_Cliente'
            elif 'municipio' in sql_column.lower() or 'cidade' in sql_column.lower():
                return 'Municipio_Cliente'

        elif category == 'cliente':
            if 'segmento' in sql_column.lower():
                return 'Cod_Segmento_Cliente'
            else:
                return 'Cod_Cliente'

        elif category == 'produto':
            if 'familia' in sql_column.lower():
                return 'Cod_Familia_Produto'
            elif 'grupo' in sql_column.lower():
                return 'Cod_Grupo_Produto'
            elif 'linha' in sql_column.lower():
                if 'des_' in sql_column.lower() or 'descri' in sql_column.lower():
                    return 'Des_Linha_Produto'
                else:
                    return 'Cod_Linha_Produto'

        elif category == 'representante':
            if 'regiao' in sql_column.lower():
                return 'Cod_Regiao_Vendedor'
            else:
                return 'Cod_Vendedor'

        return None

    def _process_field_value(self, value: Any, field_name: str) -> Any:
        """
        Processa valor do campo baseado no tipo de campo

        Args:
            value: Valor original
            field_name: Nome do campo

        Returns:
            Valor processado
        """
        # Para listas, retornar como estão
        if isinstance(value, list):
            return value

        # Para UF, garantir que seja maiúsculo
        if field_name == 'UF_Cliente' and isinstance(value, str):
            return value.upper()

        # Para outros campos de string, retornar como está
        return value

    def _clean_empty_fields(self, json_structure: Dict) -> Dict:
        """
        Remove campos vazios/nulos da estrutura JSON

        Args:
            json_structure: Estrutura JSON com possíveis campos vazios

        Returns:
            Estrutura limpa
        """
        cleaned = {}

        for category, fields in json_structure.items():
            if isinstance(fields, dict):
                non_empty_fields = {}
                for field, value in fields.items():
                    if value is not None and value != [] and value != "":
                        non_empty_fields[field] = value

                if non_empty_fields:
                    cleaned[category] = non_empty_fields
            elif fields is not None and fields != [] and fields != "":
                cleaned[category] = fields

        return cleaned

    def _get_empty_filter_structure(self) -> Dict:
        """
        Retorna estrutura vazia de filtros JSON

        Returns:
            Dict com estrutura base de filtros
        """
        return {
            "periodo": {},
            "regiao": {
                "UF_Cliente": None,
                "Municipio_Cliente": None
            },
            "cliente": {
                "Cod_Cliente": None,
                "Cod_Segmento_Cliente": None
            },
            "produto": {
                "Cod_Familia_Produto": None,
                "Cod_Grupo_Produto": None,
                "Cod_Linha_Produto": None,
                "Des_Linha_Produto": None
            },
            "representante": {
                "Cod_Vendedor": None,
                "Cod_Regiao_Vendedor": None
            }
        }

    def extract_filters_from_multiple_queries(self, sql_queries: List[str]) -> Dict:
        """
        Extrai filtros de múltiplas queries SQL e combina os resultados

        Args:
            sql_queries: Lista de queries SQL

        Returns:
            Dict com filtros combinados
        """
        combined_filters = self._get_empty_filter_structure()

        for query in sql_queries:
            query_filters = self.extract_filters_from_sql(query)

            # Combinar filtros (merge inteligente)
            combined_filters = self._merge_filter_structures(combined_filters, query_filters)

        return self._clean_empty_fields(combined_filters)

    def _merge_filter_structures(self, base: Dict, new_filters: Dict) -> Dict:
        """
        Faz merge de duas estruturas de filtros

        Args:
            base: Estrutura base
            new_filters: Novos filtros para adicionar

        Returns:
            Estrutura combinada
        """
        result = copy.deepcopy(base)

        for category, fields in new_filters.items():
            if category == 'error':
                continue

            if category not in result:
                result[category] = fields
                continue

            if isinstance(fields, dict):
                if not isinstance(result[category], dict):
                    result[category] = {}

                for field, value in fields.items():
                    if value is not None and value != [] and value != "":
                        result[category][field] = value
            else:
                if fields is not None and fields != [] and fields != "":
                    result[category] = fields

        return result


def extract_filters_from_sql(sql_query: str, df_dataset: Optional[pd.DataFrame] = None) -> Dict:
    """
    Função de conveniência para extrair filtros de uma query SQL

    Args:
        sql_query: Query SQL para analisar
        df_dataset: DataFrame opcional para validação

    Returns:
        Dict com filtros em formato JSON
    """
    extractor = SQLFilterExtractor(df_dataset)
    return extractor.extract_filters_from_sql(sql_query)


def extract_filters_from_debug_info(debug_info: Dict, df_dataset: Optional[pd.DataFrame] = None) -> Dict:
    """
    Extrai filtros das queries SQL armazenadas no debug_info do agente

    Args:
        debug_info: Dict com informações de debug do agente
        df_dataset: DataFrame opcional para validação

    Returns:
        Dict com filtros extraídos e combinados
    """
    if not debug_info or 'sql_queries' not in debug_info:
        return {}

    sql_queries = debug_info['sql_queries']
    if not sql_queries:
        return {}

    extractor = SQLFilterExtractor(df_dataset)
    return extractor.extract_filters_from_multiple_queries(sql_queries)