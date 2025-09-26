"""
Parser de contexto SQL para extrair filtros de cláusulas WHERE
Versão melhorada com detecção de padrões complexos
"""

import re
import pandas as pd
from typing import Dict, List, Tuple, Optional


def extract_where_clause_context(sql_query):
    """
    Extrai o contexto da cláusula WHERE de uma query SQL e retorna em formato JSON

    Args:
        sql_query (str): Query SQL para extrair contexto

    Returns:
        dict: Dicionário com filtros extraídos da cláusula WHERE
    """
    try:
        # Normalizar a query (remover quebras de linha e múltiplos espaços)
        normalized_query = re.sub(r'\s+', ' ', sql_query.strip())

        # Encontrar a cláusula WHERE
        where_match = re.search(r'\bWHERE\b(.*?)(?:\bGROUP BY\b|\bORDER BY\b|\bHAVING\b|\bLIMIT\b|$)',
                               normalized_query, re.IGNORECASE)

        if not where_match:
            return {}

        where_clause = where_match.group(1).strip()
        context = {}

        # Buscar por padrões de filtro na cláusula WHERE
        # Igualdade com aspas simples: coluna = 'valor' OU LOWER(coluna) = 'valor'
        equality_single = re.findall(r"(?:LOWER\()?(\w+)\)?\s*=\s*'([^']*)'", where_clause, re.IGNORECASE)
        for column, value in equality_single:
            if column not in context:
                context[column] = value

        # Igualdade com aspas duplas: coluna = "valor" OU LOWER(coluna) = "valor"
        equality_double = re.findall(r"(?:LOWER\()?(\w+)\)?\s*=\s*\"([^\"]*)\"", where_clause, re.IGNORECASE)
        for column, value in equality_double:
            if column not in context:
                context[column] = value

        # LIKE com aspas simples: coluna LIKE 'valor' OU LOWER(coluna) LIKE 'valor'
        like_single = re.findall(r"(?:LOWER\()?(\w+)\)?\s+LIKE\s+'([^']*)'", where_clause, re.IGNORECASE)
        for column, value in like_single:
            if column not in context:
                context[column] = value

        # LIKE com aspas duplas: coluna LIKE "valor" OU LOWER(coluna) LIKE "valor"
        like_double = re.findall(r"(?:LOWER\()?(\w+)\)?\s+LIKE\s+\"([^\"]*)\"", where_clause, re.IGNORECASE)
        for column, value in like_double:
            if column not in context:
                context[column] = value

        # IN: coluna IN (...) OU LOWER(coluna) IN (...)
        in_clauses = re.findall(r"(?:LOWER\()?(\w+)\)?\s+IN\s*\([^)]+\)", where_clause, re.IGNORECASE)
        for column in in_clauses:
            key = f"{column}_IN"
            if key not in context:
                context[key] = "lista_valores"

        # Comparações: coluna > valor, coluna < valor, etc. OU LOWER(coluna) > valor
        # Regex melhorada para capturar DATE 'valor', 'valor' e "valor"
        comparisons = re.findall(r"(?:LOWER\()?(\w+)\)?\s*([><=!]+)\s*((?:DATE\s*)?'[^']*'|(?:DATE\s*)?\"[^\"]*\"|[^\s'\"]+)", where_clause, re.IGNORECASE)
        for column, operator, value in comparisons:
            if operator != '=':  # Não sobrescrever igualdades já processadas
                key = f"{column}_{operator}"
                if key not in context:
                    # Limpar o valor capturado
                    cleaned_value = value.strip()
                    if cleaned_value.upper().startswith('DATE'):
                        cleaned_value = cleaned_value[4:].strip()  # Remove 'DATE'
                    cleaned_value = cleaned_value.strip('\'"')  # Remove aspas
                    context[key] = cleaned_value

        # === PROCESSAMENTO ESPECIAL PARA FILTROS DE DATA COM PRESERVAÇÃO DE GRANULARIDADE ===
        # Detectar ranges de data e formatá-los preservando granularidade de mês/ano
        context = _enhance_temporal_context_with_granularity(context)

        # === PROCESSAMENTO ADICIONAL PARA PADRÕES COMPLEXOS ===
        context = _enhance_context_with_advanced_patterns(context, where_clause)

        return context

    except Exception as e:
        # Em caso de erro, retornar contexto básico
        return {"erro_parsing": str(e)}


def _enhance_context_with_advanced_patterns(context: Dict, where_clause: str) -> Dict:
    """
    Detecta padrões complexos adicionais na cláusula WHERE
    """
    enhanced_context = context.copy()

    # 1. Detectar operadores NOT e negações
    not_patterns = re.findall(r"(?:NOT\s+)?(?:LOWER\()?(\w+)\)?\s+(?:NOT\s+)?(?:=|LIKE|IN)\s*([^'\"]*?'[^']*'|[^'\"]*?\"[^\"]*\"|[^\s,)]+)", where_clause, re.IGNORECASE)
    for column, value in not_patterns:
        if re.search(r'\bNOT\b', value, re.IGNORECASE):
            enhanced_context[f"{column}_NOT"] = value.replace('NOT', '').strip()

    # 2. Detectar ranges numéricos (BETWEEN)
    between_patterns = re.findall(r"(?:LOWER\()?(\w+)\)?\s+BETWEEN\s+([^\s]+)\s+AND\s+([^\s]+)", where_clause, re.IGNORECASE)
    for column, start_val, end_val in between_patterns:
        enhanced_context[f"{column}_MIN"] = start_val.strip('\'"')
        enhanced_context[f"{column}_MAX"] = end_val.strip('\'"')

    # 3. Detectar múltiplos valores em IN
    in_values_patterns = re.findall(r"(?:LOWER\()?(\w+)\)?\s+IN\s*\(([^)]+)\)", where_clause, re.IGNORECASE)
    for column, values_str in in_values_patterns:
        # Extrair valores individuais
        values = re.findall(r"'([^']*)'|\"([^\"]*)\"", values_str)
        if values:
            # Flatten the tuples from findall
            clean_values = [v[0] or v[1] for v in values]
            enhanced_context[f"{column}_VALUES"] = clean_values

    # 4. Detectar condições complexas com OR
    or_conditions = _detect_or_conditions(where_clause)
    if or_conditions:
        enhanced_context["_or_conditions"] = or_conditions

    # 5. Detectar joins implícitos e relacionamentos
    joins = _detect_implicit_joins(where_clause)
    if joins:
        enhanced_context["_table_relationships"] = joins

    return enhanced_context


def _detect_or_conditions(where_clause: str) -> List[Dict]:
    """
    Detecta condições OR complexas na cláusula WHERE
    """
    or_conditions = []

    # Procurar por padrões OR
    or_parts = re.split(r'\bOR\b', where_clause, flags=re.IGNORECASE)

    if len(or_parts) > 1:
        for part in or_parts:
            part = part.strip()
            # Extrair condições individuais de cada parte OR
            conditions = extract_where_clause_context(f"SELECT * FROM table WHERE {part}")
            if conditions and not conditions.get("erro_parsing"):
                or_conditions.append(conditions)

    return or_conditions


def _detect_implicit_joins(where_clause: str) -> List[Dict]:
    """
    Detecta relacionamentos entre tabelas baseados na cláusula WHERE
    """
    relationships = []

    # Padrão para detectar joins implícitos: tabela1.campo = tabela2.campo
    join_patterns = re.findall(r"(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+)", where_clause, re.IGNORECASE)

    for table1, field1, table2, field2 in join_patterns:
        relationships.append({
            "left_table": table1,
            "left_field": field1,
            "right_table": table2,
            "right_field": field2,
            "join_type": "implicit_inner"
        })

    return relationships


def _enhance_temporal_context_with_granularity(context: Dict) -> Dict:
    """
    Aprimora o contexto temporal preservando granularidade de mês/ano.

    Args:
        context: Contexto extraído da query SQL

    Returns:
        Contexto aprimorado com informações de granularidade temporal
    """
    enhanced_context = context.copy()

    # Detectar ranges de data e formatá-los adequadamente
    if "Data_>=" in context and "Data_<=" in context:
        try:
            data_inicio = context["Data_>="]
            data_fim = context["Data_<="]
            enhanced_context = _process_date_range(enhanced_context, data_inicio, data_fim, "<=")
        except Exception:
            pass

    elif "Data_>=" in context and "Data_<" in context:
        try:
            data_inicio = context["Data_>="]
            data_fim = context["Data_<"]
            enhanced_context = _process_date_range(enhanced_context, data_inicio, data_fim, "<")
        except Exception:
            pass

    elif "Data_>=" in context:
        try:
            data_inicio = context["Data_>="]
            enhanced_context["Data_Inicio"] = data_inicio
        except Exception:
            pass

    elif "Data_<" in context or "Data_<=" in context:
        try:
            data_fim = context.get("Data_<") or context.get("Data_<=")
            enhanced_context["Data_Fim"] = data_fim
        except Exception:
            pass

    return enhanced_context


def _process_date_range(context: Dict, data_inicio: str, data_fim: str, fim_operator: str) -> Dict:
    """
    Processa range de datas tentando preservar granularidade de mês/ano.

    Args:
        context: Contexto atual
        data_inicio: Data de início
        data_fim: Data de fim
        fim_operator: Operador da data fim ("<" ou "<=")

    Returns:
        Contexto com informações de granularidade preservadas
    """
    try:
        from datetime import datetime

        # Tentar converter ranges para estrutura mês/ano
        structured_data = _convert_sql_range_to_structured(data_inicio, data_fim, fim_operator)

        if structured_data:
            # Adicionar informações estruturadas ao contexto
            if "mes" in structured_data and "ano" in structured_data:
                # Mês específico
                context["_temporal_granularity"] = "month"
                context["_temporal_mes"] = structured_data["mes"]
                context["_temporal_ano"] = structured_data["ano"]
                context["Periodo"] = f"{structured_data['mes']}/{structured_data['ano']}"

            elif "inicio" in structured_data and "fim" in structured_data:
                # Intervalo de meses
                context["_temporal_granularity"] = "month_range"
                context["_temporal_inicio"] = structured_data["inicio"]
                context["_temporal_fim"] = structured_data["fim"]
                inicio = structured_data["inicio"]
                fim = structured_data["fim"]
                context["Periodo"] = f"{inicio['mes']}/{inicio['ano']} a {fim['mes']}/{fim['ano']}"

            elif "ano" in structured_data:
                # Ano completo
                context["_temporal_granularity"] = "year"
                context["_temporal_ano"] = structured_data["ano"]
                context["Periodo"] = structured_data["ano"]

        else:
            # Fallback para formato tradicional
            context["Periodo"] = f"{data_inicio} a {data_fim}"

    except Exception:
        # Em caso de erro, usar formato tradicional
        context["Periodo"] = f"{data_inicio} a {data_fim}"

    return context


def _convert_sql_range_to_structured(data_inicio: str, data_fim: str, fim_operator: str) -> Optional[Dict]:
    """
    Converte range SQL para estrutura mês/ano se aplicável.

    Args:
        data_inicio: Data início (YYYY-MM-DD)
        data_fim: Data fim (YYYY-MM-DD)
        fim_operator: Operador da data fim ("<" ou "<=")

    Returns:
        Dict com estrutura mês/ano ou None se não aplicável
    """
    try:
        from datetime import datetime, timedelta

        start = datetime.strptime(data_inicio, '%Y-%m-%d')
        end = datetime.strptime(data_fim, '%Y-%m-%d')

        # Para operador "<", a data fim é exclusiva
        if fim_operator == "<":
            # Ajustar para data inclusiva para análise
            end = end - timedelta(days=1)

        # Verificar se é um mês específico
        if (start.day == 1 and
            ((fim_operator == "<" and end.day >= 28) or (fim_operator == "<=" and end.day >= 28)) and
            start.year == end.year and
            start.month == end.month):

            return {
                "mes": f"{start.month:02d}",
                "ano": str(start.year)
            }

        # Verificar se é intervalo de meses no mesmo ano
        elif (start.day == 1 and
              start.year == end.year and
              end.month >= start.month):

            return {
                "inicio": {
                    "mes": f"{start.month:02d}",
                    "ano": str(start.year)
                },
                "fim": {
                    "mes": f"{end.month:02d}",
                    "ano": str(start.year)
                }
            }

        # Verificar se é ano completo
        elif (start.month == 1 and start.day == 1 and
              end.month == 12 and end.day >= 28 and
              start.year == end.year):

            return {"ano": str(start.year)}

    except Exception:
        pass

    return None


def extract_context_with_metadata(sql_query: str) -> Dict:
    """
    Extrai contexto com metadados adicionais sobre a query

    Returns:
        Dict contendo:
        - filters: Filtros extraídos
        - metadata: Informações sobre a query (agregações, ordenação, etc.)
        - complexity: Nível de complexidade da query
    """
    try:
        # Extrair contexto básico
        filters = extract_where_clause_context(sql_query)

        # Detectar metadados adicionais
        metadata = _extract_query_metadata(sql_query)

        # Calcular complexidade
        complexity = _calculate_query_complexity(sql_query, filters, metadata)

        return {
            "filters": filters,
            "metadata": metadata,
            "complexity": complexity
        }

    except Exception as e:
        return {
            "filters": {"erro_parsing": str(e)},
            "metadata": {},
            "complexity": "unknown"
        }


def _extract_query_metadata(sql_query: str) -> Dict:
    """
    Extrai metadados sobre a estrutura da query
    """
    metadata = {}

    normalized_query = re.sub(r'\s+', ' ', sql_query.strip())

    # Detectar agregações
    agg_functions = re.findall(r'\b(COUNT|SUM|AVG|MAX|MIN|GROUP_CONCAT)\s*\(', normalized_query, re.IGNORECASE)
    if agg_functions:
        metadata["aggregations"] = [func.lower() for func in agg_functions]

    # Detectar GROUP BY
    group_by_match = re.search(r'\bGROUP BY\s+([^ORDER\s]+)(?:\bORDER BY\b|$)', normalized_query, re.IGNORECASE)
    if group_by_match:
        metadata["group_by_fields"] = [field.strip() for field in group_by_match.group(1).split(',')]

    # Detectar ORDER BY
    order_by_match = re.search(r'\bORDER BY\s+([^LIMIT\s]+)(?:\bLIMIT\b|$)', normalized_query, re.IGNORECASE)
    if order_by_match:
        metadata["order_by_fields"] = [field.strip() for field in order_by_match.group(1).split(',')]

    # Detectar LIMIT
    limit_match = re.search(r'\bLIMIT\s+(\d+)', normalized_query, re.IGNORECASE)
    if limit_match:
        metadata["limit"] = int(limit_match.group(1))

    # Detectar HAVING
    having_match = re.search(r'\bHAVING\b(.+?)(?:\bORDER BY\b|\bLIMIT\b|$)', normalized_query, re.IGNORECASE)
    if having_match:
        metadata["has_having_clause"] = True

    # Detectar subconsultas
    subquery_count = len(re.findall(r'\(.*SELECT.*\)', normalized_query, re.IGNORECASE))
    if subquery_count > 0:
        metadata["subquery_count"] = subquery_count

    return metadata


def _calculate_query_complexity(sql_query: str, filters: Dict, metadata: Dict) -> str:
    """
    Calcula o nível de complexidade da query
    """
    complexity_score = 0

    # Pontuação baseada nos filtros
    complexity_score += len([k for k in filters.keys() if not k.startswith('_')])

    # Pontuação baseada nos metadados
    if metadata.get("aggregations"):
        complexity_score += len(metadata["aggregations"])

    if metadata.get("group_by_fields"):
        complexity_score += len(metadata["group_by_fields"])

    if metadata.get("subquery_count", 0) > 0:
        complexity_score += metadata["subquery_count"] * 2

    if metadata.get("has_having_clause"):
        complexity_score += 2

    # Classificar complexidade
    if complexity_score == 0:
        return "simple"
    elif complexity_score <= 3:
        return "moderate"
    elif complexity_score <= 6:
        return "complex"
    else:
        return "very_complex"


def analyze_filter_evolution(previous_context: Dict, current_context: Dict) -> Dict:
    """
    Analisa como os filtros evoluíram entre duas consultas

    Returns:
        Dict com análise das mudanças:
        - added: Filtros adicionados
        - removed: Filtros removidos
        - modified: Filtros modificados
        - unchanged: Filtros que permaneceram iguais
    """
    analysis = {
        "added": {},
        "removed": {},
        "modified": {},
        "unchanged": {}
    }

    if not previous_context:
        analysis["added"] = current_context.copy()
        return analysis

    if not current_context:
        analysis["removed"] = previous_context.copy()
        return analysis

    # Comparar filtros
    prev_keys = set(previous_context.keys())
    curr_keys = set(current_context.keys())

    # Filtros adicionados
    for key in curr_keys - prev_keys:
        analysis["added"][key] = current_context[key]

    # Filtros removidos
    for key in prev_keys - curr_keys:
        analysis["removed"][key] = previous_context[key]

    # Filtros comuns - verificar modificações
    for key in prev_keys & curr_keys:
        if previous_context[key] != current_context[key]:
            analysis["modified"][key] = {
                "from": previous_context[key],
                "to": current_context[key]
            }
        else:
            analysis["unchanged"][key] = current_context[key]

    return analysis