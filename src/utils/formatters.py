"""
Funções de formatação centralizadas para contexto, SQL e números
"""

import re


def format_context_for_display(context_dict):
    """
    Formata contexto de forma amigável para exibição no sidebar (versão legada para compatibilidade)
    """
    if not context_dict or context_dict.get('sem_filtros') == 'consulta_geral':
        return "🔍 **Consulta Geral**\n\n*Nenhum filtro ativo*"

    display_parts = ["✅ **Filtros Ativos**", ""]

    # Categorizar filtros baseado na hierarquia completa
    temporal_filters = []
    region_filters = []
    client_filters = []
    product_filters = []
    representative_filters = []

    for key, value in context_dict.items():
        # Período
        if key in ['Data', 'Data_>=', 'Data_<', 'periodo', 'mes', 'ano']:
            temporal_filters.append((key, value))
        # Região
        elif key in ['UF_Cliente', 'Municipio_Cliente', 'cidade', 'estado', 'municipio', 'uf']:
            region_filters.append((key, value))
        # Cliente
        elif key in ['Cod_Cliente', 'Cod_Segmento_Cliente']:
            client_filters.append((key, value))
        # Produto
        elif key in ['Cod_Familia_Produto', 'Cod_Grupo_Produto', 'Cod_Linha_Produto', 'Des_Linha_Produto', 'Produto', 'produto', 'linha']:
            product_filters.append((key, value))
        # Representante
        elif key in ['Cod_Vendedor', 'Cod_Regiao_Vendedor']:
            representative_filters.append((key, value))

    # Formatação por categoria com melhor visual
    if temporal_filters:
        display_parts.append("📅 **Período**")

        # Detectar se é um range de datas
        start_date = None
        end_date = None

        for key, value in temporal_filters:
            if 'Data_>=' in key or key == 'inicio':
                start_date = value
            elif 'Data_<' in key or key == 'fim':
                end_date = value

        if start_date and end_date:
            display_parts.append(f"⏰ **Período**: {start_date} até {end_date}")
        else:
            for key, value in temporal_filters:
                if key == 'Data':
                    display_parts.append(f"📆 **Data**: {value}")
                elif 'mes' in key.lower():
                    display_parts.append(f"📅 **Mês**: {value}")
                elif 'ano' in key.lower():
                    display_parts.append(f"🗓️ **Ano**: {value}")
                else:
                    display_name = key.replace("Data_", "").replace(">=", "A partir de").replace("<", "Antes de")
                    display_parts.append(f"📅 **{display_name}**: {value}")
        display_parts.append("")

    if region_filters:
        display_parts.append("📍 **Região**")
        for key, value in region_filters:
            if key in ['Municipio_Cliente', 'cidade', 'municipio']:
                display_parts.append(f"🏙️ Cidade: **{value}**")
            elif key in ['UF_Cliente', 'estado', 'uf']:
                display_parts.append(f"🗺️ Estado: **{value}**")
            else:
                display_parts.append(f"📍 {key}: **{value}**")
        display_parts.append("")

    if client_filters:
        display_parts.append("👥 **Cliente**")
        for key, value in client_filters:
            if key == 'Cod_Cliente':
                display_parts.append(f"🏢 Cliente: **{value}**")
            elif key == 'Cod_Segmento_Cliente':
                display_parts.append(f"📊 Segmento: **{value}**")
            else:
                display_parts.append(f"👥 {key}: **{value}**")
        display_parts.append("")

    if product_filters:
        display_parts.append("🛍️ **Produto**")
        for key, value in product_filters:
            if key == 'Cod_Familia_Produto':
                display_parts.append(f"🏭 Família: **{value}**")
            elif key == 'Cod_Grupo_Produto':
                display_parts.append(f"📋 Grupo: **{value}**")
            elif key == 'Cod_Linha_Produto':
                display_parts.append(f"📦 Cód. Linha: **{value}**")
            elif key in ['Des_Linha_Produto', 'linha']:
                display_parts.append(f"📦 Linha: **{value}**")
            elif key in ['Produto', 'produto']:
                display_parts.append(f"🏷️ Produto: **{value}**")
            else:
                display_parts.append(f"🛍️ {key}: **{value}**")
        display_parts.append("")

    if representative_filters:
        display_parts.append("👨‍💼 **Representante**")
        for key, value in representative_filters:
            if key == 'Cod_Vendedor':
                display_parts.append(f"🤝 Vendedor: **{value}**")
            elif key == 'Cod_Regiao_Vendedor':
                display_parts.append(f"🗺️ Região Vendedor: **{value}**")
            else:
                display_parts.append(f"👨‍💼 {key}: **{value}**")
        display_parts.append("")

    # Adicionar rodapé informativo se houver filtros
    if any([temporal_filters, region_filters, client_filters, product_filters, representative_filters]):
        display_parts.extend(["---", "💡 *Filtros aplicados à consulta atual*"])

    return "\n".join(display_parts).strip()


def format_sql_query(query):
    """
    Formata uma query SQL para melhor legibilidade
    """
    if not query:
        return query

    # Remove ANSI escape sequences
    query = re.sub(r"\x1b\[[0-9;]*m", "", query)

    # Remove caracteres de controle
    query = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", query)

    # Normaliza espaços em branco
    query = " ".join(query.split())

    # Formata as principais palavras-chave SQL
    keywords = [
        "SELECT", "FROM", "WHERE", "JOIN", "LEFT JOIN", "RIGHT JOIN", "INNER JOIN",
        "GROUP BY", "ORDER BY", "HAVING", "UNION", "INSERT", "UPDATE", "DELETE", "AS"
    ]

    formatted_query = query
    for keyword in keywords:
        # Adiciona quebras de linha antes das principais palavras-chave
        if keyword in ["FROM", "WHERE", "GROUP BY", "ORDER BY", "HAVING"]:
            formatted_query = re.sub(
                f" {keyword} ", f"\n{keyword} ", formatted_query, flags=re.IGNORECASE
            )
        elif keyword == "SELECT":
            formatted_query = re.sub(
                f"^{keyword} ", f"{keyword}\n    ", formatted_query, flags=re.IGNORECASE
            )

    # Ajusta indentação
    lines = formatted_query.split("\n")
    formatted_lines = []
    for line in lines:
        line = line.strip()
        if line.upper().startswith(
            ("SELECT", "FROM", "WHERE", "GROUP BY", "ORDER BY", "HAVING")
        ):
            formatted_lines.append(line)
        else:
            formatted_lines.append("    " + line if line else line)

    return "\n".join(formatted_lines)


def format_compact_number(value):
    """
    Formata números grandes em notação compacta (1M, 2.5M, etc.)
    """
    try:
        if value >= 1_000_000_000:
            return f"{value/1_000_000_000:.1f}B"
        elif value >= 1_000_000:
            return f"{value/1_000_000:.1f}M"
        elif value >= 1_000:
            return f"{value/1_000:.1f}K"
        else:
            return f"{value:.0f}"
    except:
        return str(value)