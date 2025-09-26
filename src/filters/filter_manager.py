"""
Gerenciamento de filtros interativos na interface Streamlit
Versão simplificada usando novo sistema JSON
"""

import streamlit as st
import sys
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple


def apply_disabled_filters_to_context(context_dict, disabled_filters=None):
    """
    Remove filtros desabilitados do contexto antes de enviar para o agente
    Versão simplificada usando novo sistema JSON
    """
    if not context_dict or not disabled_filters:
        return context_dict

    # Implementação simplificada - remove filtros desabilitados
    filtered_context = {}
    disabled_filter_ids = set(disabled_filters) if isinstance(disabled_filters, (list, set)) else set()

    for campo, valor in context_dict.items():
        filter_id = f"{campo}:{valor}"

        # Tratamento especial para ranges de data
        if campo in ['Data_>=', 'Data_<'] and 'Data_>=' in context_dict and 'Data_<' in context_dict:
            start_date = context_dict.get('Data_>=')
            end_date = context_dict.get('Data_<')
            if start_date and end_date:
                range_id = f"Data_range:{start_date}_{end_date}"
                if range_id not in disabled_filter_ids:
                    filtered_context[campo] = valor
        elif filter_id not in disabled_filter_ids:
            filtered_context[campo] = valor

    return filtered_context


def filter_user_friendly_context(context_dict):
    """
    Filtra contexto para mostrar apenas variáveis relevantes para o usuário,
    removendo variáveis técnicas internas.
    """
    if not context_dict:
        return {}

    # Variáveis técnicas que devem ser ocultadas (prefixos e nomes específicos)
    technical_prefixes = [
        '_temporal_', '_comparative_', '_requires_', '_preserve_', '_enable_',
        '_disable_', '_auto_', '_override_', '_expand_', '_allow_'
    ]

    technical_keywords = [
        'merge_timestamp', 'merge_operations', 'conflicts_resolved', 'context_age',
        'calculation_required', 'comparison_type', 'temporal_metadata',
        'growth_type', 'variation_type', 'evolution_granularity'
    ]

    # Variáveis que devem sempre ser mostradas (lista de permissão)
    user_relevant_fields = [
        # Período
        'Data', 'Data_>=', 'Data_<', 'periodo', 'mes', 'ano',
        # Região
        'UF_Cliente', 'Municipio_Cliente', 'cidade', 'estado', 'municipio', 'uf',
        # Cliente
        'Cod_Cliente', 'Cod_Segmento_Cliente',
        # Produto
        'Cod_Familia_Produto', 'Cod_Grupo_Produto', 'Cod_Linha_Produto', 'Des_Linha_Produto',
        # Representante
        'Cod_Vendedor', 'Cod_Regiao_Vendedor',
        # Campos legados/compatibilidade
        'Cliente', 'Produto', 'Regiao', 'Vendedor', 'sem_filtros'
    ]

    filtered_context = {}

    for key, value in context_dict.items():
        # Sempre incluir campos relevantes para o usuário
        if key in user_relevant_fields:
            filtered_context[key] = value
            continue

        # Verificar se é uma variável técnica
        is_technical = False

        # Verificar prefixos técnicos
        if any(key.startswith(prefix) for prefix in technical_prefixes):
            is_technical = True
        # Verificar palavras-chave técnicas
        elif any(keyword in key.lower() for keyword in technical_keywords):
            is_technical = True

        # Incluir apenas se não for técnica
        if not is_technical:
            filtered_context[key] = value

    return filtered_context


def create_interactive_filter_manager(context_dict):
    """
    Cria interface interativa para gerenciar filtros na sidebar
    """
    if not context_dict or context_dict.get('sem_filtros') == 'consulta_geral':
        st.markdown("🔍 **Consulta Geral**\n\n*Nenhum filtro ativo*")
        return

    # Inicializar estado dos filtros desabilitados se não existir
    if 'disabled_filters' not in st.session_state:
        st.session_state.disabled_filters = set()

    # LIMPEZA: Remover filtros desabilitados que não estão mais no contexto atual
    # Isso garante que filtros antigos não apareçam mais na interface
    current_filter_ids = set()

    # Coletar todos os IDs de filtros atualmente presentes no contexto
    for key, value in context_dict.items():
        if key in ['Data_>=', 'Data_<'] and 'Data_>=' in context_dict and 'Data_<' in context_dict:
            # Para ranges de data, usar ID especial
            start_date = context_dict.get('Data_>=')
            end_date = context_dict.get('Data_<')
            if start_date and end_date:
                current_filter_ids.add(f"Data_range:{start_date}_{end_date}")
        elif key not in ['Data_>=', 'Data_<']:  # Evitar duplicação para ranges
            filter_id = f"{key}:{value}"
            current_filter_ids.add(filter_id)

    # Remover filtros desabilitados que não estão mais no contexto atual
    st.session_state.disabled_filters = {
        f_id for f_id in st.session_state.disabled_filters
        if f_id in current_filter_ids
    }

    st.markdown("✅ **Filtros Ativos**")
    st.markdown("*Desmarque para ignorar na próxima consulta*")
    st.markdown("---")

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

    # Criar controles para cada categoria
    _create_temporal_filter_controls(temporal_filters)
    _create_region_filter_controls(region_filters)
    _create_client_filter_controls(client_filters)
    _create_product_filter_controls(product_filters)
    _create_representative_filter_controls(representative_filters)


def _create_temporal_filter_controls(temporal_filters):
    """Cria controles para filtros temporais"""
    if not temporal_filters:
        return

    st.markdown("📅 **Período**")

    # Detectar se é um range de datas
    start_date = None
    end_date = None

    for key, value in temporal_filters:
        if 'Data_>=' in key or key == 'inicio':
            start_date = value
        elif 'Data_<' in key or key == 'fim':
            end_date = value

    if start_date and end_date:
        # Tratar range como um filtro único
        filter_id = f"Data_range:{start_date}_{end_date}"
        is_enabled = filter_id not in st.session_state.disabled_filters

        # Usar formatação inteligente para o período
        formatted_period = _format_intelligent_date_range(start_date, end_date)
        display_text = f"📅 Período: {formatted_period}"

        enabled = st.checkbox(
            label=display_text,
            value=is_enabled,
            key=f"checkbox_{filter_id}"
        )

        if enabled and filter_id in st.session_state.disabled_filters:
            st.session_state.disabled_filters.remove(filter_id)
        elif not enabled and filter_id not in st.session_state.disabled_filters:
            st.session_state.disabled_filters.add(filter_id)
    else:
        # Filtros temporais individuais
        for key, value in temporal_filters:
            _create_single_filter_control(key, value, _get_temporal_display_text)


def _create_region_filter_controls(region_filters):
    """Cria controles para filtros de região"""
    if not region_filters:
        return

    for key, value in region_filters:
        _create_single_filter_control(key, value, _get_region_display_text)


def _create_client_filter_controls(client_filters):
    """Cria controles para filtros de cliente"""
    if not client_filters:
        return

    for key, value in client_filters:
        _create_single_filter_control(key, value, _get_client_display_text)


def _create_product_filter_controls(product_filters):
    """Cria controles para filtros de produto"""
    if not product_filters:
        return

    for key, value in product_filters:
        _create_single_filter_control(key, value, _get_product_display_text)


def _create_representative_filter_controls(representative_filters):
    """Cria controles para filtros de representante"""
    if not representative_filters:
        return

    for key, value in representative_filters:
        _create_single_filter_control(key, value, _get_representative_display_text)


def _create_single_filter_control(key, value, display_text_func):
    """Cria um controle de checkbox individual para um filtro"""
    filter_id = f"{key}:{value}"
    is_enabled = filter_id not in st.session_state.disabled_filters

    display_text = display_text_func(key, value)

    enabled = st.checkbox(
        label=display_text,
        value=is_enabled,
        key=f"checkbox_{filter_id}"
    )

    if enabled and filter_id in st.session_state.disabled_filters:
        st.session_state.disabled_filters.remove(filter_id)
    elif not enabled and filter_id not in st.session_state.disabled_filters:
        st.session_state.disabled_filters.add(filter_id)


def _format_intelligent_date_range(start_date_str, end_date_str):
    """
    Formata inteligentemente um range de datas baseado no padrão detectado.

    Args:
        start_date_str: Data início no formato 'YYYY-MM-DD'
        end_date_str: Data fim no formato 'YYYY-MM-DD'

    Returns:
        String formatada para exibição amigável
    """
    try:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d')

        # Calcular diferença para detectar padrão
        diff_days = (end_date - start_date).days

        # Mês específico: diferença de 28-31 dias e início no dia 1
        if (28 <= diff_days <= 31 and
            start_date.day == 1 and
            end_date.day == 1 and
            end_date.month != start_date.month):

            month_names = {
                1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril',
                5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
                9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'
            }
            month_name = month_names.get(start_date.month, str(start_date.month).zfill(2))
            return f"{start_date.month:02d}/{start_date.year} ({month_name})"

        # Ano completo: SOMENTE quando realmente começa em jan e termina em dez do mesmo ano
        elif (355 <= diff_days <= 366 and
              start_date.month == 1 and start_date.day == 1 and
              end_date.month == 1 and end_date.day == 1 and
              end_date.year == start_date.year + 1):
            return f"01/{start_date.year} a 12/{start_date.year}"

        # Range customizado: mostrar período completo
        else:
            start_formatted = f"{start_date.month:02d}/{start_date.year}"
            # Para end_date, mostrar o mês anterior já que é exclusivo
            end_display = end_date - timedelta(days=1)
            end_formatted = f"{end_display.month:02d}/{end_display.year}"

            if start_formatted == end_formatted:
                return start_formatted
            else:
                return f"{start_formatted} a {end_formatted}"

    except (ValueError, AttributeError):
        # Fallback para formato original se parsing falhar
        return f"{start_date_str} até {end_date_str}"


def _get_temporal_display_text(key, value):
    """Gera texto de exibição para filtros temporais"""
    if key == 'Data':
        # Tentar detectar diferentes formatos de data para melhor formatação
        if isinstance(value, str):
            # Formato completo de data YYYY-MM-DD
            if len(value) == 10 and value.count('-') == 2:
                try:
                    date_obj = datetime.strptime(value, '%Y-%m-%d')
                    return f"📆 Data: {date_obj.month:02d}/{date_obj.year}"
                except ValueError:
                    pass

            # Formato apenas ano (YYYY)
            elif len(value) == 4 and value.isdigit():
                year = int(value)
                return f"📆 Data: 01/{year} a 12/{year}"

            # Formato ano-mês (YYYY-MM)
            elif len(value) == 7 and value.count('-') == 1:
                try:
                    year, month = value.split('-')
                    year, month = int(year), int(month)
                    month_names = {
                        1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril',
                        5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
                        9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'
                    }
                    month_name = month_names.get(month, str(month).zfill(2))
                    return f"📆 Data: {month:02d}/{year} ({month_name})"
                except (ValueError, IndexError):
                    pass

        # Fallback para formato original
        return f"📆 Data: {value}"
    elif 'mes' in key.lower():
        return f"📅 Mês: {value}"
    elif 'ano' in key.lower():
        return f"🗓️ Ano: {value}"
    else:
        display_name = key.replace("Data_", "").replace(">=", "A partir de").replace("<", "Antes de")
        return f"📅 {display_name}: {value}"


def _get_region_display_text(key, value):
    """Gera texto de exibição para filtros de região"""
    if key in ['Municipio_Cliente', 'cidade', 'municipio']:
        return f"🏙️ Cidade: {value}"
    elif key in ['UF_Cliente', 'estado', 'uf']:
        return f"🗺️ Estado: {value}"
    else:
        return f"📍 {key}: {value}"


def _get_client_display_text(key, value):
    """Gera texto de exibição para filtros de cliente"""
    if key == 'Cod_Cliente':
        return f"🏢 Cliente: {value}"
    elif key == 'Cod_Segmento_Cliente':
        return f"📊 Segmento: {value}"
    else:
        return f"👥 {key}: {value}"


def _get_product_display_text(key, value):
    """Gera texto de exibição para filtros de produto"""
    if key == 'Cod_Familia_Produto':
        return f"🏭 Família: {value}"
    elif key == 'Cod_Grupo_Produto':
        return f"📋 Grupo: {value}"
    elif key == 'Cod_Linha_Produto':
        return f"📦 Cód. Linha: {value}"
    elif key in ['Des_Linha_Produto', 'linha']:
        return f"📦 Linha: {value}"
    elif key in ['Produto', 'produto']:
        return f"🏷️ Produto: {value}"
    else:
        return f"🛍️ {key}: {value}"


def _get_representative_display_text(key, value):
    """Gera texto de exibição para filtros de representante"""
    if key == 'Cod_Vendedor':
        return f"🤝 Vendedor: {value}"
    elif key == 'Cod_Regiao_Vendedor':
        return f"🗺️ Região Vendedor: {value}"
    else:
        return f"👨‍💼 {key}: {value}"


# ========== FUNCIONALIDADES SIMPLIFICADAS ==========
# Nota: Funções antigas removidas - agora usando sistema JSON Filter Manager


def create_enhanced_filter_manager(context_dict: Dict, show_suggestions: bool = True) -> None:
    """
    Versão melhorada do gerenciador de filtros com funcionalidades automáticas

    Args:
        context_dict: Contexto atual dos filtros
        show_suggestions: Se deve mostrar sugestões de filtros
    """
    if not context_dict or context_dict.get('sem_filtros') == 'consulta_geral':
        _render_empty_filter_state()
        return

    # Inicializar estado dos filtros desabilitados se não existir
    if 'disabled_filters' not in st.session_state:
        st.session_state.disabled_filters = set()

    # Limpeza automática de filtros obsoletos
    _cleanup_obsolete_filters(context_dict)

    # Header principal
    st.markdown("✅ **Filtros Ativos**")

    # Botão de limpeza
    if st.button("🗑️ Limpar Todos os Filtros", key="clear_all_filters"):
        st.session_state.disabled_filters = set()
        st.session_state.last_context = {}
        st.rerun()

    st.markdown("*Desmarque para ignorar na próxima consulta*")

    # Analisar compatibilidade dos filtros
    if show_suggestions:
        _show_filter_analysis(context_dict)

    st.markdown("---")

    # Criar controles existentes
    _create_enhanced_filter_controls(context_dict)


def _render_empty_filter_state():
    """Renderiza estado quando não há filtros"""
    st.markdown("🔍 **Nenhum filtro ativo**")
    st.markdown("*Faça uma consulta para ver os filtros detectados automaticamente*")

    # Dica para o usuário
    with st.expander("💡 Dicas de Uso", expanded=False):
        st.markdown("""
        **Como usar filtros automáticos:**
        - Digite consultas naturais como "vendas de janeiro de 2015"
        - Mencione locais como "apenas São Paulo" ou "clientes do RJ"
        - Use frases como "últimos 3 meses" para períodos relativos
        - Diga "sem filtros" para limpar todos os filtros
        """)


def _cleanup_obsolete_filters(context_dict: Dict):
    """Remove filtros desabilitados que não estão mais no contexto atual"""
    current_filter_ids = set()

    # Coletar todos os IDs de filtros atualmente presentes no contexto
    for key, value in context_dict.items():
        if key in ['Data_>=', 'Data_<'] and 'Data_>=' in context_dict and 'Data_<' in context_dict:
            # Para ranges de data, usar ID especial
            start_date = context_dict.get('Data_>=')
            end_date = context_dict.get('Data_<')
            if start_date and end_date:
                current_filter_ids.add(f"Data_range:{start_date}_{end_date}")
        elif key not in ['Data_>=', 'Data_<']:  # Evitar duplicação para ranges
            filter_id = f"{key}:{value}"
            current_filter_ids.add(filter_id)

    # Remover filtros desabilitados que não estão mais no contexto atual
    st.session_state.disabled_filters = {
        f_id for f_id in st.session_state.disabled_filters
        if f_id in current_filter_ids
    }


def _show_filter_analysis(context_dict: Dict):
    """Mostra análise de compatibilidade e sugestões - simplificado"""
    # Análise simplificada sem dependências externas
    if len(context_dict) > 5:
        with st.expander("💡 Dica", expanded=False):
            st.info("ℹ️ Muitos filtros ativos podem tornar os resultados muito específicos.")


def _create_enhanced_filter_controls(context_dict: Dict):
    """Cria controles de filtro com funcionalidades melhoradas"""
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

    # Criar controles para cada categoria com contadores
    _create_temporal_filter_controls_enhanced(temporal_filters)
    _create_region_filter_controls_enhanced(region_filters)
    _create_client_filter_controls_enhanced(client_filters)
    _create_product_filter_controls_enhanced(product_filters)
    _create_representative_filter_controls_enhanced(representative_filters)


def _create_temporal_filter_controls_enhanced(temporal_filters):
    """Cria controles melhorados para filtros temporais"""
    if not temporal_filters:
        return

    st.markdown(f"📅 **Período**")

    _create_temporal_filter_controls(temporal_filters)


def _create_region_filter_controls_enhanced(region_filters):
    """Cria controles melhorados para filtros de região"""
    if not region_filters:
        return

    st.markdown(f"📍 **Região**")
    _create_region_filter_controls(region_filters)


def _create_client_filter_controls_enhanced(client_filters):
    """Cria controles melhorados para filtros de cliente"""
    if not client_filters:
        return

    st.markdown(f"👥 **Cliente**")
    _create_client_filter_controls(client_filters)


def _create_product_filter_controls_enhanced(product_filters):
    """Cria controles melhorados para filtros de produto"""
    if not product_filters:
        return

    st.markdown(f"🛍️ **Produto**")
    _create_product_filter_controls(product_filters)


def _create_representative_filter_controls_enhanced(representative_filters):
    """Cria controles melhorados para filtros de representante"""
    if not representative_filters:
        return

    st.markdown(f"👨‍💼 **Representante**")
    _create_representative_filter_controls(representative_filters)


def _generate_change_summary(old_context: Dict, new_context: Dict, detected_filters: Dict) -> List[str]:
    """Gera resumo das mudanças realizadas nos filtros"""
    changes = []

    # Comando de limpeza
    if detected_filters.get('clear_all_filters'):
        changes.append("* Todos os filtros foram removidos")
        return changes

    # Comparar contextos
    old_keys = set(old_context.keys())
    new_keys = set(new_context.keys())

    # Filtros adicionados
    added = new_keys - old_keys
    for key in added:
        value = new_context[key]
        changes.append(f"+ Adicionado: {key} = {value}")

    # Filtros removidos
    removed = old_keys - new_keys
    for key in removed:
        value = old_context[key]
        changes.append(f"- Removido: {key} = {value}")

    # Filtros modificados
    for key in old_keys & new_keys:
        if old_context[key] != new_context[key]:
            changes.append(f"* Modificado: {key} de '{old_context[key]}' para '{new_context[key]}'")

    return changes


def _generate_evolution_summary(evolution: Dict) -> List[str]:
    """Gera resumo da evolução dos filtros"""
    changes = []

    for key, value in evolution.get('added', {}).items():
        changes.append(f"+ Detectado: {key} = {value}")

    for key, value in evolution.get('removed', {}).items():
        changes.append(f"- Removido: {key} = {value}")

    for key, change_info in evolution.get('modified', {}).items():
        changes.append(f"* Atualizado: {key} de '{change_info['from']}' para '{change_info['to']}'")

    return changes


def get_active_filters_summary(context_dict: Dict) -> str:
    """
    Gera resumo textual dos filtros ativos para exibição
    Versão simplificada

    Returns:
        String com resumo dos filtros
    """
    if not context_dict:
        return "Nenhum filtro ativo"

    # Implementação simplificada
    count = len([v for v in context_dict.values() if v is not None and v != [] and v != ""])
    if count == 0:
        return "Nenhum filtro ativo"
    elif count == 1:
        return "1 filtro ativo"
    else:
        return f"{count} filtros ativos"