"""
Detector inteligente de filtros baseado em linguagem natural
"""

import re
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import pandas as pd


class IntelligentFilterDetector:
    """
    Detecta filtros automaticamente a partir de texto em linguagem natural
    """

    def __init__(self, alias_mapping=None, text_normalizer=None):
        self.alias_mapping = alias_mapping or {}
        self.text_normalizer = text_normalizer

        # Padrões temporais
        self.temporal_patterns = {
            # Meses específicos
            r'janeiro|jan': '01',
            r'fevereiro|fev': '02',
            r'março|mar': '03',
            r'abril|abr': '04',
            r'maio|mai': '05',
            r'junho|jun': '06',
            r'julho|jul': '07',
            r'agosto|ago': '08',
            r'setembro|set': '09',
            r'outubro|out': '10',
            r'novembro|nov': '11',
            r'dezembro|dez': '12',

            # Períodos relativos
            r'último\s+mês|mês\s+passado|mês\s+anterior': 'last_month',
            r'últimos?\s+(\d+)\s+meses?': 'last_n_months',
            r'último\s+ano|ano\s+passado': 'last_year',
            r'últimos?\s+(\d+)\s+anos?': 'last_n_years',
            r'este\s+mês|mês\s+atual': 'this_month',
            r'este\s+ano|ano\s+atual': 'this_year',
        }

        # Padrões geográficos
        self.geographic_patterns = {
            r'são\s+paulo|sp|s\.?p\.?': 'SP',
            r'rio\s+de\s+janeiro|rj|r\.?j\.?': 'RJ',
            r'minas\s+gerais|mg|m\.?g\.?': 'MG',
            r'para(í|i)ba|pb|p\.?b\.?': 'PB',
            r'pernambuco|pe|p\.?e\.?': 'PE',
            r'bahia|ba|b\.?a\.?': 'BA',
            r'brasília|df|d\.?f\.?': 'DF',
            # Cidades
            r'cidade\s+de\s+([^,\s]+)': 'cidade',
            r'município\s+de\s+([^,\s]+)': 'cidade',
        }

        # Padrões de exclusão/inclusão
        self.exclusion_patterns = {
            r'excluir|exceto|sem|não\s+incluir|remover': 'exclude',
            r'apenas|somente|só|incluir\s+apenas': 'include_only',
            r'todos?\s+exceto': 'all_except',
        }

        # Padrões de limpeza de filtros
        self.clear_patterns = [
            r'sem\s+filtros?',
            r'remover?\s+(todos?\s+)?filtros?',
            r'limpar\s+filtros?',
            r'sem\s+restrições?',
            r'consulta\s+geral',
            r'todos?\s+os?\s+dados?',
        ]

    def detect_filters_from_text(self, text: str, current_context: Dict = None,
                                max_date: Optional[datetime] = None) -> Dict:
        """
        Detecta filtros a partir de texto em linguagem natural

        Args:
            text: Texto da consulta do usuário
            current_context: Contexto atual dos filtros
            max_date: Data máxima do dataset para cálculos relativos

        Returns:
            Dict com filtros detectados
        """
        detected_filters = {}
        text_lower = text.lower()

        # 1. Verificar comandos de limpeza primeiro
        if self._is_clear_filters_command(text_lower):
            return {'clear_all_filters': True}

        # 2. Detectar filtros temporais
        temporal_filters = self._detect_temporal_filters(text_lower, max_date)
        detected_filters.update(temporal_filters)

        # 3. Detectar filtros geográficos
        geographic_filters = self._detect_geographic_filters(text_lower)
        detected_filters.update(geographic_filters)

        # 4. Detectar filtros de produto/cliente/vendedor via aliases
        alias_filters = self._detect_alias_filters(text_lower)
        detected_filters.update(alias_filters)

        # 5. Detectar padrões de exclusão/inclusão
        exclusion_context = self._detect_exclusion_patterns(text_lower)
        if exclusion_context:
            detected_filters['_filter_mode'] = exclusion_context

        # 6. Aplicar normalização se disponível
        if self.text_normalizer:
            detected_filters = self._normalize_filter_values(detected_filters)

        return detected_filters

    def _is_clear_filters_command(self, text: str) -> bool:
        """Verifica se o texto contém comando de limpeza de filtros"""
        for pattern in self.clear_patterns:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        return False

    def _detect_temporal_filters(self, text: str, max_date: Optional[datetime] = None) -> Dict:
        """Detecta filtros temporais do texto"""
        filters = {}

        # Data máxima padrão se não fornecida
        if max_date is None:
            max_date = datetime.now()

        # Buscar anos específicos (2020, 2021, etc.)
        year_matches = re.findall(r'\b(20\d{2})\b', text)
        if year_matches:
            year = year_matches[0]
            filters['Data_>='] = f'{year}-01-01'
            filters['Data_<'] = f'{int(year)+1}-01-01'

        # Buscar meses específicos com anos
        month_year_match = re.search(r'(\w+)\s+de\s+(20\d{2})', text)
        if month_year_match:
            month_name = month_year_match.group(1).lower()
            year = month_year_match.group(2)

            for pattern, month_num in self.temporal_patterns.items():
                if re.search(pattern, month_name):
                    if month_num.isdigit():
                        filters['Data_>='] = f'{year}-{month_num.zfill(2)}-01'
                        # Próximo mês
                        next_month = int(month_num) + 1
                        if next_month > 12:
                            filters['Data_<'] = f'{int(year)+1}-01-01'
                        else:
                            filters['Data_<'] = f'{year}-{str(next_month).zfill(2)}-01'
                    break

        # Períodos relativos
        for pattern, action in self.temporal_patterns.items():
            if re.search(pattern, text):
                if action == 'last_month':
                    last_month = max_date.replace(day=1) - timedelta(days=1)
                    start_date = last_month.replace(day=1)
                    filters['Data_>='] = start_date.strftime('%Y-%m-%d')
                    filters['Data_<'] = max_date.replace(day=1).strftime('%Y-%m-%d')
                elif action == 'this_month':
                    start_date = max_date.replace(day=1)
                    filters['Data_>='] = start_date.strftime('%Y-%m-%d')
                elif action == 'last_year':
                    last_year = max_date.year - 1
                    filters['Data_>='] = f'{last_year}-01-01'
                    filters['Data_<'] = f'{max_date.year}-01-01'
                elif action == 'this_year':
                    filters['Data_>='] = f'{max_date.year}-01-01'
                elif 'last_n_months' in action:
                    match = re.search(r'últimos?\s+(\d+)\s+meses?', text)
                    if match:
                        n_months = int(match.group(1))
                        start_date = max_date - timedelta(days=30 * n_months)
                        filters['Data_>='] = start_date.strftime('%Y-%m-%d')
                elif 'last_n_years' in action:
                    match = re.search(r'últimos?\s+(\d+)\s+anos?', text)
                    if match:
                        n_years = int(match.group(1))
                        start_year = max_date.year - n_years
                        filters['Data_>='] = f'{start_year}-01-01'
                break

        return filters

    def _detect_geographic_filters(self, text: str) -> Dict:
        """Detecta filtros geográficos do texto"""
        filters = {}

        for pattern, value in self.geographic_patterns.items():
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                if value == 'cidade':
                    # Capturar nome da cidade
                    city_name = match.group(1) if match.groups() else None
                    if city_name:
                        filters['Municipio_Cliente'] = city_name.title()
                else:
                    # Estado
                    filters['UF_Cliente'] = value
                break

        return filters

    def _detect_alias_filters(self, text: str) -> Dict:
        """Detecta filtros baseados nos aliases configurados"""
        filters = {}

        if not self.alias_mapping:
            return filters

        for alias, field_info in self.alias_mapping.items():
            # Buscar o alias no texto
            alias_pattern = r'\b' + re.escape(alias.lower()) + r'\b'
            if re.search(alias_pattern, text):
                if isinstance(field_info, dict):
                    # Alias com valor específico
                    field_name = field_info.get('field')
                    field_value = field_info.get('value')
                    if field_name and field_value:
                        filters[field_name] = field_value
                elif isinstance(field_info, str):
                    # Alias simples (nome do campo)
                    # Tentar extrair valor após o alias
                    value_match = re.search(f'{alias_pattern}[\\s:=]+([^\\s,]+)', text)
                    if value_match:
                        filters[field_info] = value_match.group(1)

        return filters

    def _detect_exclusion_patterns(self, text: str) -> Optional[str]:
        """Detecta padrões de exclusão/inclusão"""
        for pattern, mode in self.exclusion_patterns.items():
            if re.search(pattern, text, re.IGNORECASE):
                return mode
        return None

    def _normalize_filter_values(self, filters: Dict) -> Dict:
        """Aplica normalização aos valores dos filtros"""
        normalized = {}

        for key, value in filters.items():
            if key.startswith('_') or key in ['Data_>=', 'Data_<']:
                # Pular campos especiais e datas
                normalized[key] = value
            else:
                # Aplicar normalização de texto
                if isinstance(value, str) and self.text_normalizer:
                    normalized[key] = self.text_normalizer.normalize_text(value)
                else:
                    normalized[key] = value

        return normalized

    def merge_with_current_context(self, detected_filters: Dict, current_context: Dict) -> Dict:
        """
        Mescla filtros detectados com o contexto atual, resolvendo conflitos

        Args:
            detected_filters: Filtros recém-detectados
            current_context: Contexto atual

        Returns:
            Contexto mesclado
        """
        # Se comando para limpar filtros
        if detected_filters.get('clear_all_filters'):
            return {}

        merged_context = current_context.copy()
        filter_mode = detected_filters.get('_filter_mode', 'add')

        for key, value in detected_filters.items():
            if key.startswith('_'):
                continue  # Pular campos de controle

            if filter_mode == 'include_only':
                # Modo "apenas": substituir contexto
                merged_context = {key: value}
            elif filter_mode == 'exclude':
                # Modo exclusão: remover do contexto se existir
                merged_context.pop(key, None)
            else:
                # Modo padrão: adicionar/substituir
                merged_context[key] = value

        return merged_context

    def extract_intent_changes(self, text: str) -> Dict:
        """
        Extrai mudanças de intenção do texto (adicionar vs substituir vs remover)

        Returns:
            Dict com informações sobre a intenção
        """
        intent = {
            'action': 'add',  # add, replace, remove, clear
            'scope': 'partial',  # partial, all
            'confidence': 0.5
        }

        text_lower = text.lower()

        # Comandos de limpeza total
        if self._is_clear_filters_command(text_lower):
            intent['action'] = 'clear'
            intent['scope'] = 'all'
            intent['confidence'] = 0.9

        # Palavras indicativas de substituição
        elif any(word in text_lower for word in ['agora', 'apenas', 'somente', 'só', 'trocar']):
            intent['action'] = 'replace'
            intent['confidence'] = 0.8

        # Palavras indicativas de adição
        elif any(word in text_lower for word in ['também', 'ainda', 'adicionalmente', 'incluir']):
            intent['action'] = 'add'
            intent['confidence'] = 0.7

        # Palavras indicativas de remoção
        elif any(word in text_lower for word in ['remover', 'excluir', 'tirar', 'sem']):
            intent['action'] = 'remove'
            intent['confidence'] = 0.8

        return intent