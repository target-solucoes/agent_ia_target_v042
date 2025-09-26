"""
Sistema Unificado de Filtros - Core Centralizado
Solução single source of truth para gerenciamento de filtros
"""

import re
import sqlparse
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Tuple, Any, Union
from enum import Enum
from dataclasses import dataclass, field
import copy
import json


class FilterAction(Enum):
    """Ações possíveis sobre filtros"""
    ADD = "add"
    REMOVE = "remove"
    MODIFY = "modify"
    CLEAR_ALL = "clear_all"
    DISABLE = "disable"
    ENABLE = "enable"


class FilterCategory(Enum):
    """Categorias de filtros para organização hierárquica"""
    TEMPORAL = "temporal"
    GEOGRAPHIC = "geographic"
    CLIENT = "client"
    PRODUCT = "product"
    REPRESENTATIVE = "representative"
    OTHER = "other"


@dataclass
class FilterDefinition:
    """Definição de um filtro individual"""
    key: str
    value: Any
    category: FilterCategory
    operator: str = "="
    enabled: bool = True
    source: str = "user"  # user, sql, auto_detected
    metadata: Dict = field(default_factory=dict)

    def __post_init__(self):
        if not self.metadata:
            self.metadata = {}

    @property
    def filter_id(self) -> str:
        """ID único do filtro"""
        return f"{self.key}:{self.value}"

    def to_context_pair(self) -> Tuple[str, Any]:
        """Converte para par (chave, valor) do contexto"""
        if self.operator != "=":
            key = f"{self.key}_{self.operator}"
        else:
            key = self.key
        return key, self.value


@dataclass
class FilterState:
    """Estado centralizado de todos os filtros - Single Source of Truth"""
    filters: Dict[str, FilterDefinition] = field(default_factory=dict)
    disabled_filter_ids: Set[str] = field(default_factory=set)
    metadata: Dict = field(default_factory=dict)
    last_sync_timestamp: Optional[datetime] = None

    def __post_init__(self):
        if not self.metadata:
            self.metadata = {}
        if self.last_sync_timestamp is None:
            self.last_sync_timestamp = datetime.now()

    def add_filter(self, filter_def: FilterDefinition) -> bool:
        """Adiciona ou atualiza um filtro"""
        try:
            self.filters[filter_def.filter_id] = filter_def
            self._update_sync_timestamp()
            return True
        except Exception:
            return False

    def remove_filter(self, filter_id: str) -> bool:
        """Remove um filtro"""
        if filter_id in self.filters:
            del self.filters[filter_id]
            self.disabled_filter_ids.discard(filter_id)
            self._update_sync_timestamp()
            return True
        return False

    def disable_filter(self, filter_id: str) -> bool:
        """Desabilita um filtro sem removê-lo"""
        if filter_id in self.filters:
            self.disabled_filter_ids.add(filter_id)
            self._update_sync_timestamp()
            return True
        return False

    def enable_filter(self, filter_id: str) -> bool:
        """Habilita um filtro previamente desabilitado"""
        if filter_id in self.filters:
            self.disabled_filter_ids.discard(filter_id)
            self._update_sync_timestamp()
            return True
        return False

    def clear_all_filters(self) -> bool:
        """Remove todos os filtros"""
        self.filters.clear()
        self.disabled_filter_ids.clear()
        self._update_sync_timestamp()
        return True

    def get_active_filters(self) -> Dict[str, FilterDefinition]:
        """Retorna apenas filtros ativos (habilitados)"""
        return {
            fid: fdef for fid, fdef in self.filters.items()
            if fid not in self.disabled_filter_ids
        }

    def get_disabled_filters(self) -> Dict[str, FilterDefinition]:
        """Retorna apenas filtros desabilitados"""
        return {
            fid: fdef for fid, fdef in self.filters.items()
            if fid in self.disabled_filter_ids
        }

    def to_context_dict(self, only_active: bool = True) -> Dict[str, Any]:
        """Converte estado para dicionário de contexto"""
        filters_to_convert = self.get_active_filters() if only_active else self.filters
        context = {}

        for filter_def in filters_to_convert.values():
            key, value = filter_def.to_context_pair()
            context[key] = value

        return context

    def get_filters_by_category(self, category: FilterCategory) -> Dict[str, FilterDefinition]:
        """Retorna filtros de uma categoria específica"""
        return {
            fid: fdef for fid, fdef in self.filters.items()
            if fdef.category == category
        }

    def _update_sync_timestamp(self):
        """Atualiza timestamp de sincronização"""
        self.last_sync_timestamp = datetime.now()

    def is_stale(self, max_age_seconds: int = 60) -> bool:
        """Verifica se o estado está desatualizado"""
        if not self.last_sync_timestamp:
            return True
        age = datetime.now() - self.last_sync_timestamp
        return age.total_seconds() > max_age_seconds


class SQLFilterExtractor:
    """Extrator robusto de filtros SQL usando sqlparse"""

    def __init__(self):
        self.category_mapping = {
            # Temporal
            'data': FilterCategory.TEMPORAL,
            'data_inicio': FilterCategory.TEMPORAL,
            'data_fim': FilterCategory.TEMPORAL,
            'periodo': FilterCategory.TEMPORAL,
            'mes': FilterCategory.TEMPORAL,
            'ano': FilterCategory.TEMPORAL,

            # Geographic
            'uf_cliente': FilterCategory.GEOGRAPHIC,
            'municipio_cliente': FilterCategory.GEOGRAPHIC,
            'cidade': FilterCategory.GEOGRAPHIC,
            'estado': FilterCategory.GEOGRAPHIC,

            # Client
            'cod_cliente': FilterCategory.CLIENT,
            'cod_segmento_cliente': FilterCategory.CLIENT,
            'cliente': FilterCategory.CLIENT,

            # Product
            'cod_familia_produto': FilterCategory.PRODUCT,
            'cod_grupo_produto': FilterCategory.PRODUCT,
            'cod_linha_produto': FilterCategory.PRODUCT,
            'des_linha_produto': FilterCategory.PRODUCT,
            'produto': FilterCategory.PRODUCT,

            # Representative
            'cod_vendedor': FilterCategory.REPRESENTATIVE,
            'cod_regiao_vendedor': FilterCategory.REPRESENTATIVE,
            'vendedor': FilterCategory.REPRESENTATIVE,
        }

    def extract_filters_from_sql(self, sql_query: str) -> List[FilterDefinition]:
        """
        Extrai filtros de uma query SQL usando sqlparse
        Muito mais robusto que o parser regex anterior
        """
        try:
            # Parse da query SQL
            parsed = sqlparse.parse(sql_query)[0]
            filters = []

            # Extrair filtros de diferentes partes da query
            filters.extend(self._extract_from_where_clause(parsed))
            filters.extend(self._extract_from_having_clause(parsed))
            filters.extend(self._extract_from_subqueries(parsed))

            return filters

        except Exception as e:
            # Fallback para parser regex se sqlparse falhar
            return self._fallback_regex_extraction(sql_query)

    def _extract_from_where_clause(self, parsed_query) -> List[FilterDefinition]:
        """Extrai filtros da cláusula WHERE"""
        filters = []

        def extract_from_token(token):
            if hasattr(token, 'tokens'):
                for subtoken in token.tokens:
                    extract_from_token(subtoken)
            elif token.ttype is sqlparse.tokens.Keyword and token.value.upper() == 'WHERE':
                # Encontrou WHERE, próximos tokens são condições
                parent = token.parent
                if parent:
                    where_index = parent.tokens.index(token)
                    for i in range(where_index + 1, len(parent.tokens)):
                        next_token = parent.tokens[i]
                        if (next_token.ttype is sqlparse.tokens.Keyword and
                            next_token.value.upper() in ['GROUP', 'ORDER', 'HAVING', 'LIMIT']):
                            break
                        filters.extend(self._parse_condition_token(next_token))

        extract_from_token(parsed_query)
        return filters

    def _extract_from_having_clause(self, parsed_query) -> List[FilterDefinition]:
        """Extrai filtros da cláusula HAVING"""
        # Implementação similar ao WHERE
        return []

    def _extract_from_subqueries(self, parsed_query) -> List[FilterDefinition]:
        """Extrai filtros de subconsultas"""
        filters = []

        def find_subqueries(token):
            if hasattr(token, 'tokens'):
                for subtoken in token.tokens:
                    if isinstance(subtoken, sqlparse.sql.Parenthesis):
                        # Possível subconsulta
                        subquery_content = str(subtoken)[1:-1]  # Remove parênteses
                        if 'SELECT' in subquery_content.upper():
                            filters.extend(self.extract_filters_from_sql(subquery_content))
                    find_subqueries(subtoken)

        find_subqueries(parsed_query)
        return filters

    def _parse_condition_token(self, token) -> List[FilterDefinition]:
        """Parse de um token de condição específico"""
        filters = []

        # Implementar parsing de condições específicas
        # Por agora, usar fallback regex para condições simples
        condition_str = str(token).strip()

        # Padrões comuns de filtros
        patterns = [
            (r"(\w+)\s*=\s*'([^']*)'", "="),
            (r"(\w+)\s*>\s*'([^']*)'", ">"),
            (r"(\w+)\s*<\s*'([^']*)'", "<"),
            (r"(\w+)\s*>=\s*'([^']*)'", ">="),
            (r"(\w+)\s*<=\s*'([^']*)'", "<="),
            (r"(\w+)\s+LIKE\s+'([^']*)'", "LIKE"),
        ]

        for pattern, operator in patterns:
            matches = re.finditer(pattern, condition_str, re.IGNORECASE)
            for match in matches:
                column = match.group(1).lower()
                value = match.group(2)

                category = self.category_mapping.get(column, FilterCategory.OTHER)

                filter_def = FilterDefinition(
                    key=column,
                    value=value,
                    category=category,
                    operator=operator,
                    source="sql"
                )
                filters.append(filter_def)

        return filters

    def _fallback_regex_extraction(self, sql_query: str) -> List[FilterDefinition]:
        """Fallback para extração via regex quando sqlparse falha"""
        filters = []

        # Usar lógica do parser antigo como fallback
        normalized_query = re.sub(r'\s+', ' ', sql_query.strip())

        # Buscar por WHERE clause
        where_match = re.search(r'\bWHERE\b(.*?)(?:\bGROUP BY\b|\bORDER BY\b|\bHAVING\b|\bLIMIT\b|$)',
                               normalized_query, re.IGNORECASE)

        if not where_match:
            return filters

        where_clause = where_match.group(1).strip()

        # Padrões de extração
        patterns = [
            (r"(?:LOWER\()?(\w+)\)?\s*=\s*'([^']*)'", "="),
            (r"(?:LOWER\()?(\w+)\)?\s*>\s*'([^']*)'", ">"),
            (r"(?:LOWER\()?(\w+)\)?\s*<\s*'([^']*)'", "<"),
            (r"(?:LOWER\()?(\w+)\)?\s*>=\s*'([^']*)'", ">="),
            (r"(?:LOWER\()?(\w+)\)?\s*<=\s*'([^']*)'", "<="),
        ]

        for pattern, operator in patterns:
            matches = re.finditer(pattern, where_clause, re.IGNORECASE)
            for match in matches:
                column = match.group(1).lower()
                value = match.group(2)

                category = self.category_mapping.get(column, FilterCategory.OTHER)

                filter_def = FilterDefinition(
                    key=column,
                    value=value,
                    category=category,
                    operator=operator,
                    source="sql"
                )
                filters.append(filter_def)

        return filters


class ContextSynchronizer:
    """Sincronizador bidirecional entre FilterState e contexto do agente"""

    def __init__(self):
        self.last_agent_context_hash: Optional[str] = None
        self.last_filter_state_hash: Optional[str] = None

    def sync_from_agent_context(self, agent_context: Dict, filter_state: FilterState) -> bool:
        """
        Sincroniza filtros do contexto do agente para o FilterState

        Returns:
            bool: True se houve mudanças, False caso contrário
        """
        try:
            # Calcular hash do contexto atual do agente
            current_agent_hash = self._hash_dict(agent_context)

            # Verificar se houve mudanças
            if current_agent_hash == self.last_agent_context_hash:
                return False

            # Converter contexto do agente para filtros
            new_filters = self._context_to_filters(agent_context)

            # Aplicar filtros ao estado
            changes_made = False
            current_filter_ids = set(filter_state.filters.keys())
            new_filter_ids = set(f.filter_id for f in new_filters)

            # Remover filtros que não estão mais no contexto
            for filter_id in current_filter_ids - new_filter_ids:
                filter_state.remove_filter(filter_id)
                changes_made = True

            # Adicionar/atualizar novos filtros
            for filter_def in new_filters:
                if (filter_def.filter_id not in filter_state.filters or
                    filter_state.filters[filter_def.filter_id].value != filter_def.value):
                    filter_state.add_filter(filter_def)
                    changes_made = True

            # Atualizar hash
            self.last_agent_context_hash = current_agent_hash
            return changes_made

        except Exception:
            return False

    def sync_to_agent_context(self, filter_state: FilterState) -> Dict[str, Any]:
        """
        Sincroniza FilterState para contexto do agente

        Returns:
            Dict: Contexto atualizado para o agente
        """
        try:
            # Converter estado atual para contexto
            context = filter_state.to_context_dict(only_active=True)

            # Atualizar hash
            self.last_filter_state_hash = self._hash_dict(context)

            return context

        except Exception:
            return {}

    def is_sync_needed(self, agent_context: Dict, filter_state: FilterState) -> bool:
        """Verifica se sincronização é necessária"""
        agent_hash = self._hash_dict(agent_context)
        filter_hash = self._hash_dict(filter_state.to_context_dict())

        return (agent_hash != self.last_agent_context_hash or
                filter_hash != self.last_filter_state_hash)

    def _context_to_filters(self, context: Dict) -> List[FilterDefinition]:
        """Converte dicionário de contexto para lista de FilterDefinition"""
        filters = []

        for key, value in context.items():
            # Detectar operador do nome da chave
            operator = "="
            clean_key = key

            for op in [">=", "<=", ">", "<", "!=", "LIKE"]:
                if key.endswith(f"_{op}"):
                    operator = op
                    clean_key = key[:-len(f"_{op}")]
                    break

            # Determinar categoria
            category = FilterCategory.OTHER
            clean_key_lower = clean_key.lower()

            if clean_key_lower in ['data', 'data_inicio', 'data_fim', 'periodo', 'mes', 'ano']:
                category = FilterCategory.TEMPORAL
            elif clean_key_lower in ['uf_cliente', 'municipio_cliente', 'cidade', 'estado']:
                category = FilterCategory.GEOGRAPHIC
            elif clean_key_lower in ['cod_cliente', 'cod_segmento_cliente', 'cliente']:
                category = FilterCategory.CLIENT
            elif 'produto' in clean_key_lower or 'familia' in clean_key_lower or 'grupo' in clean_key_lower:
                category = FilterCategory.PRODUCT
            elif 'vendedor' in clean_key_lower or 'representante' in clean_key_lower:
                category = FilterCategory.REPRESENTATIVE

            filter_def = FilterDefinition(
                key=clean_key,
                value=value,
                category=category,
                operator=operator,
                source="agent_context"
            )
            filters.append(filter_def)

        return filters

    def _hash_dict(self, d: Dict) -> str:
        """Cria hash de um dicionário para comparação"""
        try:
            # Criar representação ordenada e estável
            sorted_items = sorted(d.items())
            dict_str = json.dumps(sorted_items, sort_keys=True, default=str)
            return str(hash(dict_str))
        except Exception:
            return str(hash(str(d)))


class UnifiedFilterManager:
    """Gerenciador central unificado de filtros"""

    def __init__(self, text_normalizer=None, alias_mapping=None):
        self.filter_state = FilterState()
        self.sql_extractor = SQLFilterExtractor()
        self.context_synchronizer = ContextSynchronizer()
        self.text_normalizer = text_normalizer
        self.alias_mapping = alias_mapping or {}

        # Import dynamic para evitar dependências circulares
        self._intelligent_detector = None
        self._conflict_resolver = None

    @property
    def intelligent_detector(self):
        """Lazy loading do detector inteligente"""
        if self._intelligent_detector is None:
            from .intelligent_filter_detector import IntelligentFilterDetector
            self._intelligent_detector = IntelligentFilterDetector(
                self.alias_mapping, self.text_normalizer
            )
        return self._intelligent_detector

    @property
    def conflict_resolver(self):
        """Lazy loading do resolvedor de conflitos"""
        if self._conflict_resolver is None:
            from .filter_conflict_resolver import FilterConflictResolver
            self._conflict_resolver = FilterConflictResolver()
        return self._conflict_resolver

    def process_user_query(self, user_query: str, max_date: Optional[datetime] = None) -> Tuple[bool, List[str]]:
        """
        Processa query do usuário para detectar filtros automaticamente

        Returns:
            Tuple[bool, List[str]]: (houve_mudancas, lista_de_mudancas)
        """
        try:
            # Detectar filtros da query
            detected_filters_dict = self.intelligent_detector.detect_filters_from_text(
                user_query, self.filter_state.to_context_dict(), max_date
            )

            if not detected_filters_dict:
                return False, []

            # Comando de limpeza
            if detected_filters_dict.get('clear_all_filters'):
                self.filter_state.clear_all_filters()
                return True, ["Todos os filtros foram removidos"]

            # Converter para FilterDefinitions
            new_filters = self._dict_to_filter_definitions(detected_filters_dict, "user_query")

            # Resolver conflitos
            current_context = self.filter_state.to_context_dict()
            resolved_context, conflicts = self.conflict_resolver.resolve_conflicts(
                detected_filters_dict, current_context, 'smart'
            )

            # Aplicar filtros resolvidos
            changes = []
            for filter_def in new_filters:
                # Verificar se é filtro novo ou modificado
                existing_filter = self.filter_state.filters.get(filter_def.filter_id)
                if not existing_filter:
                    self.filter_state.add_filter(filter_def)
                    changes.append(f"+ Adicionado: {filter_def.key} = {filter_def.value}")
                elif existing_filter.value != filter_def.value:
                    self.filter_state.add_filter(filter_def)  # Substitui o existente
                    changes.append(f"* Modificado: {filter_def.key} de '{existing_filter.value}' para '{filter_def.value}'")

            return len(changes) > 0, changes

        except Exception:
            return False, []

    def process_sql_response(self, sql_queries: List[str]) -> Tuple[bool, List[str]]:
        """
        Processa respostas SQL para extrair e atualizar filtros automaticamente

        Returns:
            Tuple[bool, List[str]]: (houve_mudancas, lista_de_mudancas)
        """
        try:
            all_extracted_filters = []

            # Extrair filtros de todas as queries
            for query in sql_queries:
                filters = self.sql_extractor.extract_filters_from_sql(query)
                all_extracted_filters.extend(filters)

            if not all_extracted_filters:
                return False, []

            # Aplicar filtros extraídos
            changes = []
            for filter_def in all_extracted_filters:
                existing_filter = self.filter_state.filters.get(filter_def.filter_id)
                if not existing_filter:
                    self.filter_state.add_filter(filter_def)
                    changes.append(f"+ Detectado SQL: {filter_def.key} = {filter_def.value}")
                elif existing_filter.value != filter_def.value:
                    self.filter_state.add_filter(filter_def)
                    changes.append(f"* Atualizado SQL: {filter_def.key} para '{filter_def.value}'")

            return len(changes) > 0, changes

        except Exception:
            return False, []

    def sync_with_agent_context(self, agent_context: Dict) -> bool:
        """
        Sincroniza com contexto do agente

        Returns:
            bool: True se houve mudanças
        """
        return self.context_synchronizer.sync_from_agent_context(agent_context, self.filter_state)

    def get_agent_context(self) -> Dict[str, Any]:
        """Retorna contexto para sincronizar com o agente"""
        return self.context_synchronizer.sync_to_agent_context(self.filter_state)

    def apply_disabled_filters_to_context(self, context_dict: Dict, disabled_filter_ids: Set[str]) -> Dict:
        """
        Aplica filtros desabilitados ao contexto (API compatível)

        Args:
            context_dict: Contexto atual
            disabled_filter_ids: IDs dos filtros desabilitados

        Returns:
            Dict: Contexto filtrado
        """
        if not context_dict or not disabled_filter_ids:
            return context_dict

        # Atualizar estado interno com filtros desabilitados
        for filter_id in disabled_filter_ids:
            self.filter_state.disable_filter(filter_id)

        # Retornar contexto apenas com filtros ativos
        return self.filter_state.to_context_dict(only_active=True)

    def toggle_filter(self, filter_id: str, enabled: bool) -> bool:
        """
        Habilita/desabilita um filtro

        Args:
            filter_id: ID do filtro
            enabled: True para habilitar, False para desabilitar

        Returns:
            bool: True se operação foi bem-sucedida
        """
        if enabled:
            return self.filter_state.enable_filter(filter_id)
        else:
            return self.filter_state.disable_filter(filter_id)

    def clear_all_filters(self) -> bool:
        """Remove todos os filtros"""
        return self.filter_state.clear_all_filters()

    def get_filters_by_category(self, category: FilterCategory) -> Dict[str, FilterDefinition]:
        """Retorna filtros por categoria"""
        return self.filter_state.get_filters_by_category(category)

    def get_active_filters_summary(self) -> str:
        """Gera resumo dos filtros ativos"""
        active_filters = self.filter_state.get_active_filters()

        if not active_filters:
            return "Nenhum filtro ativo"

        # Agrupar por categoria
        by_category = {}
        for filter_def in active_filters.values():
            if filter_def.category not in by_category:
                by_category[filter_def.category] = 0
            by_category[filter_def.category] += 1

        summary_parts = []
        category_names = {
            FilterCategory.TEMPORAL: "temporal",
            FilterCategory.GEOGRAPHIC: "geográfico",
            FilterCategory.CLIENT: "cliente",
            FilterCategory.PRODUCT: "produto",
            FilterCategory.REPRESENTATIVE: "representante",
            FilterCategory.OTHER: "outros"
        }

        for category, count in by_category.items():
            name = category_names.get(category, "outros")
            summary_parts.append(f"{count} {name}")

        return f"Filtros ativos: {', '.join(summary_parts)}"

    def _dict_to_filter_definitions(self, filters_dict: Dict, source: str) -> List[FilterDefinition]:
        """Converte dicionário de filtros para lista de FilterDefinition"""
        filters = []

        for key, value in filters_dict.items():
            if key.startswith('_') or key == 'clear_all_filters':
                continue

            # Detectar operador
            operator = "="
            clean_key = key

            for op in [">=", "<=", ">", "<", "!=", "LIKE"]:
                if key.endswith(f"_{op}"):
                    operator = op
                    clean_key = key[:-len(f"_{op}")]
                    break

            # Determinar categoria
            category = FilterCategory.OTHER
            clean_key_lower = clean_key.lower()

            if clean_key_lower in ['data', 'data_inicio', 'data_fim', 'periodo', 'mes', 'ano']:
                category = FilterCategory.TEMPORAL
            elif clean_key_lower in ['uf_cliente', 'municipio_cliente', 'cidade', 'estado']:
                category = FilterCategory.GEOGRAPHIC
            elif clean_key_lower in ['cod_cliente', 'cod_segmento_cliente', 'cliente']:
                category = FilterCategory.CLIENT
            elif 'produto' in clean_key_lower or 'familia' in clean_key_lower:
                category = FilterCategory.PRODUCT
            elif 'vendedor' in clean_key_lower:
                category = FilterCategory.REPRESENTATIVE

            filter_def = FilterDefinition(
                key=clean_key,
                value=value,
                category=category,
                operator=operator,
                source=source
            )
            filters.append(filter_def)

        return filters


# Instância global para uso em toda a aplicação
_global_filter_manager: Optional[UnifiedFilterManager] = None


def get_unified_filter_manager(text_normalizer=None, alias_mapping=None) -> UnifiedFilterManager:
    """
    Singleton para obter instância global do UnifiedFilterManager
    """
    global _global_filter_manager

    if _global_filter_manager is None:
        _global_filter_manager = UnifiedFilterManager(text_normalizer, alias_mapping)

    return _global_filter_manager


def reset_unified_filter_manager():
    """Reset da instância global (útil para testes)"""
    global _global_filter_manager
    _global_filter_manager = None