"""
Resolvedor de conflitos entre filtros
"""

from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import re


class FilterConflictResolver:
    """
    Resolve conflitos entre filtros quando múltiplos filtros afetam a mesma dimensão
    """

    def __init__(self):
        # Definir grupos de filtros que podem conflitar
        self.conflict_groups = {
            'temporal': [
                'Data', 'Data_>=', 'Data_<', 'Data_<=', 'Data_>',
                'ano', 'mes', 'periodo', 'Data_Inicio', 'Data_Fim'
            ],
            'geographic': [
                'UF_Cliente', 'Municipio_Cliente', 'cidade', 'estado',
                'municipio', 'uf', 'regiao'
            ],
            'client': [
                'Cod_Cliente', 'Cliente', 'cliente',
                'Cod_Segmento_Cliente', 'segmento'
            ],
            'product': [
                'Cod_Familia_Produto', 'Cod_Grupo_Produto', 'Cod_Linha_Produto',
                'Des_Linha_Produto', 'Produto', 'produto', 'linha'
            ],
            'representative': [
                'Cod_Vendedor', 'Cod_Regiao_Vendedor', 'vendedor',
                'representante', 'regiao_vendedor'
            ]
        }

        # Prioridades para resolução de conflitos (maior número = maior prioridade)
        self.field_priorities = {
            # Temporal - formatos específicos têm prioridade
            'Data_>=': 10, 'Data_<': 10, 'Data_<=': 10,
            'Data': 8, 'periodo': 7, 'ano': 5, 'mes': 6,

            # Geographic - códigos têm prioridade sobre nomes
            'UF_Cliente': 10, 'Municipio_Cliente': 9,
            'estado': 8, 'cidade': 8, 'uf': 7, 'municipio': 7,

            # Cliente - códigos têm prioridade
            'Cod_Cliente': 10, 'Cod_Segmento_Cliente': 9,
            'Cliente': 8, 'cliente': 7,

            # Produto - códigos específicos têm prioridade
            'Cod_Linha_Produto': 10, 'Des_Linha_Produto': 9,
            'Cod_Familia_Produto': 8, 'Cod_Grupo_Produto': 8,
            'Produto': 7, 'produto': 6,

            # Representante
            'Cod_Vendedor': 10, 'Cod_Regiao_Vendedor': 9,
            'vendedor': 8, 'representante': 7
        }

    def resolve_conflicts(self, new_filters: Dict, existing_context: Dict,
                         resolution_strategy: str = 'smart') -> Tuple[Dict, List[str]]:
        """
        Resolve conflitos entre novos filtros e contexto existente

        Args:
            new_filters: Novos filtros detectados
            existing_context: Contexto atual
            resolution_strategy: Estratégia de resolução ('smart', 'replace', 'merge', 'keep_existing')

        Returns:
            Tuple[Dict, List[str]]: (contexto_resolvido, lista_de_conflitos_detectados)
        """
        resolved_context = existing_context.copy()
        conflicts_detected = []

        if not new_filters:
            return resolved_context, conflicts_detected

        # Verificar comando de limpeza
        if new_filters.get('clear_all_filters'):
            return {}, ['Todos os filtros foram limpos']

        # Detectar conflitos por grupo
        for group_name, fields in self.conflict_groups.items():
            group_conflicts = self._detect_group_conflicts(new_filters, existing_context, fields)

            if group_conflicts:
                conflicts_detected.extend([f"Conflito em {group_name}: {c}" for c in group_conflicts])

                # Resolver conflitos conforme estratégia
                if resolution_strategy == 'smart':
                    resolved_filters = self._smart_conflict_resolution(
                        new_filters, existing_context, fields, group_name
                    )
                elif resolution_strategy == 'replace':
                    resolved_filters = self._replace_resolution(new_filters, existing_context, fields)
                elif resolution_strategy == 'merge':
                    resolved_filters = self._merge_resolution(new_filters, existing_context, fields)
                elif resolution_strategy == 'keep_existing':
                    resolved_filters = self._keep_existing_resolution(new_filters, existing_context, fields)
                else:
                    resolved_filters = self._smart_conflict_resolution(
                        new_filters, existing_context, fields, group_name
                    )

                # Aplicar filtros resolvidos
                resolved_context.update(resolved_filters)
            else:
                # Sem conflitos - adicionar novos filtros normalmente
                for field in fields:
                    if field in new_filters:
                        resolved_context[field] = new_filters[field]

        # Adicionar campos que não pertencem a nenhum grupo
        ungrouped_fields = self._get_ungrouped_fields(new_filters)
        for field, value in ungrouped_fields.items():
            resolved_context[field] = value

        return resolved_context, conflicts_detected

    def _detect_group_conflicts(self, new_filters: Dict, existing_context: Dict,
                              fields: List[str]) -> List[str]:
        """Detecta conflitos dentro de um grupo de campos"""
        conflicts = []

        new_fields_in_group = {f: v for f, v in new_filters.items() if f in fields}
        existing_fields_in_group = {f: v for f, v in existing_context.items() if f in fields}

        if not new_fields_in_group or not existing_fields_in_group:
            return conflicts

        # Verificar conflitos diretos (mesmo campo, valores diferentes)
        for field in new_fields_in_group:
            if field in existing_fields_in_group:
                if new_fields_in_group[field] != existing_fields_in_group[field]:
                    conflicts.append(f"{field}: '{existing_fields_in_group[field]}' vs '{new_fields_in_group[field]}'")

        # Verificar conflitos semânticos (campos diferentes, mesma dimensão)
        if len(new_fields_in_group) > 0 and len(existing_fields_in_group) > 0:
            # Se há campos novos e existentes do mesmo grupo, pode haver conflito semântico
            for new_field in new_fields_in_group:
                for existing_field in existing_fields_in_group:
                    if new_field != existing_field:
                        # Verificar se são semanticamente conflitantes
                        if self._are_semantically_conflicting(new_field, existing_field, fields):
                            conflicts.append(f"Conflito semântico: {existing_field} vs {new_field}")

        return conflicts

    def _are_semantically_conflicting(self, field1: str, field2: str, group_fields: List[str]) -> bool:
        """Verifica se dois campos são semanticamente conflitantes"""
        # Regras específicas por grupo
        temporal_exclusives = [
            ['Data_>=', 'ano'], ['Data_<', 'ano'], ['Data', 'periodo'],
            ['mes', 'Data_>='], ['mes', 'Data_<']
        ]

        geographic_exclusives = [
            ['UF_Cliente', 'Municipio_Cliente'],  # Estado vs Cidade
            ['estado', 'cidade']
        ]

        all_exclusives = temporal_exclusives + geographic_exclusives

        for exclusive_pair in all_exclusives:
            if (field1 in exclusive_pair and field2 in exclusive_pair and
                field1 != field2 and all(f in group_fields for f in exclusive_pair)):
                return True

        return False

    def _smart_conflict_resolution(self, new_filters: Dict, existing_context: Dict,
                                 fields: List[str], group_name: str) -> Dict:
        """Resolução inteligente baseada em prioridades e contexto"""
        resolved = {}

        new_fields_in_group = {f: v for f, v in new_filters.items() if f in fields}
        existing_fields_in_group = {f: v for f, v in existing_context.items() if f in fields}

        # Estratégia específica por grupo
        if group_name == 'temporal':
            resolved = self._resolve_temporal_conflicts(new_fields_in_group, existing_fields_in_group)
        elif group_name == 'geographic':
            resolved = self._resolve_geographic_conflicts(new_fields_in_group, existing_fields_in_group)
        else:
            # Resolução genérica baseada em prioridades
            resolved = self._resolve_by_priority(new_fields_in_group, existing_fields_in_group)

        return resolved

    def _resolve_temporal_conflicts(self, new_fields: Dict, existing_fields: Dict) -> Dict:
        """Resolução específica para conflitos temporais"""
        resolved = existing_fields.copy()

        # Prioridade: ranges de data > datas específicas > períodos nomeados
        if any(f in new_fields for f in ['Data_>=', 'Data_<', 'Data_<=']):
            # Novo filtro é um range - substitui tudo
            resolved = {f: v for f, v in new_fields.items()}
        elif any(f in existing_fields for f in ['Data_>=', 'Data_<', 'Data_<=']):
            # Existente é range, novo não é - manter existente
            pass
        elif 'Data' in new_fields:
            # Nova data específica - substitui períodos nomeados
            resolved = {f: v for f, v in new_fields.items()}
        else:
            # Usar prioridades normais
            resolved = self._resolve_by_priority(new_fields, existing_fields)

        return resolved

    def _resolve_geographic_conflicts(self, new_fields: Dict, existing_fields: Dict) -> Dict:
        """Resolução específica para conflitos geográficos"""
        resolved = existing_fields.copy()

        # Se novo filtro é mais específico (cidade vs estado), usar o novo
        if 'Municipio_Cliente' in new_fields or 'cidade' in new_fields:
            # Cidade é mais específica que estado
            resolved = {f: v for f, v in new_fields.items()}
        elif ('UF_Cliente' in new_fields or 'estado' in new_fields) and not any(
            f in existing_fields for f in ['Municipio_Cliente', 'cidade']
        ):
            # Novo estado, sem cidade existente
            resolved.update(new_fields)
        else:
            # Usar prioridades normais
            resolved = self._resolve_by_priority(new_fields, existing_fields)

        return resolved

    def _resolve_by_priority(self, new_fields: Dict, existing_fields: Dict) -> Dict:
        """Resolução genérica baseada em prioridades"""
        resolved = existing_fields.copy()

        for field, value in new_fields.items():
            new_priority = self.field_priorities.get(field, 5)

            # Verificar se há conflito com campos existentes
            conflict_found = False
            for existing_field in existing_fields:
                existing_priority = self.field_priorities.get(existing_field, 5)

                if new_priority > existing_priority:
                    # Novo campo tem prioridade maior - substitui
                    resolved.pop(existing_field, None)
                    resolved[field] = value
                    conflict_found = True
                elif new_priority < existing_priority:
                    # Campo existente tem prioridade maior - manter existente
                    conflict_found = True

            if not conflict_found:
                # Sem conflito direto - adicionar
                resolved[field] = value

        return resolved

    def _replace_resolution(self, new_filters: Dict, existing_context: Dict, fields: List[str]) -> Dict:
        """Estratégia de substituição total"""
        new_fields_in_group = {f: v for f, v in new_filters.items() if f in fields}
        return new_fields_in_group if new_fields_in_group else existing_context

    def _merge_resolution(self, new_filters: Dict, existing_context: Dict, fields: List[str]) -> Dict:
        """Estratégia de mesclagem (manter ambos quando possível)"""
        resolved = {f: v for f, v in existing_context.items() if f in fields}
        new_fields_in_group = {f: v for f, v in new_filters.items() if f in fields}

        # Adicionar novos campos que não conflitam diretamente
        for field, value in new_fields_in_group.items():
            if field not in resolved:
                resolved[field] = value

        return resolved

    def _keep_existing_resolution(self, new_filters: Dict, existing_context: Dict, fields: List[str]) -> Dict:
        """Estratégia de manter existente"""
        return {f: v for f, v in existing_context.items() if f in fields}

    def _get_ungrouped_fields(self, filters: Dict) -> Dict:
        """Retorna campos que não pertencem a nenhum grupo"""
        all_grouped_fields = set()
        for fields in self.conflict_groups.values():
            all_grouped_fields.update(fields)

        return {f: v for f, v in filters.items()
                if f not in all_grouped_fields and not f.startswith('_')}

    def analyze_filter_compatibility(self, filters: Dict) -> Dict:
        """
        Analisa a compatibilidade dos filtros fornecidos

        Returns:
            Dict com análise de compatibilidade
        """
        analysis = {
            'compatible': True,
            'conflicts': [],
            'warnings': [],
            'suggestions': []
        }

        # Verificar conflitos internos
        for group_name, fields in self.conflict_groups.items():
            group_filters = {f: v for f, v in filters.items() if f in fields}

            if len(group_filters) > 1:
                # Múltiplos filtros no mesmo grupo - verificar compatibilidade
                conflicts = self._analyze_group_compatibility(group_filters, group_name)
                if conflicts:
                    analysis['compatible'] = False
                    analysis['conflicts'].extend(conflicts)

        # Verificar combinações problemáticas
        warnings = self._detect_problematic_combinations(filters)
        analysis['warnings'].extend(warnings)

        # Gerar sugestões
        suggestions = self._generate_filter_suggestions(filters)
        analysis['suggestions'].extend(suggestions)

        return analysis

    def _analyze_group_compatibility(self, group_filters: Dict, group_name: str) -> List[str]:
        """Analisa compatibilidade dentro de um grupo"""
        conflicts = []

        if group_name == 'temporal':
            # Verificar ranges inválidos
            if 'Data_>=' in group_filters and 'Data_<' in group_filters:
                try:
                    start_date = datetime.strptime(group_filters['Data_>='], '%Y-%m-%d')
                    end_date = datetime.strptime(group_filters['Data_<'], '%Y-%m-%d')
                    if start_date >= end_date:
                        conflicts.append("Range de datas inválido: data início >= data fim")
                except:
                    conflicts.append("Formato de data inválido no range temporal")

        elif group_name == 'geographic':
            # Verificar hierarquia geográfica
            if ('UF_Cliente' in group_filters and 'Municipio_Cliente' in group_filters):
                # Idealmente validaria se a cidade pertence ao estado, mas por agora só avisa
                pass

        return conflicts

    def _detect_problematic_combinations(self, filters: Dict) -> List[str]:
        """Detecta combinações de filtros que podem ser problemáticas"""
        warnings = []

        # Filtros muito restritivos
        filter_count = len([k for k in filters.keys() if not k.startswith('_')])
        if filter_count > 5:
            warnings.append(f"Muitos filtros ativos ({filter_count}) - resultado pode ser muito restritivo")

        # Combinações específicas problemáticas
        if 'Cod_Cliente' in filters and 'UF_Cliente' in filters:
            warnings.append("Filtro por cliente específico + estado pode ser redundante")

        return warnings

    def _generate_filter_suggestions(self, filters: Dict) -> List[str]:
        """Gera sugestões para melhorar os filtros"""
        suggestions = []

        # Sugestões para melhorar especificidade
        if 'ano' in filters and 'mes' not in filters:
            suggestions.append("Considere adicionar um filtro de mês para maior especificidade")

        if 'UF_Cliente' in filters and 'Municipio_Cliente' not in filters:
            suggestions.append("Considere especificar uma cidade dentro do estado")

        # Sugestões para simplificar
        temporal_filters = [f for f in filters.keys() if f in self.conflict_groups['temporal']]
        if len(temporal_filters) > 2:
            suggestions.append("Considere usar um range de datas único em vez de múltiplos filtros temporais")

        return suggestions