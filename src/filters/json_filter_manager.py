"""
Sistema de Filtros Limpo - APENAS EXTRAÇÃO SQL
Remove completamente a extração baseada em texto/resposta,
focando exclusivamente na extração das cláusulas WHERE do SQL.
"""

import pandas as pd
from typing import Dict, List, Set, Optional, Tuple
import copy


class JSONFilterManager:
    """
    Gerenciador de filtros focado exclusivamente em extração SQL
    """

    def __init__(self, df_dataset: pd.DataFrame):
        """
        Inicializa o gerenciador com o dataset para validação

        Args:
            df_dataset: DataFrame com dados para validação de valores
        """
        self.df_dataset = df_dataset
        self.filtros_persistentes = {
            "periodo": {"Data": None},
            "regiao": {"UF_Cliente": [], "Municipio_Cliente": []},
            "cliente": {"Cod_Cliente": [], "Cod_Segmento_Cliente": []},
            "produto": {
                "Cod_Familia_Produto": [],
                "Cod_Grupo_Produto": [],
                "Cod_Linha_Produto": [],
                "Des_Linha_Produto": []
            },
            "representante": {"Cod_Vendedor": [], "Cod_Regiao_Vendedor": []}
        }

        # Gerar listas de valores válidos diretamente do dataset
        self._gerar_valores_validos()

    def _gerar_valores_validos(self):
        """Gera listas de valores válidos diretamente do dataset"""
        self.valores_validos = {}

        # Lista de colunas possíveis para validação
        colunas_validacao = [
            "UF_Cliente", "Municipio_Cliente", "Cod_Cliente", "Cod_Segmento_Cliente",
            "Cod_Linha_Produto", "Des_Linha_Produto", "Cod_Familia_Produto",
            "Cod_Grupo_Produto", "Cod_Vendedor", "Cod_Regiao_Vendedor"
        ]

        # Apenas adicionar colunas que existem no dataset
        for coluna in colunas_validacao:
            if coluna in self.df_dataset.columns:
                self.valores_validos[coluna] = self.df_dataset[coluna].dropna().unique().tolist()

    def validar_valores(self, campo: str, valores: List[str], categoria: str) -> List[str]:
        """
        Valida valores contra o dataset com estratégia permissiva

        Args:
            campo: Nome do campo
            valores: Lista de valores para validar
            categoria: Categoria do filtro

        Returns:
            Lista de valores válidos
        """
        # Campos que sempre são aceitos sem validação rígida
        campos_permissivos = [
            'Data', 'Data_>=', 'Data_<', 'periodo', 'mes', 'ano',  # Temporais
            'cidade', 'estado', 'municipio', 'uf',  # Regionais alternativos
            'cliente', 'produto', 'linha', 'segmento'  # Genéricos
        ]

        # Se é um campo permissivo, aceitar diretamente
        if campo in campos_permissivos or campo.lower() in [c.lower() for c in campos_permissivos]:
            return valores

        # Para campos com validação no dataset
        if campo in self.valores_validos:
            # Converter valores para string para comparação consistente
            valores_str = [str(v) for v in valores]
            validos_str = [str(v) for v in self.valores_validos[campo]]

            # Validação exata primeiro
            valores_exatos = [v for v in valores_str if v in validos_str]

            # Se não encontrou exatos, tentar validação fuzzy (parcial)
            if not valores_exatos and valores_str:
                valores_fuzzy = []
                for valor in valores_str:
                    # Busca parcial case-insensitive
                    matches = [v for v in validos_str if valor.upper() in v.upper() or v.upper() in valor.upper()]
                    if matches:
                        valores_fuzzy.extend(matches[:1])  # Apenas primeiro match

                if valores_fuzzy:
                    return valores_fuzzy

            return valores_exatos

        # Para campos não mapeados, aceitar como está (estratégia permissiva)
        return valores

    def sincronizar_com_contexto_agente(self, contexto_agente: Dict) -> bool:
        """
        Sincroniza filtros persistentes com contexto do agente

        Args:
            contexto_agente: Contexto do agente

        Returns:
            True se houve mudanças
        """
        houve_mudancas = False

        # Atualizar filtros persistentes com base no contexto do agente
        for campo, valor in contexto_agente.items():
            categoria = self._determinar_categoria(campo)
            if categoria and valor is not None:
                if categoria not in self.filtros_persistentes:
                    self.filtros_persistentes[categoria] = {}

                if self.filtros_persistentes[categoria].get(campo) != valor:
                    self.filtros_persistentes[categoria][campo] = valor
                    houve_mudancas = True

        return houve_mudancas

    def _determinar_categoria(self, campo: str) -> Optional[str]:
        """
        Determina a categoria de um campo

        Args:
            campo: Nome do campo

        Returns:
            Nome da categoria ou None
        """
        campo_lower = campo.lower()

        if campo_lower in ['data', 'data_>=', 'data_<', 'periodo', 'mes', 'ano']:
            return "periodo"
        elif campo_lower in ['uf_cliente', 'municipio_cliente', 'cidade', 'estado']:
            return "regiao"
        elif campo_lower in ['cod_cliente', 'cod_segmento_cliente', 'cliente']:
            return "cliente"
        elif any(x in campo_lower for x in ['produto', 'familia', 'grupo', 'linha']):
            return "produto"
        elif any(x in campo_lower for x in ['vendedor', 'representante', 'regiao_vendedor']):
            return "representante"

        return None

    def obter_contexto_para_agente(self) -> Dict:
        """
        Converte filtros persistentes para formato de contexto do agente

        Returns:
            Dicionário no formato esperado pelo agente
        """
        contexto = {}

        for categoria, campos in self.filtros_persistentes.items():
            for campo, valor in campos.items():
                if valor is not None and valor != [] and valor != "":
                    contexto[campo] = valor

        return contexto

    def obter_resumo_filtros_ativos(self) -> str:
        """
        Gera resumo textual dos filtros ativos

        Returns:
            String com resumo dos filtros
        """
        filtros_ativos = []

        for categoria, campos in self.filtros_persistentes.items():
            count = sum(1 for v in campos.values() if v is not None and v != [] and v != "")
            if count > 0:
                nome_categoria = {
                    "periodo": "temporal",
                    "regiao": "geográfico",
                    "cliente": "cliente",
                    "produto": "produto",
                    "representante": "representante"
                }.get(categoria, categoria)
                filtros_ativos.append(f"{count} {nome_categoria}")

        if not filtros_ativos:
            return "Nenhum filtro ativo"

        return f"Filtros ativos: {', '.join(filtros_ativos)}"

    def aplicar_filtros_desabilitados(self, contexto: Dict, filtros_desabilitados: Set[str]) -> Dict:
        """
        Remove filtros desabilitados do contexto

        Args:
            contexto: Contexto atual
            filtros_desabilitados: Set com IDs de filtros desabilitados

        Returns:
            Contexto filtrado
        """
        if not filtros_desabilitados:
            return contexto

        contexto_filtrado = {}

        for campo, valor in contexto.items():
            filter_id = f"{campo}:{valor}"

            # Tratamento especial para ranges de data
            if campo in ['Data_>=', 'Data_<'] and 'Data_>=' in contexto and 'Data_<' in contexto:
                start_date = contexto.get('Data_>=')
                end_date = contexto.get('Data_<')
                if start_date and end_date:
                    range_id = f"Data_range:{start_date}_{end_date}"
                    if range_id not in filtros_desabilitados:
                        contexto_filtrado[campo] = valor
            elif filter_id not in filtros_desabilitados:
                contexto_filtrado[campo] = valor

        return contexto_filtrado


# Instância global para uso em toda a aplicação
_global_json_filter_manager: Optional[JSONFilterManager] = None


def get_json_filter_manager(df_dataset: pd.DataFrame) -> JSONFilterManager:
    """
    Singleton para obter instância global do JSONFilterManager

    Args:
        df_dataset: DataFrame com dados

    Returns:
        Instância do JSONFilterManager
    """
    global _global_json_filter_manager

    if _global_json_filter_manager is None:
        _global_json_filter_manager = JSONFilterManager(df_dataset)

    return _global_json_filter_manager


def reset_json_filter_manager():
    """Reset da instância global (útil para testes)"""
    global _global_json_filter_manager
    _global_json_filter_manager = None


def processar_filtros_apenas_sql(sql_queries: List[str], contexto_atual: Dict,
                                df_dataset: pd.DataFrame) -> Tuple[Dict, List[str]]:
    """
    NOVA FUNÇÃO PRINCIPAL - Processa filtros APENAS das queries SQL

    Args:
        sql_queries: Lista de queries SQL executadas
        contexto_atual: Contexto atual de filtros
        df_dataset: DataFrame com dados para validação

    Returns:
        Tuple[Dict, List[str]]: (contexto_atualizado, lista_de_mudanças)
    """
    from .sql_filter_extractor import SQLFilterExtractor

    mudancas = []
    contexto_atualizado = contexto_atual.copy()

    if not sql_queries:
        mudancas.append("INFO: Nenhuma query SQL disponível - contexto mantido")
        return contexto_atualizado, mudancas

    # ÚNICA ESTRATÉGIA: Extrair filtros das queries SQL
    extractor = SQLFilterExtractor(df_dataset)
    sql_filters = extractor.extract_filters_from_multiple_queries(sql_queries)

    if sql_filters and any(sql_filters.values()):
        # Converter para formato de contexto
        sql_context = _convert_sql_json_to_context(sql_filters)

        if sql_context:
            # Merge com contexto existente
            old_keys = set(contexto_atualizado.keys())
            contexto_atualizado.update(sql_context)
            new_keys = set(contexto_atualizado.keys())

            added_keys = new_keys - old_keys
            if added_keys:
                mudancas.append(f"SQL: Filtros extraídos do SQL: {len(added_keys)} filtros detectados")
                for key in added_keys:
                    mudancas.append(f"  + {key} = {contexto_atualizado[key]}")
            else:
                mudancas.append("INFO: Filtros SQL extraídos, mas sem novas adições")

            return contexto_atualizado, mudancas

    mudancas.append("INFO: Nenhum filtro encontrado nas queries SQL")
    return contexto_atualizado, mudancas


def _convert_sql_json_to_context(sql_filters: Dict) -> Dict:
    """
    Converte estrutura JSON de filtros extraídos do SQL para formato de contexto

    Args:
        sql_filters: Filtros extraídos em formato JSON

    Returns:
        Dict no formato de contexto do agente
    """
    context = {}

    for category, fields in sql_filters.items():
        if not isinstance(fields, dict) or not fields:
            continue

        if category == 'periodo':
            # Processar estrutura temporal
            if 'mes' in fields and 'ano' in fields:
                # Mês específico - converter para range
                mes = fields['mes']
                ano = fields['ano']
                try:
                    from datetime import datetime

                    # Normalizar mes e ano
                    if isinstance(mes, str) and mes.isdigit():
                        mes_int = int(mes)
                    elif isinstance(mes, int):
                        mes_int = mes
                    else:
                        raise ValueError("Mês inválido")

                    ano_int = int(ano)

                    start_date = f"{ano_int}-{mes_int:02d}-01"

                    # Calcular primeiro dia do próximo mês
                    if mes_int == 12:
                        end_year = ano_int + 1
                        end_month = 1
                    else:
                        end_year = ano_int
                        end_month = mes_int + 1

                    end_date = f"{end_year}-{end_month:02d}-01"

                    context['Data_>='] = start_date
                    context['Data_<'] = end_date
                except (ValueError, TypeError):
                    # Fallback para formato simples
                    context['periodo'] = f"{mes}/{ano}"

            elif 'inicio' in fields and 'fim' in fields:
                # Intervalo de meses
                inicio = fields['inicio']
                fim = fields['fim']

                if isinstance(inicio, dict) and isinstance(fim, dict):
                    try:
                        start_mes = inicio.get('mes')
                        start_ano = inicio.get('ano')
                        end_mes = fim.get('mes')
                        end_ano = fim.get('ano')

                        if all([start_mes, start_ano, end_mes, end_ano]):
                            # Normalizar valores
                            start_mes_int = int(start_mes)
                            start_ano_int = int(start_ano)
                            end_mes_int = int(end_mes)
                            end_ano_int = int(end_ano)

                            start_date = f"{start_ano_int}-{start_mes_int:02d}-01"

                            # Calcular primeiro dia do mês seguinte ao fim
                            if end_mes_int == 12:
                                next_year = end_ano_int + 1
                                next_month = 1
                            else:
                                next_year = end_ano_int
                                next_month = end_mes_int + 1

                            end_date = f"{next_year}-{next_month:02d}-01"

                            context['Data_>='] = start_date
                            context['Data_<'] = end_date
                    except (ValueError, TypeError):
                        # Fallback
                        context['periodo'] = f"{inicio} até {fim}"

            # Campos diretos de data
            for field, value in fields.items():
                if field in ['Data_>=', 'Data_<', 'Data_<=', 'Data_>', 'Data']:
                    context[field] = value

        else:
            # Para outras categorias, mapear campos diretamente
            for field, value in fields.items():
                if value is not None and value != [] and value != "":
                    context[field] = value

    return context