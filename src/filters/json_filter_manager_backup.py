"""
Sistema de Filtros Automáticos via JSON Response
Implementação simplificada que usa apenas o JSON de response do modelo
para extrair e gerenciar filtros persistentes
"""

import json
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Set, Optional, Tuple, Any, Union
import copy
import re


class JSONFilterManager:
    """
    Gerenciador de filtros que funciona exclusivamente com JSON response do modelo
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
        Valida valores contra o dataset com estratégia mais permissiva

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

    def processar_json_response(self, response_text: str, contexto_atual: Dict) -> Tuple[Dict, List[str]]:
        """
        Processa o JSON de response do modelo para extrair filtros

        Args:
            response_text: Texto da response do modelo
            contexto_atual: Contexto atual de filtros

        Returns:
            Tuple[Dict, List[str]]: (contexto_atualizado, lista_de_mudanças)
        """
        try:
            # Extrair JSON da response
            json_filtros = self._extrair_json_da_response(response_text)

            if not json_filtros:
                return contexto_atual, []

            # Processar filtros do JSON
            return self._atualizar_filtros_do_json(json_filtros, contexto_atual)

        except Exception as e:
            # Em caso de erro, retornar contexto atual inalterado
            return contexto_atual, [f"Erro ao processar JSON: {str(e)}"]

    def _extrair_json_da_response(self, response_text: str) -> Optional[Dict]:
        """
        Extrai JSON da response do modelo usando múltiplas estratégias CORRIGIDAS

        Args:
            response_text: Texto da response

        Returns:
            Dict com filtros extraídos ou None se não encontrar
        """
        # Estratégia 1: Procurar por bloco JSON entre ```json``` (mais permissivo)
        json_block_match = re.search(r'```json\s*\n?(.*?)\n?```', response_text, re.DOTALL | re.IGNORECASE)
        if json_block_match:
            json_content = json_block_match.group(1).strip()

            # CORREÇÃO: Limpar escape de aspas que podem estar causando problemas
            json_content = json_content.replace('\\"', '"')

            try:
                return json.loads(json_content)
            except json.JSONDecodeError as e:
                # Log do erro para debug e tentar parsear linha por linha
                print(f"Erro ao fazer parse do JSON em bloco: {e}")
                print(f"Conteúdo original: {json_content[:500]}...")

                # Tentar corrigir escape duplo
                try:
                    json_content_fixed = json_content.replace('\\', '')
                    return json.loads(json_content_fixed)
                except json.JSONDecodeError:
                    pass

        # Estratégia 2: Procurar por JSON direto no texto (sem markdown)
        # Padrão mais amplo para capturar estruturas JSON
        json_pattern = r'\{[^{}]*?"(?:periodo|regiao|cliente|produto|representante)"[^{}]*?\}|\{(?:[^{}]|"[^"]*")*?"(?:periodo|regiao|cliente|produto|representante)"(?:[^{}]|"[^"]*")*?\}'
        json_matches = re.findall(json_pattern, response_text, re.DOTALL)

        for match in json_matches:
            try:
                return json.loads(match)
            except json.JSONDecodeError:
                continue

        # Estratégia 3: Procurar estrutura multi-linha mais complexa
        # Captura JSONs que podem ter quebras de linha
        multiline_pattern = r'\{[\s\S]*?"(?:periodo|regiao|cliente|produto|representante)"[\s\S]*?\}'
        multiline_matches = re.findall(multiline_pattern, response_text)

        for match in multiline_matches:
            try:
                return json.loads(match)
            except json.JSONDecodeError:
                continue

        # Estratégia 4: Procurar JSON no final da response com padrão mais amplo
        # O modelo às vezes coloca JSON no final sem markdown
        json_patterns = [
            r'\{[^{}]*?\"periodo\"[^{}]*?\}(?:\s*json\s*)?$',  # Original
            r'\{[^{}]*?periodo[^{}]*?\}(?:\s*json\s*)?$',      # Sem aspas
            r'\{.*?periodo.*?\}(?:\s*json\s*)?$',              # Mais amplo
        ]

        for pattern in json_patterns:
            json_end_matches = re.findall(pattern, response_text, re.MULTILINE | re.DOTALL)

            for match in json_end_matches:
                # Limpar possível sufixo 'json'
                clean_match = re.sub(r'\s*json\s*$', '', match.strip())

                # CORREÇÃO: Limpar diferentes tipos de escape
                clean_match = clean_match.replace('\\\\\\\"', '\\"')  # Triple escape
                clean_match = clean_match.replace('\\\\\"', '\"')     # Double escape
                clean_match = clean_match.replace('\\"', '"')         # Single escape

                try:
                    return json.loads(clean_match)
                except json.JSONDecodeError:
                    # Tentar remover todos os escapes
                    try:
                        clean_match_fixed = clean_match.replace('\\', '')
                        return json.loads(clean_match_fixed)
                    except json.JSONDecodeError:
                        continue

        # Estratégia 5: Procurar estrutura completa na response
        # Padrão para detectar estrutura de filtros mesmo sem JSON válido
        if any(keyword in response_text.lower() for keyword in ['periodo', 'regiao', 'cliente', 'produto', 'representante']):
            return self._extrair_filtros_por_texto(response_text)

        return None

    def _extrair_filtros_por_texto(self, response_text: str) -> Dict:
        """
        Extrai filtros analisando texto quando JSON não está disponível
        VERSÃO APRIMORADA COM PRESERVAÇÃO DE GRANULARIDADE TEMPORAL

        Args:
            response_text: Texto da response

        Returns:
            Dict com estrutura de filtros preservando granularidade de mês/ano
        """
        filtros = {
            "periodo": {},
            "regiao": {"UF_Cliente": None, "Municipio_Cliente": None},
            "cliente": {"Cod_Cliente": None, "Cod_Segmento_Cliente": None},
            "produto": {
                "Cod_Familia_Produto": None,
                "Cod_Grupo_Produto": None,
                "Cod_Linha_Produto": None,
                "Des_Linha_Produto": None
            },
            "representante": {"Cod_Vendedor": None, "Cod_Regiao_Vendedor": None}
        }

        text_lower = response_text.lower()

        # DETECÇÃO TEMPORAL COM PRESERVAÇÃO DE GRANULARIDADE
        # Usar TextNormalizer para análise avançada
        try:
            from src.text_normalizer import normalizer

            # Obter dados temporais estruturados
            structured_temporal = normalizer.get_structured_temporal_data(response_text)

            if structured_temporal and "periodo" in structured_temporal:
                filtros["periodo"] = structured_temporal["periodo"]
                # VALIDAÇÃO CRÍTICA: Verificar se interval foi detectado corretamente
                if "_debug_interval" in structured_temporal:
                    debug_info = structured_temporal["_debug_interval"]
                    # Log para debug mode apenas
                    filtros["_temporal_parsing_log"] = f"Interval detected: {debug_info['start_month_name']} to {debug_info['end_month_name']} via pattern {debug_info['pattern_used']}"
            else:
                # Fallback para parsing básico se estruturado falhar
                self._apply_basic_temporal_parsing(text_lower, filtros)

        except Exception as e:
            # Fallback para parsing básico em caso de erro
            self._apply_basic_temporal_parsing(text_lower, filtros)
            filtros["_temporal_parsing_error"] = str(e)

        # DETECÇÃO GEOGRÁFICA APRIMORADA
        # UFs (siglas de estado)
        uf_pattern = r'\b([A-Z]{2})\b'
        uf_matches = re.findall(uf_pattern, response_text)
        if "UF_Cliente" in self.valores_validos:
            valid_ufs = [uf for uf in uf_matches if uf in self.valores_validos["UF_Cliente"]]
            if valid_ufs:
                filtros["regiao"]["UF_Cliente"] = valid_ufs

        # DETECÇÃO RESTRITIVA DE CIDADES - APENAS CORRESPONDÊNCIA EXATA
        if "Municipio_Cliente" in self.valores_validos:
            for cidade_valida in self.valores_validos["Municipio_Cliente"]:
                cidade_patterns = [
                    cidade_valida.lower(),
                    cidade_valida.lower().replace(' ', ''),  # Sem espaços
                    cidade_valida.lower().replace('ã', 'a').replace('ç', 'c').replace('õ', 'o'),  # Normalizado
                    # Padrões específicos conhecidos
                    cidade_valida.lower().replace('são', 'sao').replace('joão', 'joao')
                ]

                for pattern in cidade_patterns:
                    # CORREÇÃO: Busca APENAS por correspondência exata ou palavra completa
                    # Usar word boundary para evitar falsas correspondências
                    if (pattern == text_lower.strip() or  # Texto exato
                        f" {pattern} " in f" {text_lower} " or  # Palavra completa com espaços
                        text_lower.startswith(f"{pattern} ") or  # Começa com palavra
                        text_lower.endswith(f" {pattern}") or  # Termina com palavra
                        f"/{pattern}/" in text_lower or  # Entre barras (URLs)
                        f"'{pattern}'" in text_lower or  # Entre aspas simples
                        f'"{pattern}"' in text_lower):  # Entre aspas duplas
                        filtros["regiao"]["Municipio_Cliente"] = [cidade_valida]
                        break

                if filtros["regiao"]["Municipio_Cliente"]:
                    break

        # DETECÇÃO DE SEGMENTOS DE CLIENTE
        segmento_patterns = {
            'atacado': ['ATACADO'],
            'varejo': ['VAREJO'],
            'industria': ['INDUSTRIA', 'INDUSTRIAL'],
            'construcao': ['CONSTRUCAO', 'CONSTRUÇÃO'],
            'agropecuario': ['AGROPECUARIO', 'AGROPECUÁRIA'],
        }

        for palavra_chave, segmentos_validos in segmento_patterns.items():
            if palavra_chave in text_lower:
                if "Cod_Segmento_Cliente" in self.valores_validos:
                    # Verificar se algum dos segmentos válidos existe no dataset
                    for segmento in segmentos_validos:
                        if segmento in self.valores_validos["Cod_Segmento_Cliente"]:
                            filtros["cliente"]["Cod_Segmento_Cliente"] = [segmento]
                            break

        # DETECÇÃO DE PRODUTOS POR PALAVRAS-CHAVE
        produto_keywords = {
            'tubos': ['Des_Linha_Produto'],
            'conexoes': ['Des_Linha_Produto'],
            'soldavel': ['Des_Linha_Produto'],
            'esgoto': ['Des_Linha_Produto'],
            'pvc': ['Des_Linha_Produto'],
        }

        for keyword, campos in produto_keywords.items():
            if keyword in text_lower:
                for campo in campos:
                    if campo in self.valores_validos:
                        # Buscar produtos que contenham a palavra-chave
                        produtos_encontrados = [
                            p for p in self.valores_validos[campo]
                            if keyword.upper() in str(p).upper()
                        ]
                        if produtos_encontrados:
                            filtros["produto"][campo] = produtos_encontrados[:3]  # Limitar a 3 primeiros
                            break

        return filtros

    def _apply_basic_temporal_parsing(self, text_lower: str, filtros: Dict) -> None:
        """
        Aplica parsing temporal básico como fallback - VERSÃO RESTRITIVA

        Args:
            text_lower: Texto em minúsculas
            filtros: Dicionário de filtros a ser atualizado
        """
        # Padrões de data específicos (mais restritivos)
        date_patterns = [
            r'(\d{4}-\d{2}-\d{2})',  # 2023-01-15
            r'(\d{2}/\d{2}/\d{4})',  # 15/01/2023
            # REMOVIDO: r'(\d{4})', - muito amplo, captura códigos de cliente
        ]

        # Padrão de ano apenas em contexto temporal explícito
        year_context_patterns = [
            r'\b(?:ano|year)\s+(\d{4})\b',         # "ano 2023"
            r'\bem\s+(\d{4})\b',                   # "em 2024"
            r'\b(\d{4})\s*(?:ano|year)\b',         # "2023 ano"
            r'\bde\s+(\d{4})\b',                   # "de 2023"
            r'\b(\d{4})\s*[-/]\s*\d{1,2}\b',      # "2023-01", "2023/12"
        ]

        # Primeiro tentar padrões de data completos
        for pattern in date_patterns:
            matches = re.findall(pattern, text_lower)
            if matches:
                match = matches[0]
                # Validar se é uma data plausível (não código de cliente)
                if self._is_plausible_date(match):
                    filtros["periodo"]["Data"] = match
                    return

        # Depois tentar anos em contexto temporal
        for pattern in year_context_patterns:
            matches = re.findall(pattern, text_lower)
            if matches:
                year = matches[0]
                # Validar se é um ano plausível (1990-2030)
                if self._is_plausible_year(year):
                    filtros["periodo"]["Data"] = year
                    return

    def _is_plausible_date(self, date_str: str) -> bool:
        """
        Verifica se uma string representa uma data plausível

        Args:
            date_str: String de data

        Returns:
            bool: True se é data plausível
        """
        try:
            if '-' in date_str:
                # Formato YYYY-MM-DD
                parts = date_str.split('-')
                if len(parts) == 3:
                    year, month, day = int(parts[0]), int(parts[1]), int(parts[2])
                    return (1990 <= year <= 2030 and 1 <= month <= 12 and 1 <= day <= 31)
            elif '/' in date_str:
                # Formato DD/MM/YYYY
                parts = date_str.split('/')
                if len(parts) == 3:
                    day, month, year = int(parts[0]), int(parts[1]), int(parts[2])
                    return (1990 <= year <= 2030 and 1 <= month <= 12 and 1 <= day <= 31)
            else:
                # Apenas ano
                year = int(date_str)
                return self._is_plausible_year(str(year))
        except (ValueError, IndexError):
            pass
        return False

    def _is_plausible_year(self, year_str: str) -> bool:
        """
        Verifica se uma string representa um ano plausível

        Args:
            year_str: String do ano

        Returns:
            bool: True se é ano plausível (1990-2030)
        """
        try:
            year = int(year_str)
            return 1990 <= year <= 2030
        except ValueError:
            return False

    def _atualizar_filtros_do_json(self, json_filtros: Dict, contexto_atual: Dict) -> Tuple[Dict, List[str]]:
        """
        Atualiza filtros persistentes com base no JSON extraído - VERSÃO MERGE INTELIGENTE APRIMORADA

        Args:
            json_filtros: Filtros extraídos do JSON
            contexto_atual: Contexto atual

        Returns:
            Tuple[Dict, List[str]]: (contexto_atualizado, mudanças)
        """
        mudancas = []
        # MUDANÇA CRÍTICA: Começar com contexto atual para preservar filtros existentes
        contexto_atualizado = copy.deepcopy(contexto_atual)

        # LOG: Estado inicial
        mudancas.append(f"Contexto inicial: {len(contexto_atual)} filtros ativos")

        # Comando de limpeza
        if json_filtros.get('clear_all_filters') or json_filtros.get('limpar_filtros'):
            self._limpar_todos_filtros()
            return {}, ["Todos os filtros foram removidos"]

        # LOG: JSON extraído
        total_campos_json = sum(len(campos) for categoria, campos in json_filtros.items()
                              if categoria in ["periodo", "regiao", "cliente", "produto", "representante"])
        mudancas.append(f"JSON extraído: {total_campos_json} campos em {len(json_filtros)} categorias")

        # MERGE INTELIGENTE: Processar cada categoria com estratégia de combinação
        contexto_antes_merge = len(contexto_atualizado)
        for categoria, campos in json_filtros.items():
            if categoria in ["periodo", "regiao", "cliente", "produto", "representante"]:
                mudancas_categoria = self._processar_categoria_com_merge(
                    categoria, campos, contexto_atualizado
                )
                mudancas.extend(mudancas_categoria)

        # Processar remoções explícitas
        if json_filtros.get('remover_filtros'):
            mudancas_remocao = self._processar_remocoes(json_filtros['remover_filtros'], contexto_atualizado)
            mudancas.extend(mudancas_remocao)

        # VALIDAÇÃO DE SANIDADE: Detectar perda de filtros
        contexto_depois_merge = len(contexto_atualizado)
        if contexto_atual and contexto_depois_merge < len(contexto_atual):
            filtros_perdidos = set(contexto_atual.keys()) - set(contexto_atualizado.keys())
            if filtros_perdidos:
                mudancas.append(f"ALERTA: {len(filtros_perdidos)} filtros foram perdidos: {filtros_perdidos}")

        # LOG: Resultado final
        mudancas.append(f"Contexto final: {len(contexto_atualizado)} filtros ativos")

        return contexto_atualizado, mudancas

    def _processar_categoria_com_merge(self, categoria: str, campos: Dict, contexto: Dict) -> List[str]:
        """
        Processa filtros de uma categoria específica COM MERGE INTELIGENTE CORRIGIDO

        Estratégia CORRIGIDA:
        1. Se o JSON contém `null` para um campo, PRESERVAR valor existente no contexto (CORREÇÃO CRÍTICA)
        2. Se o JSON contém valor, ADICIONAR/SOBRESCREVER dependendo do tipo
        3. Para listas, fazer merge; para valores únicos, substituir
        4. GARANTIR que valores existentes não sejam perdidos quando JSON tem null
        5. TRATAMENTO ESPECIAL para categoria 'periodo' com estrutura inicio/fim

        Args:
            categoria: Nome da categoria
            campos: Dicionário com campos e valores do JSON
            contexto: Contexto sendo atualizado

        Returns:
            Lista de mudanças realizadas
        """
        mudancas = []

        # TRATAMENTO ESPECIAL PARA CATEGORIA PERÍODO
        if categoria == "periodo":
            return self._processar_periodo_estruturado(campos, contexto)

        for campo, valor_json in campos.items():
            valor_existente = contexto.get(campo)

            # CORREÇÃO CRÍTICA: Se JSON tem `null`, verificar se há valor existente para preservar
            if valor_json is None:
                # Se há valor existente no contexto, PRESERVÁ-LO explicitamente
                if valor_existente is not None and valor_existente != [] and valor_existente != "":
                    # Manter o valor existente - não removê-lo
                    mudancas.append(f"Preservado: {campo} = {valor_existente} (JSON null, mas valor existe)")
                # Se não há valor existente, realmente não fazer nada
                continue

            # REGRA 2: Se JSON tem valor, aplicar merge inteligente com validação aprimorada

            # Validar valores do JSON com logs detalhados
            if isinstance(valor_json, list):
                valores_validos = self.validar_valores(campo, valor_json, categoria)
                if not valores_validos and valor_json:  # Log apenas se havia valores mas foram rejeitados
                    mudancas.append(f"Rejeitados valores inválidos para {campo}: {valor_json}")
            else:
                valores_validos = self.validar_valores(campo, [valor_json], categoria)
                if valores_validos:
                    valores_validos = valores_validos[0]  # Único valor
                elif valor_json:  # Log apenas se havia valor mas foi rejeitado
                    mudancas.append(f"Rejeitado valor inválido para {campo}: {valor_json}")

            # CORREÇÃO: Não pular se valores_validos está vazio - aplicar fallback para preservação
            if not valores_validos:
                # FALLBACK: Se validação falhou mas há valor no JSON, tentar preservar valor existente
                if valor_existente is not None and valor_existente != [] and valor_existente != "":
                    mudancas.append(f"Preservado por fallback: {campo} = {valor_existente} (validação falhou)")
                continue

            # MERGE STRATEGY baseado no tipo de campo
            if self._campo_permite_multiplos_valores(campo):
                # Campos que permitem listas (ex: Municipio_Cliente)
                valores_finais = self._merge_lista_valores(valor_existente, valores_validos)
                if valores_finais != valor_existente:
                    contexto[campo] = valores_finais
                    mudancas.append(f"+ Merged: {campo} = {valores_finais}")
            else:
                # Campos únicos (ex: Data) - substituir sempre
                if valores_validos != valor_existente:
                    contexto[campo] = valores_validos
                    if valor_existente:
                        mudancas.append(f"* Atualizado: {campo} de '{valor_existente}' para '{valores_validos}'")
                    else:
                        mudancas.append(f"+ Adicionado: {campo} = {valores_validos}")

        return mudancas

    def _processar_periodo_estruturado(self, campos: Dict, contexto: Dict) -> List[str]:
        """
        Processa categoria período com tratamento especial para estruturas inicio/fim.

        Args:
            campos: Campos da categoria período do JSON
            contexto: Contexto sendo atualizado

        Returns:
            Lista de mudanças realizadas
        """
        mudancas = []

        # CASO 1: Estrutura inicio/fim (intervalo)
        if "inicio" in campos and "fim" in campos:
            inicio = campos["inicio"]
            fim = campos["fim"]

            if inicio and fim and isinstance(inicio, dict) and isinstance(fim, dict):
                # Converter estrutura para Data_>= e Data_<
                try:
                    from src.text_normalizer import normalizer

                    # Construir dados estruturados temporários para conversão
                    structured_data = {
                        "periodo": {
                            "inicio": inicio,
                            "fim": fim
                        }
                    }

                    # Converter para ranges
                    ranges = normalizer.convert_structured_to_ranges(structured_data)

                    if ranges and "Data_>=" in ranges and "Data_<" in ranges:
                        # Limpar filtros de data existentes
                        for key in list(contexto.keys()):
                            if key.startswith("Data"):
                                del contexto[key]

                        # Adicionar novos ranges
                        contexto["Data_>="] = ranges["Data_>="]
                        contexto["Data_<"] = ranges["Data_<"]

                        mudancas.append(f"+ Interval: {inicio['mes']}/{inicio['ano']} até {fim['mes']}/{fim['ano']} → {ranges['Data_>=']} até {ranges['Data_<']}")
                    else:
                        mudancas.append("❌ Falha ao converter interval para ranges de data")

                except Exception as e:
                    mudancas.append(f"❌ Erro ao processar interval: {str(e)}")

        # CASO 2: Mês/ano único
        elif "mes" in campos and "ano" in campos:
            mes = campos["mes"]
            ano = campos["ano"]

            if mes and ano:
                try:
                    from src.text_normalizer import normalizer

                    # Construir dados estruturados temporários
                    structured_data = {
                        "periodo": {
                            "mes": mes,
                            "ano": ano
                        }
                    }

                    # Converter para ranges
                    ranges = normalizer.convert_structured_to_ranges(structured_data)

                    if ranges and "Data_>=" in ranges and "Data_<" in ranges:
                        # Limpar filtros de data existentes
                        for key in list(contexto.keys()):
                            if key.startswith("Data"):
                                del contexto[key]

                        # Adicionar novos ranges
                        contexto["Data_>="] = ranges["Data_>="]
                        contexto["Data_<"] = ranges["Data_<"]

                        mudancas.append(f"+ Mês único: {mes}/{ano} → {ranges['Data_>=']} até {ranges['Data_<']}")
                    else:
                        mudancas.append("❌ Falha ao converter mês/ano para ranges de data")

                except Exception as e:
                    mudancas.append(f"❌ Erro ao processar mês/ano: {str(e)}")

        # CASO 3: Data ranges diretos (compatibilidade)
        elif "Data_>=" in campos or "Data_<" in campos:
            for campo in ["Data_>=", "Data_<"]:
                if campo in campos and campos[campo]:
                    contexto[campo] = campos[campo]
                    mudancas.append(f"+ Data range: {campo} = {campos[campo]}")

        # CASO 4: Data simples (compatibilidade)
        elif "Data" in campos and campos["Data"]:
            contexto["Data"] = campos["Data"]
            mudancas.append(f"+ Data: {campos['Data']}")

        return mudancas

    def _campo_permite_multiplos_valores(self, campo: str) -> bool:
        """
        Determina se um campo pode ter múltiplos valores (lista)

        Args:
            campo: Nome do campo

        Returns:
            bool: True se permite lista de valores
        """
        # Campos que tradicionalmente aceitam listas
        campos_lista = [
            'UF_Cliente', 'Municipio_Cliente', 'Cod_Cliente', 'Cod_Segmento_Cliente',
            'Cod_Familia_Produto', 'Cod_Grupo_Produto', 'Cod_Linha_Produto', 'Des_Linha_Produto',
            'Cod_Vendedor', 'Cod_Regiao_Vendedor'
        ]

        # Campos temporais geralmente são únicos
        campos_unicos = ['Data', 'Data_>=', 'Data_<', 'periodo', 'mes', 'ano']

        if campo in campos_unicos:
            return False
        elif campo in campos_lista:
            return True
        else:
            # Default: permitir listas para campos não reconhecidos
            return True

    def _merge_lista_valores(self, valor_existente, novo_valor):
        """
        Merge inteligente de listas de valores

        Args:
            valor_existente: Valor atual (pode ser None, string, ou lista)
            novo_valor: Novo valor (pode ser string ou lista)

        Returns:
            Lista merged ou valor único
        """
        # Normalizar para listas
        existente_lista = []
        if valor_existente:
            if isinstance(valor_existente, list):
                existente_lista = valor_existente.copy()
            else:
                existente_lista = [valor_existente]

        novo_lista = []
        if isinstance(novo_valor, list):
            novo_lista = novo_valor.copy()
        else:
            novo_lista = [novo_valor]

        # Merge sem duplicatas, preservando ordem
        resultado = existente_lista.copy()
        for item in novo_lista:
            if item not in resultado:
                resultado.append(item)

        # Retornar lista ou valor único baseado no tamanho
        if len(resultado) == 1:
            return resultado[0]
        else:
            return resultado

    def _processar_categoria(self, categoria: str, campos: Dict, contexto: Dict) -> List[str]:
        """
        Processa filtros de uma categoria específica

        Args:
            categoria: Nome da categoria
            campos: Dicionário com campos e valores
            contexto: Contexto sendo atualizado

        Returns:
            Lista de mudanças realizadas
        """
        mudancas = []

        for campo, valor in campos.items():
            if valor is None:
                continue

            # Validar valores
            if isinstance(valor, list):
                valores_validos = self.validar_valores(campo, valor, categoria)
            else:
                valores_validos = self.validar_valores(campo, [valor], categoria)
                if valores_validos:
                    valores_validos = valores_validos[0]  # Único valor

            # Atualizar contexto se há valores válidos
            if valores_validos:
                contexto[campo] = valores_validos
                mudancas.append(f"+ Adicionado: {campo} = {valores_validos}")

        return mudancas

    def _processar_remocoes(self, remover_filtros: List[str], contexto: Dict) -> List[str]:
        """
        Processa remoções explícitas de filtros

        Args:
            remover_filtros: Lista de filtros para remover
            contexto: Contexto sendo atualizado

        Returns:
            Lista de mudanças realizadas
        """
        mudancas = []

        for filtro in remover_filtros:
            if filtro in contexto:
                valor_removido = contexto[filtro]
                del contexto[filtro]
                mudancas.append(f"- Removido: {filtro} = {valor_removido}")

        return mudancas

    def _limpar_todos_filtros(self):
        """Limpa todos os filtros persistentes"""
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


def detectar_filtros_na_pergunta(pergunta_usuario: str, df_dataset: pd.DataFrame) -> Dict:
    """
    Detecta filtros automaticamente na pergunta original do usuário
    VERSÃO COM PRESERVAÇÃO DE GRANULARIDADE TEMPORAL

    Args:
        pergunta_usuario: Pergunta original do usuário
        df_dataset: DataFrame para validação

    Returns:
        Dict com filtros detectados preservando granularidade de mês/ano
    """
    manager = get_json_filter_manager(df_dataset)

    # Usar extração de texto aprimorada com TextNormalizer
    filtros_detectados = manager._extrair_filtros_por_texto(pergunta_usuario)

    # DETECÇÃO ESPECÍFICA PARA PERGUNTA (complementar)
    pergunta_lower = pergunta_usuario.lower()

    # Detectar termos geográficos comuns nas perguntas - BUSCA PRECISA
    termos_geograficos = {
        'joinville': 'JOINVILLE',
        'florianopolis': 'FLORIANOPOLIS',
        'florianópolis': 'FLORIANOPOLIS',
        'sao paulo': 'SAO PAULO',
        'são paulo': 'SAO PAULO',
        'rio de janeiro': 'RIO DE JANEIRO',
        'porto alegre': 'PORTO ALEGRE',
        'curitiba': 'CURITIBA',
        'belo horizonte': 'BELO HORIZONTE',
        'brasilia': 'BRASILIA',
        'brasília': 'BRASILIA',
        'salvador': 'SALVADOR',
        'fortaleza': 'FORTALEZA'
    }

    for termo, cidade_oficial in termos_geograficos.items():
        # BUSCA PRECISA: palavra completa com boundary
        if (f" {termo} " in f" {pergunta_lower} " or
            pergunta_lower.startswith(f"{termo} ") or
            pergunta_lower.endswith(f" {termo}") or
            pergunta_lower == termo):

            if "Municipio_Cliente" in manager.valores_validos:
                if cidade_oficial in manager.valores_validos["Municipio_Cliente"]:
                    if not filtros_detectados["regiao"]["Municipio_Cliente"]:
                        filtros_detectados["regiao"]["Municipio_Cliente"] = [cidade_oficial]
                    break

    # Detectar ranking/top patterns que podem indicar filtros implícitos
    ranking_patterns = [
        r'top\s+(\d+)',
        r'principais?\s+(\d+)?',
        r'maiores?\s+(\d+)?',
        r'melhores?\s+(\d+)?'
    ]

    for pattern in ranking_patterns:
        if re.search(pattern, pergunta_lower):
            # Se é uma pergunta de ranking, pode ser que precise de contexto mais específico
            # Mas deixar vazio para permitir análise geral se não há outros filtros
            break

    return filtros_detectados


def processar_filtros_automaticos(response_text: str, contexto_atual: Dict, df_dataset: pd.DataFrame, pergunta_usuario: str = "") -> Tuple[Dict, List[str]]:
    """
    Função principal para processar filtros automáticos - VERSÃO REFATORADA COM EXTRAÇÃO SQL

    Esta função agora prioriza a extração de filtros das queries SQL executadas,
    usando o sistema baseado em texto como fallback para compatibilidade.

    Args:
        response_text: Texto da response do modelo
        contexto_atual: Contexto atual de filtros
        df_dataset: DataFrame com dados para validação
        pergunta_usuario: Pergunta original do usuário (usado como fallback)

    Returns:
        Tuple[Dict, List[str]]: (contexto_atualizado, lista_de_mudanças)
    """
    # NOTA: Esta função é mantida para compatibilidade, mas a lógica principal
    # foi movida para app.py onde temos acesso ao debug_info com queries SQL.
    # Aqui implementamos apenas o fallback baseado em texto.

    manager = get_json_filter_manager(df_dataset)
    mudancas = []

    # Implementar fallback baseado no sistema original
    contexto_atualizado = contexto_atual.copy()

    if pergunta_usuario.strip():
        filtros_pergunta = detectar_filtros_na_pergunta(pergunta_usuario, df_dataset)

        # VALIDAÇÃO CRÍTICA: Verificar se detectou intervalos temporais
        if filtros_pergunta and "periodo" in filtros_pergunta:
            periodo_info = filtros_pergunta["periodo"]
            # Se detectou interval com início e fim, priorizar sempre
            if "inicio" in periodo_info and "fim" in periodo_info:
                contexto_da_pergunta, mudancas_pergunta = manager._atualizar_filtros_do_json(filtros_pergunta, contexto_atual)
                mudancas.extend(["🎯 INTERVAL detectado na pergunta original:"] + mudancas_pergunta)
                contexto_atualizado = contexto_da_pergunta
            else:
                # Single month/year - ainda prioritário mas check response também
                contexto_da_pergunta, mudancas_pergunta = manager._atualizar_filtros_do_json(filtros_pergunta, contexto_atual)
                if mudancas_pergunta and any(change.startswith("+") or change.startswith("*") for change in mudancas_pergunta):
                    mudancas.extend(["🔍 Detectado na pergunta original:"] + mudancas_pergunta)
                    contexto_atualizado = contexto_da_pergunta
        else:
            # Sem período detectado na pergunta, processar outros filtros
            contexto_da_pergunta, mudancas_pergunta = manager._atualizar_filtros_do_json(filtros_pergunta, contexto_atual)
            if mudancas_pergunta and any(change.startswith("+") or change.startswith("*") for change in mudancas_pergunta):
                mudancas.extend(["🔍 Detectado na pergunta original:"] + mudancas_pergunta)
                contexto_atualizado = contexto_da_pergunta

    # ESTRATÉGIA 2: COMPLEMENTAR COM ANÁLISE DA RESPONSE (se necessário)
    # Apenas se não detectou intervalo temporal na pergunta
    pergunta_tem_intervalo = False
    if contexto_atualizado:
        # Check se já tem interval complete do processamento da pergunta
        for campo, valor in contexto_atualizado.items():
            if campo.startswith('Data_') and 'Data_>=' in contexto_atualizado and 'Data_<' in contexto_atualizado:
                pergunta_tem_intervalo = True
                break

    if not pergunta_tem_intervalo:
        contexto_response, mudancas_response = manager.processar_json_response(response_text, contexto_atualizado)

        if mudancas_response and any(change.startswith("+") or change.startswith("*") for change in mudancas_response):
            mudancas.extend(["📝 Detectado na response:"] + mudancas_response)
            contexto_atualizado = contexto_response

    # LOG: Estratégia utilizada
    if not mudancas:
        mudancas.append("ℹ️ Nenhum novo filtro detectado em pergunta ou response")
    else:
        # VALIDAÇÃO FINAL: Log do que foi detectado
        if 'Data_>=' in contexto_atualizado and 'Data_<' in contexto_atualizado:
            mudancas.append(f"✅ Interval final: {contexto_atualizado['Data_>=']} até {contexto_atualizado['Data_<']}")

    return contexto_atualizado, mudancas


def processar_filtros_com_sql_prioritario(sql_queries: List[str], response_text: str,
                                         contexto_atual: Dict, df_dataset: pd.DataFrame,
                                         pergunta_usuario: str = "") -> Tuple[Dict, List[str]]:
    """
    Nova função que prioriza extração de filtros das queries SQL

    Args:
        sql_queries: Lista de queries SQL executadas
        response_text: Texto da response do modelo (fallback)
        contexto_atual: Contexto atual de filtros
        df_dataset: DataFrame com dados para validação
        pergunta_usuario: Pergunta original do usuário (fallback)

    Returns:
        Tuple[Dict, List[str]]: (contexto_atualizado, lista_de_mudanças)
    """
    from .sql_filter_extractor import SQLFilterExtractor

    mudancas = []
    contexto_atualizado = contexto_atual.copy()

    if sql_queries:
        # ESTRATÉGIA PRIORITÁRIA: Extrair filtros das queries SQL
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
                    mudancas.append(f"🎯 **Filtros extraídos do SQL:** {len(added_keys)} filtros detectados")
                    for key in added_keys:
                        mudancas.append(f"  + {key} = {contexto_atualizado[key]}")

                return contexto_atualizado, mudancas

    # FALLBACK: Se não conseguiu extrair do SQL, usar sistema original
    return processar_filtros_automaticos(response_text, contexto_atual, df_dataset, pergunta_usuario)


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