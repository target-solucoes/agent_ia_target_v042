"""
Módulo de normalização de texto para garantir consistência em consultas e dados.
Aplica transformações padrão para resolver problemas de capitalização e formatação.
"""

import re
import unicodedata
import pandas as pd
from typing import Union, List, Dict, Any, Tuple, Optional
import yaml
import calendar
from datetime import datetime, timedelta

class TextNormalizer:
    """Classe para normalização consistente de texto em datasets e consultas."""
    
    def __init__(self):
        """Inicializa o normalizador com configurações padrão."""
        self.text_columns_cache = {}
        self.dataset_context = None

    def set_dataset_context(self, df):
        """
        Configura o contexto do dataset para interpretação de consultas temporais relativas.

        Args:
            df: DataFrame com coluna 'Data' para extrair contexto temporal
        """
        if df is not None and 'Data' in df.columns:
            max_date = df['Data'].max()
            min_date = df['Data'].min()

            # Imports para cálculos de contexto
            from dateutil.relativedelta import relativedelta

            self.dataset_context = {
                'last_month': max_date.strftime('%Y-%m'),
                'max_date': max_date,
                'min_date': min_date,
                'max_date_str': max_date.strftime('%Y-%m-%d'),
                'min_date_str': min_date.strftime('%Y-%m-%d'),
                'total_months': (max_date.year - min_date.year) * 12 + (max_date.month - min_date.month) + 1,

                # Pré-computar exemplos comuns de períodos relativos
                'temporal_examples': {
                    'last_3_months_start': (max_date.replace(day=1) - relativedelta(months=2)).strftime('%Y-%m-%d'),
                    'last_3_months_end': (max_date.replace(day=1) + relativedelta(months=1)).strftime('%Y-%m-%d'),
                    'last_6_months_start': (max_date.replace(day=1) - relativedelta(months=5)).strftime('%Y-%m-%d'),
                    'last_6_months_end': (max_date.replace(day=1) + relativedelta(months=1)).strftime('%Y-%m-%d'),
                    'last_12_months_start': (max_date.replace(day=1) - relativedelta(months=11)).strftime('%Y-%m-%d'),
                    'last_12_months_end': (max_date.replace(day=1) + relativedelta(months=1)).strftime('%Y-%m-%d'),
                }
            }

    def generate_temporal_context_reminder(self, query_type='general'):
        """
        Gera um lembrete de contexto temporal específico para reforçar interpretação correta.

        Args:
            query_type: Tipo da consulta ('general', 'last_month', 'relative_period')

        Returns:
            String com contexto temporal reforçado
        """
        if not self.dataset_context:
            return ""

        base_context = f"""
LEMBRETE CRÍTICO DE CONTEXTO TEMPORAL:
- Data de referência "HOJE": {self.dataset_context['max_date_str']} (última data do dataset)
- "Último mês" sempre significa: {self.dataset_context['last_month']}
- Período do dataset: {self.dataset_context['min_date_str']} até {self.dataset_context['max_date_str']}
"""

        if query_type == 'last_month':
            return base_context + f"""
- Para "último mês": use mês {self.dataset_context['last_month']}
- NÃO use mês atual do sistema
- NÃO use CURRENT_DATE ou NOW()
"""

        elif query_type == 'relative_period':
            examples = self.dataset_context['temporal_examples']
            return base_context + f"""
EXEMPLOS PRÉ-COMPUTADOS:
- "últimos 3 meses": {examples['last_3_months_start']} até {examples['last_3_months_end']}
- "últimos 6 meses": {examples['last_6_months_start']} até {examples['last_6_months_end']}
- "últimos 12 meses": {examples['last_12_months_start']} até {examples['last_12_months_end']}
- SEMPRE calcule a partir de {self.dataset_context['max_date_str']}
"""

        return base_context
    
    def normalize_text(self, text: Union[str, None]) -> str:
        """
        Normaliza uma string individual aplicando transformações consistentes.
        
        Args:
            text: String a ser normalizada
            
        Returns:
            String normalizada ou string vazia se entrada for None/NaN
        """
        if pd.isna(text) or text is None:
            return ""
        
        # Converter para string se não for
        text = str(text)
        
        # Remover espaços extras no início e fim
        text = text.strip()
        
        # Normalizar caracteres Unicode (remover acentos)
        text = unicodedata.normalize('NFD', text)
        text = ''.join(char for char in text if unicodedata.category(char) != 'Mn')
        
        # Converter para minúsculas
        text = text.lower()
        
        # Normalizar espaços múltiplos
        text = re.sub(r'\s+', ' ', text)
        
        return text
    
    def normalize_column(self, series: pd.Series) -> pd.Series:
        """
        Normaliza uma coluna inteira do pandas DataFrame.
        
        Args:
            series: Serie do pandas a ser normalizada
            
        Returns:
            Serie normalizada
        """
        return series.apply(self.normalize_text)
    
    def identify_text_columns(self, df: pd.DataFrame) -> List[str]:
        """
        Identifica colunas que contêm texto e podem se beneficiar da normalização.
        
        Args:
            df: DataFrame para análise
            
        Returns:
            Lista de nomes de colunas que contêm texto
        """
        text_columns = []
        
        for col in df.columns:
            # Verificar se é coluna de tipo object ou category
            if df[col].dtype in ['object', 'category']:
                # Verificar se contém strings (não apenas números)
                sample_values = df[col].dropna().head(100)
                if len(sample_values) > 0:
                    # Verificar se pelo menos alguns valores são strings não-numéricas
                    string_count = sum(
                        1 for val in sample_values 
                        if isinstance(val, str) and not val.strip().replace('.', '').replace(',', '').isdigit()
                    )
                    if string_count > len(sample_values) * 0.1:  # 10% threshold
                        text_columns.append(col)
        
        return text_columns
    
    def normalize_dataframe(self, df: pd.DataFrame, specific_columns: List[str] = None) -> pd.DataFrame:
        """
        Normaliza todas as colunas de texto de um DataFrame.
        
        Args:
            df: DataFrame a ser normalizado
            specific_columns: Lista específica de colunas para normalizar (opcional)
            
        Returns:
            DataFrame com colunas de texto normalizadas
        """
        df_normalized = df.copy()
        
        # Determinar quais colunas normalizar
        if specific_columns is not None:
            columns_to_normalize = specific_columns
        else:
            columns_to_normalize = self.identify_text_columns(df)
        
        # Aplicar normalização às colunas identificadas
        for col in columns_to_normalize:
            if col in df_normalized.columns:
                df_normalized[col] = self.normalize_column(df_normalized[col])
        
        return df_normalized
    
    def normalize_query_terms(self, query: str, alias_mapping: Dict[str, List[str]] = None) -> Dict[str, Any]:
        """
        Normaliza termos de uma consulta do usuário e mapeia aliases.
        
        Args:
            query: Consulta do usuário
            alias_mapping: Dicionário de mapeamento de aliases (opcional)
            
        Returns:
            Dicionário com query normalizada e termos mapeados
        """
        normalized_query = self.normalize_text(query)
        
        result = {
            'original_query': query,
            'normalized_query': normalized_query,
            'mapped_terms': {}
        }
        
        # Se houver mapeamento de aliases, aplicar
        if alias_mapping:
            for column, aliases in alias_mapping.items():
                normalized_aliases = [self.normalize_text(alias) for alias in aliases]
                
                # Verificar se algum alias aparece na query normalizada
                for i, alias in enumerate(normalized_aliases):
                    if alias in normalized_query and alias.strip():
                        result['mapped_terms'][alias] = {
                            'original_alias': aliases[i],
                            'mapped_column': column
                        }
        
        return result
    
    def create_search_index(self, df: pd.DataFrame, text_columns: List[str] = None) -> Dict[str, Dict[str, List[int]]]:
        """
        Cria um índice de busca para facilitar consultas rápidas.
        
        Args:
            df: DataFrame para indexar
            text_columns: Colunas específicas para indexar (opcional)
            
        Returns:
            Dicionário com índice de busca por coluna e termo
        """
        if text_columns is None:
            text_columns = self.identify_text_columns(df)
        
        search_index = {}
        
        for col in text_columns:
            if col in df.columns:
                search_index[col] = {}
                
                for idx, value in df[col].items():
                    normalized_value = self.normalize_text(value)
                    if normalized_value:
                        if normalized_value not in search_index[col]:
                            search_index[col][normalized_value] = []
                        search_index[col][normalized_value].append(idx)
        
        return search_index
    
    def parse_temporal_entities(self, text: str) -> Dict[str, Any]:
        """
        Extrai e converte entidades temporais de texto natural para formatos estruturados.
        
        Exemplos:
        - "julho de 2015" → {"Data_>=": "2015-07-01", "Data_<": "2015-08-01"}
        - "janeiro 2020" → {"Data_>=": "2020-01-01", "Data_<": "2020-02-01"}
        - "dezembro de 2023" → {"Data_>=": "2023-12-01", "Data_<": "2024-01-01"}
        
        Args:
            text: Texto contendo possíveis referências temporais
            
        Returns:
            Dicionário com entidades temporais estruturadas
        """
        text_lower = text.lower().strip()
        
        # Mapeamento de meses em português
        month_mapping = {
            'janeiro': 1, 'jan': 1,
            'fevereiro': 2, 'fev': 2,
            'março': 3, 'mar': 3, 'marco': 3,
            'abril': 4, 'abr': 4,
            'maio': 5, 'mai': 5,
            'junho': 6, 'jun': 6,
            'julho': 7, 'jul': 7,
            'agosto': 8, 'ago': 8,
            'setembro': 9, 'set': 9, 'sep': 9,
            'outubro': 10, 'out': 10, 'oct': 10,
            'novembro': 11, 'nov': 11,
            'dezembro': 12, 'dez': 12, 'dec': 12
        }
        
        temporal_entities = {}
        
        # Padrão principal: "mês de ano" ou "mês ano" ou "mês/ano"
        month_year_patterns = [
            r'\b(\w+)\s+de\s+(\d{4})\b',  # "julho de 2015"
            r'\b(\w+)\s+(\d{4})\b',       # "julho 2015"
            r'\b(\w+)/(\d{4})\b',         # "julho/2015"
            r'\bem\s+(\w+)\s+de\s+(\d{4})\b',  # "em julho de 2015"
            r'\bno\s+(\w+)\s+de\s+(\d{4})\b',  # "no julho de 2015"
            r'\bdurante\s+(\w+)\s+de\s+(\d{4})\b',  # "durante julho de 2015"
        ]
        
        for pattern in month_year_patterns:
            matches = re.finditer(pattern, text_lower)
            for match in matches:
                month_text = match.group(1).strip()
                year_text = match.group(2).strip()
                
                # Verificar se o mês é reconhecido
                if month_text in month_mapping:
                    month_num = month_mapping[month_text]
                    year_num = int(year_text)
                    
                    # Calcular primeiro e último dia do mês
                    start_date = f"{year_num:04d}-{month_num:02d}-01"
                    
                    # Calcular primeiro dia do mês seguinte
                    if month_num == 12:
                        next_month = 1
                        next_year = year_num + 1
                    else:
                        next_month = month_num + 1
                        next_year = year_num
                    
                    end_date = f"{next_year:04d}-{next_month:02d}-01"
                    
                    temporal_entities['Data_>='] = start_date
                    temporal_entities['Data_<'] = end_date
                    
                    # Adicionar metadados para debugging
                    temporal_entities['_temporal_metadata'] = {
                        'original_text': match.group(0),
                        'parsed_month': month_text,
                        'parsed_year': year_text,
                        'month_number': month_num,
                        'year_number': year_num
                    }
                    break  # Usar apenas a primeira ocorrência válida
        
        # CRÍTICO: Primeiro verificar padrões de INTERVALO antes de single months
        # Isso evita que single month patterns sobrescrevam intervals
        between_patterns = [
            r'\bentre\s+(\w+)\s+e\s+(\w+)\s+de\s+(\d{4})\b',  # "entre junho e julho de 2015"
            r'\bentre\s+(\w+)/(\d{4})\s+e\s+(\w+)/(\d{4})\b',  # "entre junho/2015 e julho/2015"
            r'\bentre\s+os\s+per[íi]odos?\s+de\s+(\w+)\s+e\s+(\w+)\s+de\s+(\d{4})\b',  # "entre os períodos de junho e julho de 2015"
            r'\bentre\s+os\s+meses\s+de\s+(\w+)\s+e\s+(\w+)\s+de\s+(\d{4})\b',  # "entre os meses de junho e julho de 2015"
            r'\bdo\s+per[íi]odo\s+de\s+(\w+)\s+a\s+(\w+)\s+de\s+(\d{4})\b',  # "do período de junho a julho de 2015"
            r'\bde\s+(\w+)\s+a\s+(\w+)\s+de\s+(\d{4})\b',  # "de junho a julho de 2015"
            r'\bper[íi]odos?\s+de\s+(\w+)/(\d{4})\s+a\s+(\w+)/(\d{4})\b',  # "período de fevereiro/2015 a julho/2015"
            r'\bentre\s+os\s+per[íi]odos?\s+de\s+(\w+)/(\d{4})\s+e\s+(\w+)/(\d{4})\b',  # "entre os períodos de fev/2015 e jul/2015"
        ]

        between_match = None
        matched_pattern_index = -1
        for i, between_pattern in enumerate(between_patterns):
            between_match = re.search(between_pattern, text_lower)
            if between_match:
                matched_pattern_index = i
                break

        if between_match:
            # Processar diferentes padrões com estruturas de grupos específicas
            try:
                start_month_text = None
                end_month_text = None
                year_text = None

                # Padrões com formato "mês e mês de ano" (índices 0, 2, 3, 4, 5)
                if matched_pattern_index in [0, 2, 3, 4, 5]:
                    start_month_text = between_match.group(1).strip()
                    end_month_text = between_match.group(2).strip()
                    year_text = between_match.group(3).strip()

                # Padrão com formato "mês/ano e mês/ano" (índice 1)
                elif matched_pattern_index == 1:
                    start_month_text = between_match.group(1).strip()
                    year_text = between_match.group(2).strip()  # Usar primeiro ano
                    end_month_text = between_match.group(3).strip()
                    # Segundo ano em between_match.group(4) - verificar se são iguais
                    second_year = between_match.group(4).strip()
                    if year_text != second_year:
                        # Se anos diferentes, usar range completo
                        year_text = year_text  # Manter primeiro ano para início

                # Padrão "período de mês/ano a mês/ano" (índice 6)
                elif matched_pattern_index == 6:
                    start_month_text = between_match.group(1).strip()
                    year_text = between_match.group(2).strip()  # Usar primeiro ano
                    end_month_text = between_match.group(3).strip()

                # Padrão "entre os períodos de mês/ano e mês/ano" (índice 7)
                elif matched_pattern_index == 7:
                    start_month_text = between_match.group(1).strip()
                    year_text = between_match.group(2).strip()  # Usar primeiro ano
                    end_month_text = between_match.group(3).strip()

                # Validar se os meses foram encontrados corretamente
                if start_month_text and end_month_text and year_text:
                    if start_month_text in month_mapping and end_month_text in month_mapping:
                        start_month_num = month_mapping[start_month_text]
                        end_month_num = month_mapping[end_month_text]
                        year_num = int(year_text)

                        # Data de início: primeiro dia do primeiro mês
                        start_date = f"{year_num:04d}-{start_month_num:02d}-01"

                        # Data de fim: primeiro dia do mês após o último mês
                        if end_month_num == 12:
                            next_month = 1
                            next_year = year_num + 1
                        else:
                            next_month = end_month_num + 1
                            next_year = year_num

                        end_date = f"{next_year:04d}-{next_month:02d}-01"

                        temporal_entities['Data_>='] = start_date
                        temporal_entities['Data_<'] = end_date

                        temporal_entities['_temporal_metadata'] = {
                            'original_text': between_match.group(0),
                            'type': 'period_between_months',
                            'start_month': start_month_text,
                            'end_month': end_month_text,
                            'parsed_year': year_text,
                            'pattern_index': matched_pattern_index,
                            'start_month_num': start_month_num,
                            'end_month_num': end_month_num
                        }

            except (IndexError, ValueError) as e:
                # Em caso de erro no parsing, continuar sem definir entidades temporais
                pass
        
        # Padrão para anos individuais: "em 2015", "no ano de 2015", "no período de 2015"
        year_patterns = [
            r'\bem\s+(\d{4})\b',
            r'\bno\s+ano\s+de\s+(\d{4})\b',
            r'\bdurante\s+(\d{4})\b',
            r'\bno\s+per[ií]odo\s+de\s+(\d{4})\b',
            r'\bper[ií]odo\s+de\s+(\d{4})\b',
        ]
        
        if not temporal_entities:  # Só aplicar se não encontrou padrão mês/ano
            for pattern in year_patterns:
                year_match = re.search(pattern, text_lower)
                if year_match:
                    year_num = int(year_match.group(1))
                    
                    temporal_entities['Data_>='] = f"{year_num:04d}-01-01"
                    temporal_entities['Data_<'] = f"{year_num + 1:04d}-01-01"
                    
                    temporal_entities['_temporal_metadata'] = {
                        'original_text': year_match.group(0),
                        'type': 'full_year',
                        'parsed_year': year_num
                    }
                    break
        
        # Verificar se há referência ao "último mês" usando o contexto do dataset
        if not temporal_entities and self.dataset_context:
            last_month_reference = self._detect_last_month_reference(text_lower)
            if last_month_reference:
                temporal_entities.update(last_month_reference)

        # Verificar se há referência a períodos relativos múltiplos (últimos X meses/dias/anos)
        if not temporal_entities and self.dataset_context:
            relative_period_reference = self._detect_relative_period_reference(text_lower)
            if relative_period_reference:
                temporal_entities.update(relative_period_reference)

        return temporal_entities

    def _detect_last_month_reference(self, text: str) -> Dict[str, Any]:
        """
        Detecta inteligentemente referências ao último mês baseado no contexto do dataset.

        Args:
            text: Texto da consulta em lowercase

        Returns:
            Dicionário com entidades temporais ou vazio se não detectado
        """
        if not self.dataset_context:
            return {}

        # Usar raciocínio mais flexível para detectar menções ao último mês
        last_month_indicators = [
            'último mês', 'ultimo mês', 'último mes', 'ultimo mes',
            'mês anterior', 'mes anterior', 'mês passado', 'mes passado',
            'último período', 'ultimo período', 'ultimo periodo',
            'mês mais recente', 'mes mais recente', 'período mais recente',
            'último mês completo', 'ultimo mês completo',
            'dados mais recentes do mês', 'dados mais recentes do mes',
            'análise do período mais recente', 'analise do período mais recente',
            'analise do periodo mais recente'
        ]

        # Verificar se há indicação clara de último mês
        has_last_month_reference = any(indicator in text for indicator in last_month_indicators)

        if not has_last_month_reference:
            # Verificar contextos mais sutis que podem indicar último mês
            subtle_indicators = [
                ('último', 'mês'), ('ultimo', 'mês'), ('último', 'mes'), ('ultimo', 'mes'),
                ('anterior', 'mês'), ('anterior', 'mes'), ('passado', 'mês'), ('passado', 'mes'),
                ('recente', 'mês'), ('recente', 'mes'), ('mais', 'recente'),
                ('período', 'recente'), ('periodo', 'recente'), ('dados', 'recentes')
            ]

            for word1, word2 in subtle_indicators:
                if word1 in text and word2 in text:
                    # Verificar se estão próximos (dentro de 15 palavras)
                    words = text.split()
                    if word1 in words and word2 in words:
                        idx1 = words.index(word1)
                        idx2 = words.index(word2)
                        if abs(idx1 - idx2) <= 15:
                            # Verificar se também há menção a "mês" no contexto próximo
                            context_words = words[max(0, min(idx1, idx2) - 5):min(len(words), max(idx1, idx2) + 6)]
                            if any(mes_word in context_words for mes_word in ['mês', 'mes', 'período', 'periodo']):
                                has_last_month_reference = True
                                break

        if has_last_month_reference:
            # Extrair o último mês do contexto do dataset
            last_month_str = self.dataset_context['last_month']  # formato: 'YYYY-MM'
            year, month = map(int, last_month_str.split('-'))

            # Calcular primeiro e último dia do último mês
            start_date = f"{year:04d}-{month:02d}-01"

            # Calcular primeiro dia do mês seguinte
            if month == 12:
                next_month = 1
                next_year = year + 1
            else:
                next_month = month + 1
                next_year = year

            end_date = f"{next_year:04d}-{next_month:02d}-01"

            return {
                'Data_>=': start_date,
                'Data_<': end_date,
                '_temporal_metadata': {
                    'original_text': text,
                    'type': 'intelligent_last_month_detection',
                    'dataset_last_month': last_month_str,
                    'computed_month': month,
                    'computed_year': year
                }
            }

        return {}

    def _detect_relative_period_reference(self, text: str) -> Dict[str, Any]:
        """
        Detecta inteligentemente referências a períodos relativos múltiplos baseado no contexto do dataset.
        Ex: "últimos 3 meses", "últimos 6 meses", "últimos 2 anos"

        Args:
            text: Texto da consulta em lowercase

        Returns:
            Dicionário com entidades temporais ou vazio se não detectado
        """
        if not self.dataset_context:
            return {}

        import re
        from datetime import datetime
        from dateutil.relativedelta import relativedelta

        # Padrões para detectar períodos relativos múltiplos
        relative_patterns = [
            r'\b(?:últimos|ultimos)\s+(\d+)\s+(?:meses|mês|mes)\b',
            r'\b(?:últimos|ultimos)\s+(\d+)\s+(?:anos|ano)\b',
            r'\b(?:últimos|ultimos)\s+(\d+)\s+(?:dias|dia)\b',
            r'\b(?:últimos|ultimos)\s+(\d+)\s+(?:trimestres|trimestre)\b',
            r'\b(?:últimos|ultimos)\s+(\d+)\s+(?:semestres|semestre)\b',
            r'\b(?:nos\s+)?(?:últimos|ultimos)\s+(\d+)\s+(?:meses|mês|mes)\b',
            r'\b(?:nos\s+)?(?:últimos|ultimos)\s+(\d+)\s+(?:anos|ano)\b',
            r'\b(?:durante\s+os\s+)?(?:últimos|ultimos)\s+(\d+)\s+(?:meses|mês|mes)\b'
        ]

        for pattern in relative_patterns:
            match = re.search(pattern, text)
            if match:
                period_count = int(match.group(1))
                period_text = match.group(0)

                # Determinar o tipo de período
                if any(word in period_text for word in ['mês', 'mes', 'meses']):
                    period_type = 'months'
                elif any(word in period_text for word in ['ano', 'anos']):
                    period_type = 'years'
                elif any(word in period_text for word in ['dia', 'dias']):
                    period_type = 'days'
                elif any(word in period_text for word in ['trimestre', 'trimestres']):
                    period_type = 'quarters'
                    period_count = period_count * 3  # Converter para meses
                elif any(word in period_text for word in ['semestre', 'semestres']):
                    period_type = 'semesters'
                    period_count = period_count * 6  # Converter para meses
                else:
                    continue

                # Usar a data máxima do dataset como referência (não a data atual)
                max_date = self.dataset_context['max_date']

                # Para períodos em meses, calcular a partir do início do mês da data máxima
                if period_type in ['months', 'quarters', 'semesters']:
                    # Primeiro, ir para o início do mês da data máxima
                    month_start = max_date.replace(day=1)
                    # Depois, voltar o número de meses especificado
                    start_date = month_start - relativedelta(months=period_count)
                    # Data de fim é o primeiro dia do mês seguinte ao mês da data máxima
                    end_date = month_start + relativedelta(months=1)
                elif period_type == 'years':
                    # Para anos, calcular a partir do início do ano
                    year_start = max_date.replace(month=1, day=1)
                    start_date = year_start - relativedelta(years=period_count)
                    end_date = max_date + relativedelta(days=1)
                elif period_type == 'days':
                    # Para dias, calcular diretamente
                    start_date = max_date - relativedelta(days=period_count - 1)  # -1 para incluir o dia atual
                    end_date = max_date + relativedelta(days=1)

                return {
                    'Data_>=': start_date.strftime('%Y-%m-%d'),
                    'Data_<': end_date.strftime('%Y-%m-%d'),
                    '_temporal_metadata': {
                        'original_text': period_text,
                        'type': 'intelligent_relative_period_detection',
                        'period_count': period_count,
                        'period_type': period_type,
                        'dataset_max_date': max_date.strftime('%Y-%m-%d'),
                        'computed_start_date': start_date.strftime('%Y-%m-%d')
                    }
                }

        return {}
    
    def format_temporal_filter(self, temporal_data: Dict[str, Any]) -> str:
        """
        Converte entidades temporais em filtro SQL adequado.
        
        Args:
            temporal_data: Dados temporais extraídos de parse_temporal_entities()
            
        Returns:
            String com filtro SQL para cláusula WHERE
        """
        if not temporal_data or ('Data_>=' not in temporal_data or 'Data_<' not in temporal_data):
            return ""
        
        start_date = temporal_data['Data_>=']
        end_date = temporal_data['Data_<']
        
        # Retornar filtro SQL
        return f"Data >= '{start_date}' AND Data < '{end_date}'"
    
    def extract_and_format_temporal(self, text: str) -> Optional[Tuple[Dict[str, str], str]]:
        """
        Método conveniente que extrai entidades temporais e retorna tanto o contexto
        estruturado quanto o filtro SQL formatado.
        
        Args:
            text: Texto para processamento
            
        Returns:
            Tupla (contexto_estruturado, filtro_sql) ou None se nenhuma entidade encontrada
        """
        temporal_entities = self.parse_temporal_entities(text)
        
        if not temporal_entities:
            return None
        
        # Criar contexto estruturado (incluindo metadados para debugging)
        context = temporal_entities.copy()
        
        # Criar filtro SQL
        sql_filter = self.format_temporal_filter(temporal_entities)
        
        return context, sql_filter

    def get_structured_temporal_data(self, text: str) -> Dict[str, Any]:
        """
        Extrai dados temporais em formato estruturado preservando granularidade.

        Exemplos de retorno:
        - "julho de 2015" → {"periodo": {"mes": "07", "ano": "2015"}}
        - "junho/2016" → {"periodo": {"mes": "06", "ano": "2016"}}
        - "entre junho/2015 e julho/2015" → {"periodo": {"inicio": {"mes": "06", "ano": "2015"}, "fim": {"mes": "07", "ano": "2015"}}}
        - "em 2016" → {"periodo": {"ano": "2016"}}

        Args:
            text: Texto contendo referências temporais

        Returns:
            Dicionário com estrutura temporal preservando granularidade
        """
        # Primeiro, usar parsing temporal existente
        temporal_entities = self.parse_temporal_entities(text)

        if not temporal_entities:
            return {}

        # Extrair metadados para determinar granularidade
        metadata = temporal_entities.get('_temporal_metadata', {})

        # Estrutura de retorno
        resultado = {"periodo": {}}

        # Caso 1: Mês/ano específico
        if metadata.get('parsed_month') and metadata.get('parsed_year'):
            month_num = metadata.get('month_number')
            year_num = metadata.get('year_number')

            resultado["periodo"] = {
                "mes": f"{month_num:02d}",
                "ano": str(year_num)
            }

        # Caso 2: Intervalo entre meses
        elif metadata.get('type') == 'period_between_months':
            start_month_text = metadata.get('start_month')
            end_month_text = metadata.get('end_month')
            year_text = metadata.get('parsed_year')

            # Mapear nomes de meses para números
            month_mapping = {
                'janeiro': 1, 'jan': 1, 'fevereiro': 2, 'fev': 2,
                'março': 3, 'mar': 3, 'marco': 3, 'abril': 4, 'abr': 4,
                'maio': 5, 'mai': 5, 'junho': 6, 'jun': 6,
                'julho': 7, 'jul': 7, 'agosto': 8, 'ago': 8,
                'setembro': 9, 'set': 9, 'sep': 9, 'outubro': 10, 'out': 10, 'oct': 10,
                'novembro': 11, 'nov': 11, 'dezembro': 12, 'dez': 12, 'dec': 12
            }

            # CORREÇÃO: Usar números já calculados no parsing se disponíveis
            if 'start_month_num' in metadata and 'end_month_num' in metadata:
                start_month_num = metadata['start_month_num']
                end_month_num = metadata['end_month_num']
            else:
                # Fallback para lookup manual
                start_month_num = month_mapping.get(start_month_text.lower(), 1)
                end_month_num = month_mapping.get(end_month_text.lower(), 12)

            resultado["periodo"] = {
                "inicio": {
                    "mes": f"{start_month_num:02d}",
                    "ano": str(year_text)
                },
                "fim": {
                    "mes": f"{end_month_num:02d}",
                    "ano": str(year_text)
                }
            }

            # ADIÇÃO: Incluir metadados para debug
            resultado["_debug_interval"] = {
                "original_text": metadata.get('original_text', ''),
                "start_month_name": start_month_text,
                "end_month_name": end_month_text,
                "pattern_used": metadata.get('pattern_index', -1)
            }

        # Caso 3: Ano completo
        elif metadata.get('type') == 'full_year':
            year_num = metadata.get('parsed_year')
            resultado["periodo"] = {"ano": str(year_num)}

        # Caso 4: Períodos relativos
        elif metadata.get('type') in ['intelligent_last_month_detection', 'intelligent_relative_period_detection']:
            # Para períodos relativos, manter ranges de data originais
            if 'Data_>=' in temporal_entities and 'Data_<' in temporal_entities:
                resultado["periodo"] = {
                    "Data_>=": temporal_entities['Data_>='],
                    "Data_<": temporal_entities['Data_<']
                }

        # Fallback: Se não conseguiu estruturar, manter ranges originais
        if not resultado["periodo"] and 'Data_>=' in temporal_entities and 'Data_<' in temporal_entities:
            resultado["periodo"] = {
                "Data_>=": temporal_entities['Data_>='],
                "Data_<": temporal_entities['Data_<']
            }

        return resultado

    def convert_structured_to_ranges(self, structured_data: Dict[str, Any]) -> Dict[str, str]:
        """
        Converte dados estruturados de volta para ranges de data para compatibilidade.

        Args:
            structured_data: Dados estruturados do get_structured_temporal_data()

        Returns:
            Dicionário com Data_>= e Data_< para uso em filtros SQL
        """
        if not structured_data or "periodo" not in structured_data:
            return {}

        periodo = structured_data["periodo"]
        resultado = {}

        # Caso 1: Mês/ano específico
        if "mes" in periodo and "ano" in periodo:
            mes_num = int(periodo["mes"])
            ano_num = int(periodo["ano"])

            start_date = f"{ano_num:04d}-{mes_num:02d}-01"

            # Calcular primeiro dia do mês seguinte
            if mes_num == 12:
                next_month = 1
                next_year = ano_num + 1
            else:
                next_month = mes_num + 1
                next_year = ano_num

            end_date = f"{next_year:04d}-{next_month:02d}-01"

            resultado = {"Data_>=": start_date, "Data_<": end_date}

        # Caso 2: Intervalo entre meses
        elif "inicio" in periodo and "fim" in periodo:
            inicio = periodo["inicio"]
            fim = periodo["fim"]

            start_mes = int(inicio["mes"])
            start_ano = int(inicio["ano"])
            end_mes = int(fim["mes"])
            end_ano = int(fim["ano"])

            start_date = f"{start_ano:04d}-{start_mes:02d}-01"

            # Calcular primeiro dia do mês após o fim
            if end_mes == 12:
                next_month = 1
                next_year = end_ano + 1
            else:
                next_month = end_mes + 1
                next_year = end_ano

            end_date = f"{next_year:04d}-{next_month:02d}-01"

            resultado = {"Data_>=": start_date, "Data_<": end_date}

        # Caso 3: Ano completo
        elif "ano" in periodo and "mes" not in periodo:
            ano_num = int(periodo["ano"])
            resultado = {
                "Data_>=": f"{ano_num:04d}-01-01",
                "Data_<": f"{ano_num + 1:04d}-01-01"
            }

        # Caso 4: Já são ranges
        elif "Data_>=" in periodo and "Data_<" in periodo:
            resultado = {
                "Data_>=": periodo["Data_>="],
                "Data_<": periodo["Data_<"]
            }

        return resultado


def load_alias_mapping(alias_file_path: str = None) -> Dict[str, List[str]]:
    """
    Carrega mapeamento de aliases de um arquivo YAML.

    Args:
        alias_file_path: Caminho para arquivo de aliases

    Returns:
        Dicionário com mapeamento de aliases
    """
    if alias_file_path is None:
        alias_file_path = "data/mappings/alias.yaml"

    try:
        with open(alias_file_path, 'r', encoding='utf-8') as f:
            alias_data = yaml.safe_load(f)

        # Extrair apenas o mapeamento de colunas
        return alias_data.get('columns', {})

    except FileNotFoundError:
        print(f"Warning: Alias file not found at {alias_file_path}")
        return {}
    except yaml.YAMLError:
        print(f"Warning: Invalid YAML in alias file {alias_file_path}")
        return {}


# Instância global para uso conveniente
normalizer = TextNormalizer()