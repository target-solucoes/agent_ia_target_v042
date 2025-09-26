"""
Ferramenta Python Otimizada para controle de execução redundante e cache de variáveis
"""

from agno.tools.python import PythonTools
import re


class OptimizedPythonTools(PythonTools):
    """
    Classe customizada de PythonTools que controla execuções redundantes e
    mantém cache de variáveis importantes
    """

    def __init__(self, debug_info_ref=None, *args, **kwargs):
        # Remove argumentos incompatíveis com a nova versão
        kwargs.pop('run_code', None)
        kwargs.pop('pip_install', None)
        super().__init__(*args, **kwargs)
        self.debug_info_ref = debug_info_ref
        self.executed_calculations = set()  # Controle de cálculos já executados
        self.variable_cache = {}  # Cache de variáveis importantes

    def run_code(self, code: str) -> str:
        """Override para controlar execuções redundantes e variáveis"""
        # Evitar prints repetitivos de percentuais e valores
        repetitive_patterns = [
            'print(top5_total)', 'print(pe', 'print(sc', 'print("pe', 'print("sc',
            'participação', 'percentual', 'top5_total', '%'
        ]

        if any(pattern in code.lower() for pattern in repetitive_patterns):
            code_hash = hash(code.strip())
            if code_hash in self.executed_calculations:
                return "Resultado já calculado e exibido."
            self.executed_calculations.add(code_hash)

        # Controle rigoroso de variáveis Top5_total
        if 'Top5_total' in code:
            if '=' not in code and 'Top5_total' not in self.variable_cache:
                # Tentando usar sem definir - bloquear
                return "Erro: Variável Top5_total não está definida no contexto atual."
            elif '=' in code and code.strip().startswith('Top5_total'):
                # Definindo a variável - permitir e cachear
                pass

        try:
            result = super().run_code(code)

            # Cache inteligente de variáveis importantes
            if 'Top5_total' in code and '=' in code and code.strip().startswith('Top5_total'):
                try:
                    # Extrair valor do resultado se possível
                    numeric_match = re.search(r'(\d+\.?\d*)', str(result))
                    if numeric_match:
                        self.variable_cache['Top5_total'] = float(numeric_match.group(1))
                except:
                    pass

            return result

        except NameError as e:
            if 'Top5_total' in str(e):
                return "Erro: A variável Top5_total não está disponível neste contexto de execução."
            return f"Erro de variável não definida: {str(e)}"
        except Exception as e:
            return f"Erro na execução do código: {str(e)}"