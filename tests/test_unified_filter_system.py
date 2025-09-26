"""
Testes para o sistema unificado de filtros
Valida funcionalidades do core centralizado e integração
"""

import unittest
from datetime import datetime
import sys
import os

# Adicionar src ao path para imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from filters.unified_filter_core import (
    FilterState,
    FilterDefinition,
    FilterCategory,
    FilterAction,
    SQLFilterExtractor,
    ContextSynchronizer,
    UnifiedFilterManager,
    get_unified_filter_manager,
    reset_unified_filter_manager
)


class TestFilterState(unittest.TestCase):
    """Testes para a classe FilterState"""

    def setUp(self):
        self.filter_state = FilterState()

    def test_add_filter(self):
        """Testa adição de filtro"""
        filter_def = FilterDefinition(
            key="uf_cliente",
            value="SP",
            category=FilterCategory.GEOGRAPHIC
        )

        result = self.filter_state.add_filter(filter_def)
        self.assertTrue(result)
        self.assertIn(filter_def.filter_id, self.filter_state.filters)

    def test_remove_filter(self):
        """Testa remoção de filtro"""
        filter_def = FilterDefinition(
            key="uf_cliente",
            value="SP",
            category=FilterCategory.GEOGRAPHIC
        )

        self.filter_state.add_filter(filter_def)
        result = self.filter_state.remove_filter(filter_def.filter_id)
        self.assertTrue(result)
        self.assertNotIn(filter_def.filter_id, self.filter_state.filters)

    def test_disable_enable_filter(self):
        """Testa desabilitar/habilitar filtro"""
        filter_def = FilterDefinition(
            key="uf_cliente",
            value="SP",
            category=FilterCategory.GEOGRAPHIC
        )

        self.filter_state.add_filter(filter_def)

        # Desabilitar
        result = self.filter_state.disable_filter(filter_def.filter_id)
        self.assertTrue(result)
        self.assertIn(filter_def.filter_id, self.filter_state.disabled_filter_ids)

        # Verificar filtros ativos
        active_filters = self.filter_state.get_active_filters()
        self.assertNotIn(filter_def.filter_id, active_filters)

        # Habilitar novamente
        result = self.filter_state.enable_filter(filter_def.filter_id)
        self.assertTrue(result)
        self.assertNotIn(filter_def.filter_id, self.filter_state.disabled_filter_ids)

        # Verificar filtros ativos
        active_filters = self.filter_state.get_active_filters()
        self.assertIn(filter_def.filter_id, active_filters)

    def test_to_context_dict(self):
        """Testa conversão para dicionário de contexto"""
        filter_def = FilterDefinition(
            key="uf_cliente",
            value="SP",
            category=FilterCategory.GEOGRAPHIC
        )

        self.filter_state.add_filter(filter_def)
        context = self.filter_state.to_context_dict()

        self.assertIn("uf_cliente", context)
        self.assertEqual(context["uf_cliente"], "SP")

    def test_filters_by_category(self):
        """Testa filtros por categoria"""
        filter1 = FilterDefinition(
            key="uf_cliente",
            value="SP",
            category=FilterCategory.GEOGRAPHIC
        )
        filter2 = FilterDefinition(
            key="data_>=",
            value="2024-01-01",
            category=FilterCategory.TEMPORAL,
            operator=">="
        )

        self.filter_state.add_filter(filter1)
        self.filter_state.add_filter(filter2)

        geographic_filters = self.filter_state.get_filters_by_category(FilterCategory.GEOGRAPHIC)
        temporal_filters = self.filter_state.get_filters_by_category(FilterCategory.TEMPORAL)

        self.assertEqual(len(geographic_filters), 1)
        self.assertEqual(len(temporal_filters), 1)
        self.assertIn(filter1.filter_id, geographic_filters)
        self.assertIn(filter2.filter_id, temporal_filters)


class TestSQLFilterExtractor(unittest.TestCase):
    """Testes para o extrator SQL"""

    def setUp(self):
        self.extractor = SQLFilterExtractor()

    def test_simple_where_clause(self):
        """Testa extração de WHERE simples"""
        sql = "SELECT * FROM table WHERE uf_cliente = 'SP' AND data >= '2024-01-01'"

        filters = self.extractor.extract_filters_from_sql(sql)

        # Verificar se filtros foram extraídos
        self.assertGreater(len(filters), 0)

        # Verificar se pelo menos um filtro contém UF
        uf_filters = [f for f in filters if f.key == "uf_cliente"]
        self.assertGreater(len(uf_filters), 0)

    def test_complex_query_with_subquery(self):
        """Testa query complexa com subconsulta"""
        sql = """
        SELECT * FROM (
            SELECT * FROM vendas WHERE data >= '2024-01-01'
        ) WHERE uf_cliente = 'SP'
        """

        filters = self.extractor.extract_filters_from_sql(sql)

        # Verificar se conseguiu extrair filtros da query complexa
        self.assertGreater(len(filters), 0)

    def test_fallback_regex_extraction(self):
        """Testa fallback para extração via regex"""
        sql = "SELECT * FROM table WHERE uf_cliente = 'SP'"

        # Forçar uso do fallback
        filters = self.extractor._fallback_regex_extraction(sql)

        self.assertGreater(len(filters), 0)
        self.assertEqual(filters[0].key, "uf_cliente")
        self.assertEqual(filters[0].value, "SP")


class TestContextSynchronizer(unittest.TestCase):
    """Testes para o sincronizador de contexto"""

    def setUp(self):
        self.synchronizer = ContextSynchronizer()
        self.filter_state = FilterState()

    def test_sync_from_agent_context(self):
        """Testa sincronização do contexto do agente"""
        agent_context = {
            "uf_cliente": "SP",
            "data_>=": "2024-01-01"
        }

        result = self.synchronizer.sync_from_agent_context(agent_context, self.filter_state)
        self.assertTrue(result)

        # Verificar se filtros foram criados
        self.assertEqual(len(self.filter_state.filters), 2)

    def test_sync_to_agent_context(self):
        """Testa sincronização para contexto do agente"""
        filter1 = FilterDefinition(
            key="uf_cliente",
            value="SP",
            category=FilterCategory.GEOGRAPHIC
        )
        self.filter_state.add_filter(filter1)

        context = self.synchronizer.sync_to_agent_context(self.filter_state)

        self.assertIn("uf_cliente", context)
        self.assertEqual(context["uf_cliente"], "SP")

    def test_context_to_filters_conversion(self):
        """Testa conversão de contexto para filtros"""
        context = {
            "uf_cliente": "SP",
            "data_>=": "2024-01-01",
            "cod_cliente": "12345"
        }

        filters = self.synchronizer._context_to_filters(context)

        self.assertEqual(len(filters), 3)

        # Verificar categorização automática
        geographic_filters = [f for f in filters if f.category == FilterCategory.GEOGRAPHIC]
        temporal_filters = [f for f in filters if f.category == FilterCategory.TEMPORAL]
        client_filters = [f for f in filters if f.category == FilterCategory.CLIENT]

        self.assertEqual(len(geographic_filters), 1)
        self.assertEqual(len(temporal_filters), 1)
        self.assertEqual(len(client_filters), 1)


class TestUnifiedFilterManager(unittest.TestCase):
    """Testes para o gerenciador unificado"""

    def setUp(self):
        reset_unified_filter_manager()  # Reset estado global
        self.manager = UnifiedFilterManager()

    def tearDown(self):
        reset_unified_filter_manager()  # Limpar após cada teste

    def test_singleton_pattern(self):
        """Testa padrão singleton"""
        manager1 = get_unified_filter_manager()
        manager2 = get_unified_filter_manager()

        # Devem ser a mesma instância
        self.assertIs(manager1, manager2)

    def test_process_user_query_clear_command(self):
        """Testa comando de limpeza de filtros"""
        # Adicionar alguns filtros primeiro
        self.manager.filter_state.add_filter(FilterDefinition(
            key="uf_cliente",
            value="SP",
            category=FilterCategory.GEOGRAPHIC
        ))

        # Processar comando de limpeza
        has_changes, changes = self.manager.process_user_query("sem filtros")

        self.assertTrue(has_changes)
        self.assertIn("removidos", " ".join(changes).lower())
        self.assertEqual(len(self.manager.filter_state.filters), 0)

    def test_sync_with_agent_context(self):
        """Testa sincronização com contexto do agente"""
        agent_context = {
            "uf_cliente": "SP",
            "data_>=": "2024-01-01"
        }

        result = self.manager.sync_with_agent_context(agent_context)
        self.assertTrue(result)

        # Verificar se contexto foi sincronizado
        manager_context = self.manager.get_agent_context()
        self.assertEqual(manager_context["uf_cliente"], "SP")

    def test_toggle_filter(self):
        """Testa habilitar/desabilitar filtro"""
        filter_def = FilterDefinition(
            key="uf_cliente",
            value="SP",
            category=FilterCategory.GEOGRAPHIC
        )

        self.manager.filter_state.add_filter(filter_def)

        # Desabilitar
        result = self.manager.toggle_filter(filter_def.filter_id, False)
        self.assertTrue(result)

        # Verificar se foi desabilitado
        active_filters = self.manager.filter_state.get_active_filters()
        self.assertNotIn(filter_def.filter_id, active_filters)

        # Habilitar novamente
        result = self.manager.toggle_filter(filter_def.filter_id, True)
        self.assertTrue(result)

        # Verificar se foi habilitado
        active_filters = self.manager.filter_state.get_active_filters()
        self.assertIn(filter_def.filter_id, active_filters)

    def test_get_active_filters_summary(self):
        """Testa resumo de filtros ativos"""
        # Adicionar filtros de diferentes categorias
        filter1 = FilterDefinition(
            key="uf_cliente",
            value="SP",
            category=FilterCategory.GEOGRAPHIC
        )
        filter2 = FilterDefinition(
            key="data_>=",
            value="2024-01-01",
            category=FilterCategory.TEMPORAL,
            operator=">="
        )

        self.manager.filter_state.add_filter(filter1)
        self.manager.filter_state.add_filter(filter2)

        summary = self.manager.get_active_filters_summary()

        self.assertIn("Filtros ativos", summary)
        self.assertIn("geográfico", summary)
        self.assertIn("temporal", summary)


class TestIntegrationScenarios(unittest.TestCase):
    """Testes de cenários de integração completos"""

    def setUp(self):
        reset_unified_filter_manager()
        self.manager = get_unified_filter_manager()

    def tearDown(self):
        reset_unified_filter_manager()

    def test_complete_filter_lifecycle(self):
        """Testa ciclo completo de vida dos filtros"""
        # 1. Sincronizar contexto inicial do agente
        initial_context = {"uf_cliente": "SP"}
        self.manager.sync_with_agent_context(initial_context)

        # 2. Processar query do usuário que adiciona filtros
        has_changes, changes = self.manager.process_user_query("vendas de janeiro de 2024")

        # 3. Desabilitar um filtro
        filters = self.manager.filter_state.get_active_filters()
        if filters:
            first_filter_id = list(filters.keys())[0]
            self.manager.toggle_filter(first_filter_id, False)

        # 4. Obter contexto final para o agente
        final_context = self.manager.get_agent_context()

        # Verificar que o processo funcionou sem erros
        self.assertIsInstance(final_context, dict)

    def test_sql_extraction_and_sync(self):
        """Testa extração SQL e sincronização"""
        # Simular execução de SQL que gerou filtros
        sql_queries = [
            "SELECT * FROM vendas WHERE uf_cliente = 'SP' AND data >= '2024-01-01'"
        ]

        has_changes, changes = self.manager.process_sql_response(sql_queries)

        # Verificar se filtros foram extraídos
        active_filters = self.manager.filter_state.get_active_filters()
        self.assertGreater(len(active_filters), 0)

        # Verificar se contexto pode ser obtido
        context = self.manager.get_agent_context()
        self.assertIsInstance(context, dict)


if __name__ == '__main__':
    # Criar diretório de testes se não existir
    test_dir = os.path.dirname(__file__)
    if not os.path.exists(test_dir):
        os.makedirs(test_dir)

    # Executar testes
    unittest.main(verbosity=2)