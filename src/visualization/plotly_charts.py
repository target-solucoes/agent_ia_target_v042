"""
Funções de visualização com Plotly para gráficos de barras e linhas
"""

import streamlit as st
import plotly.express as px
import pandas as pd
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.formatters import format_compact_number


def render_plotly_visualization(visualization_data):
    """
    Renderiza gráfico Plotly baseado nos dados de visualização do agente.
    Retorna True se renderizou um gráfico, False se renderizou uma tabela.
    """
    if not visualization_data:
        return False

    # Se não é para visualizar como gráfico, não fazer nada
    chart_type = visualization_data.get('type')
    if chart_type not in ['bar_chart', 'line_chart'] or not visualization_data.get('has_data', False):
        return False

    try:
        # Obter dados do DataFrame
        df = visualization_data.get('data')
        config = visualization_data.get('config', {})

        if df is None or df.empty:
            return False

        # Processar baseado no tipo de gráfico
        if chart_type == 'line_chart':
            # Lógica específica para gráficos de linha
            return render_line_chart(df, config)
        else:
            # Lógica original para gráficos de barra
            return render_bar_chart(df, config)

    except Exception as e:
        # Em caso de erro, não fazer nada e deixar o conteúdo textual aparecer
        st.error(f"Erro ao renderizar gráfico: {str(e)}")
        return False


def render_bar_chart(df, config):
    """
    Renderiza gráfico de barras horizontais
    """
    # Preparar rótulos compactos para as barras
    df_with_labels = df.copy()
    df_with_labels['value_label'] = df_with_labels['value'].apply(format_compact_number)

    # Verificar se foi detectado como ID categórico pelo backend e ajustar labels
    is_categorical_id = config.get('is_categorical_id', False)

    # Detecção adicional no frontend como fallback
    if not is_categorical_id and not df_with_labels.empty:
        sample_labels = df_with_labels['label'].head().astype(str)
        # Padrões que indicam IDs categóricos: números de 3-8 dígitos
        for label in sample_labels:
            if label.isdigit() and 3 <= len(label) <= 8:
                is_categorical_id = True
                break

    # Formatar labels para IDs categóricos
    if is_categorical_id:
        original_col = config.get('original_label_column', '')
        if 'cliente' in original_col.lower():
            # Para códigos de cliente, adicionar prefixo
            df_with_labels['label'] = 'Cliente ' + df_with_labels['label'].astype(str)

    # Criar gráfico de barras horizontais
    fig = px.bar(
        df_with_labels,
        x='value',
        y='label',
        orientation='h',
        title=config.get('title', 'Top Resultados'),
        labels={
            'value': 'Valor',
            'label': 'Item'
        },
        text='value_label'  # Usar rótulos compactos
    )

    # Configurações de layout para melhor aparência
    fig.update_layout(
        height=max(400, len(df) * 45),  # Altura ligeiramente aumentada para acomodar rótulos
        margin=dict(l=20, r=120, t=50, b=20),  # Margem direita aumentada para rótulos
        xaxis_title="",
        yaxis_title="",
        showlegend=False,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
    )

    # Personalizar barras com cores azuis harmoniosas
    fig.update_traces(
        marker_color='#3498db',  # Azul agradável e harmonioso
        marker_line_color='#2980b9',  # Borda azul mais escura
        marker_line_width=1.5,
        opacity=0.85,
        textposition='outside',  # Posição dos rótulos fora das barras
        textfont=dict(size=11, color='#2c3e50', family='Arial')  # Estilo do texto dos rótulos
    )

    # Configurações do eixo Y para melhor legibilidade
    if is_categorical_id:
        # Forçar tratamento como categoria para códigos de cliente/produto
        fig.update_yaxes(
            type='category',  # Forçar tipo categoria
            categoryorder='total ascending',  # Ordenar por valor
            tickfont=dict(size=12, family='Arial')
        )
    else:
        # Comportamento padrão para outras categorias
        fig.update_yaxes(
            categoryorder='total ascending',  # Ordenar por valor
            tickfont=dict(size=12, family='Arial')
        )

    # Configurações do eixo X com formatação inteligente e estilo aprimorado
    value_format = config.get('value_format', 'number')
    if value_format == 'currency':
        fig.update_xaxes(
            tickformat=',.0f',  # Formato monetário com separadores de milhares
            tickprefix='R$ ',
            tickfont=dict(size=10, family='Arial'),
            gridcolor='rgba(52, 152, 219, 0.2)',  # Grid sutil em azul
            gridwidth=1
        )
    else:
        fig.update_xaxes(
            tickformat=',.0f',  # Formato numérico com separadores de milhares
            tickfont=dict(size=10, family='Arial'),
            gridcolor='rgba(52, 152, 219, 0.2)',  # Grid sutil em azul
            gridwidth=1
        )

    # Ajustar título do gráfico com melhor estilo
    fig.update_layout(
        title_font=dict(size=16, family='Arial', color='#2c3e50'),
        title_x=0.5  # Centralizar título
    )

    # Renderizar o gráfico no Streamlit
    st.plotly_chart(fig, use_container_width=True, theme="streamlit")

    return True


def render_line_chart(df, config):
    """
    Renderiza gráfico de linha para análise temporal.
    Retorna True se renderizou com sucesso, False caso contrário.
    """
    try:
        # Preparar dados para o gráfico de linha
        df_chart = df.copy()

        # Converter coluna de data para datetime se necessário
        try:
            df_chart['date'] = pd.to_datetime(df_chart['date'])
        except:
            pass  # Se não conseguir converter, usar como está

        # Formatar valores para exibição
        df_chart['value_label'] = df_chart['value'].apply(format_compact_number)

        # Criar gráfico de linha
        fig = px.line(
            df_chart,
            x='date',
            y='value',
            title=config.get('title', 'Análise Temporal'),
            labels={
                'date': config.get('x_label', 'Data'),
                'value': config.get('y_label', 'Valor')
            },
            markers=True  # Adicionar marcadores nos pontos
        )

        # Configurações de layout
        fig.update_layout(
            height=500,
            margin=dict(l=50, r=50, t=60, b=50),
            xaxis_title=config.get('x_label', 'Data'),
            yaxis_title=config.get('y_label', 'Valor'),
            title_font=dict(size=16, family='Arial', color='#2c3e50'),
            title_x=0.5,
            hovermode='x'
        )

        # Personalizar linha
        fig.update_traces(
            line=dict(width=3, color='#3498db'),  # Linha azul mais espessa
            marker=dict(size=6, color='#2980b9', line=dict(width=1, color='white')),
            hovertemplate='<b>%{y}</b><br>%{x}<extra></extra>'
        )

        # Configurar eixos
        fig.update_yaxes(
            tickformat=',.0f',
            tickfont=dict(size=11, family='Arial'),
            gridcolor='rgba(52, 152, 219, 0.2)',
            gridwidth=1
        )

        fig.update_xaxes(
            tickfont=dict(size=11, family='Arial'),
            gridcolor='rgba(52, 152, 219, 0.1)',
            gridwidth=1
        )

        # Renderizar no Streamlit
        st.plotly_chart(fig, use_container_width=True, theme="streamlit")

        return True

    except Exception as e:
        st.error(f"Erro ao renderizar gráfico de linha: {str(e)}")
        return False