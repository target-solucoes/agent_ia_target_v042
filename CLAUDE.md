# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is an AI agent project built with Streamlit that provides intelligent data analysis capabilities for commercial datasets. The application uses the Agno framework with DuckDB for data processing and OpenAI models for natural language interactions.

## Commands

### Environment Management
```bash
# Use uv for package management instead of pip
uv pip install package_name
uv pip install -r requirements.txt

# Install the project in development mode
uv pip install -e .
```

### Running the Application
```bash
# Start the Streamlit application
streamlit run app.py

# Alternative with Python module
python -m streamlit run app.py
```

## Architecture

### Core Components

**Main Application (`app.py`)**
- Streamlit-based web interface handling user interactions and chat interface
- Manages session state, filter management, and debugging capabilities
- Integrates with Plotly for interactive charts and visualizations
- Implements filter disable/enable functionality with context preservation

**Agent System (`src/chatbot_agents.py`)**
- Creates and configures the PrincipalAgent using Agno framework
- Integrates OpenAI GPT models (currently using "gpt-5-nano-2025-08-07")
- Provides DebugDuckDbTools for SQL query execution with automatic string normalization
- Includes persistent context management and column hierarchy for intelligent filtering
- Memory management with session-based storage

**Modular Components Structure**
- `src/config/`: Model and agent configuration files
- `src/tools/`: Custom DuckDB and Python tools with debugging capabilities
- `src/parsers/`: SQL context extraction for filter management
- `src/filters/`: Filter management system with hierarchical organization
- `src/prompts/`: Agent prompt configuration
- `src/utils/`: Data loading and formatting utilities
- `src/visualization/`: Plotly chart generation and styling

**Text Processing (`src/text_normalizer.py`)**
- Handles text normalization and encoding issues
- Provides temporal entity parsing (dates, months, years in Portuguese)
- Maps aliases from `data/mappings/alias.yaml` for data field recognition
- Supports natural language date parsing ("julho de 2015" → date ranges)

**Comparative Analysis (`src/comparative_calculator.py`)**
- Automatically detects when comparative calculations are needed
- Calculates growth metrics, variations, and period comparisons
- Generates SQL instructions for temporal analysis
- Provides formatted summaries of comparative results

### Data Architecture

**Data Storage**
- Primary data: `data/raw/DadosComercial_resumido_v02.parquet`
- Field mappings: `data/mappings/alias.yaml` (defines column aliases and state abbreviations)
- Session storage: `sessions/` directory for user session persistence

**Data Processing Flow**
1. User query → Text normalization → Alias mapping
2. Agent processes query using DebugDuckDbTools (with automatic string normalization)
3. SQL execution with WHERE clause context extraction
4. Results processing with visualization data generation
5. Streamlit rendering with Plotly charts

### Key Features

**Interactive Filter Management**
- Sidebar displays active filters with toggle controls
- Supports disabling specific filters without losing context
- Hierarchical filter organization (temporal, geographic, product, client, representative)
- Context preservation across queries using persistent context system

**Intelligent Visualization**
- Automatic chart type detection (bar charts, line charts)
- Responsive design with compact number formatting
- Support for categorical IDs and temporal data
- Plotly integration with custom styling

**Memory and Context**
- Session-based memory using UUID-based session management
- Persistent context between queries through PrincipalAgent.persistent_context
- Filter state management across conversations
- WHERE clause context extraction for automatic filter detection

## Development Guidelines

### Code Organization
- Main UI logic in `app.py`
- Business logic separated into modular `src/` components
- Configuration isolated in `src/config/`
- Data mappings in `data/mappings/`
- Session data isolated in `sessions/`

### Data Processing Patterns
- Use TextNormalizer for all text processing and temporal parsing
- Leverage ComparativeCalculator for growth and variation analysis
- Extract WHERE clause context using `sql_context_parser` for filter management
- Apply alias mappings for user-friendly field references
- Use DebugDuckDbTools for automatic string normalization in SQL queries

### Agent Development
- Configure models in `src/config/model_config.py` (current: "gpt-5-nano-2025-08-07")
- Define agent behavior in `src/config/agent_config.py` including column hierarchy
- Use OptimizedPythonTools and DebugDuckDbTools for enhanced functionality
- Implement debug information capture through debug_info_ref parameter

### UI/UX Patterns
- Filter management through interactive sidebar with apply_disabled_filters_to_context()
- Debug mode toggle for development and troubleshooting
- Session-based state management with UUID identification
- Responsive layout with proper error handling

### Dependencies
- **Core**: streamlit, pandas, agno>=1.8.1, duckdb
- **AI**: openai>=1.0.0
- **Data**: pyarrow, fastparquet, pyyaml, chardet
- **Visualization**: plotly>=5.0.0
- **Utils**: python-dotenv, numpy, sqlalchemy, ipykernel

## Configuration

### Model Configuration
Current model: "gpt-5-nano-2025-08-07" (defined in `src/config/model_config.py`)
Configure OpenAI API key in `.env` file.

### Data Mappings
Edit `data/mappings/alias.yaml` to:
- Add new column aliases for natural language queries
- Update state abbreviation mappings
- Define new metric conventions

### Agent Configuration
Edit `src/config/agent_config.py` to modify:
- Column hierarchy for intelligent filtering
- Agent behavior and tools configuration
- Memory and session management settings

## Session Management

The application uses UUID-based session management with persistent context. Each user session maintains:
- Query history and context through PrincipalAgent.persistent_context
- Active filter states with disable/enable functionality
- Persistent agent memory across conversations
- Disabled filter preferences preserved across queries