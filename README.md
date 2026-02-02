# AI Agent Factory

Local-first modular AI agent framework for running multi-agent automation pipelines.

## Features

- **Modular Architecture**: Reusable core with strict separation from project-specific logic
- **Pipeline Engine**: Sequential agent execution with automatic output chaining
- **Config-Driven Projects**: YAML-based project configuration
- **Mock Mode Support**: Full testing without external API calls
- **Google Sheets Integration**: Export leads with fan-out by route

## Quick Start

### 1. Setup Environment

```bash
# Create virtual environment
python -m venv venv

# Activate (Windows)
venv\Scripts\activate

# Activate (macOS/Linux)
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment Variables

```bash
cp .env.example .env
# Edit .env with your API keys
```

Required environment variables:
- `OPENAI_API_KEY` - OpenAI API key
- `SERPER_API_KEY` - Serper API key for Google Maps search
- `GOOGLE_SHEETS_CREDENTIALS_PATH` - Path to Google service account JSON

Optional environment variables:
- `MOCK_MAPS=1` - Use mock Maps data
- `MOCK_SHEETS=1` - Use mock Sheets operations
- `MOCK_WEBSITE_CHECK=1` - Use mock website validation
- `PIPELINE_MAX_RETRIES=3` - Max retry attempts for failed validations
- `PIPELINE_MODE=normal|retry` - Pipeline execution mode

### 3. Run Pipeline

```bash
# Normal mode - full pipeline from Maps search
python main.py

# Retry mode - reprocess failed website validations
python main.py --mode retry
```

## Project Structure

```
ai-agent-factory/
├── core/                    # Reusable base classes (NEVER project-specific)
│   ├── agent_base.py        # BaseAgent abstract class
│   ├── task_base.py         # BaseTask abstract class
│   ├── llm_client.py        # LLM client wrapper
│   ├── tool_registry.py     # Tool registration system
│   ├── config_loader.py     # YAML config loader
│   ├── logger.py            # Colored console logging
│   └── tools/               # Reusable tools
│       └── serper_tool.py   # Serper API integration
│
├── projects/                # Project-specific implementations
│   ├── business_leadgen/    # Business lead generation project
│   │   ├── project.yaml     # Project configuration
│   │   ├── agents.py        # Project agents
│   │   └── pipeline.py      # Pipeline builder
│   └── landing_generator/   # Landing page generator (skeleton)
│
├── pipelines/
│   └── runner.py            # PipelineRunner engine
│
├── prompts/templates/       # Prompt templates
├── data/
│   ├── inputs/              # Input data
│   └── outputs/             # Output data
├── tests/                   # Test suite (122 tests)
├── main.py                  # Main entrypoint
└── requirements.txt         # Dependencies
```

## Business Lead Generation Project

The `business_leadgen` project identifies businesses WITHOUT real websites for landing-page outreach targeting.

### Pipeline Modes

**Normal Mode** (default):
```
MapsSearchAgent → BusinessNormalizeAgent → WebsitePresenceValidator 
    → LeadRouterAgent → LeadFormatterAgent → GoogleSheetsExportAgent
```

**Retry Mode**:
```
RetryInputLoaderAgent → WebsitePresenceValidator 
    → LeadRouterAgent → LeadFormatterAgent → GoogleSheetsExportAgent
```

### CLI Options

```bash
# Normal execution
python main.py

# Retry failed validations
python main.py --mode retry

# Custom retry sheet
python main.py --mode retry --retry-sheet-name CUSTOM_ERRORS

# Specify spreadsheet
python main.py --spreadsheet-id <SHEET_ID>
```

### Google Sheets Output

Leads are exported to three worksheets:
- `NO_WEBSITE_TARGETS` - Businesses without websites (primary leads)
- `HAS_WEBSITE_EXCLUDED` - Businesses with websites (excluded)
- `WEBSITE_CHECK_ERRORS` - Failed validations (for retry)

## Adding New Projects

1. Create folder: `projects/<project_name>/`

2. Add `project.yaml`:
```yaml
project_name: my_project
description: Project description
agents:
  - MyFirstAgent
  - MySecondAgent
inputs:
  - query
outputs:
  - results
```

3. Create `agents.py`:
```python
from core.agent_base import BaseAgent

class MyFirstAgent(BaseAgent):
    def __init__(self):
        super().__init__(name="MyFirstAgent")
    
    def run(self, input_data: dict) -> dict:
        # Your logic here
        return {"output": "result"}
```

4. Create `pipeline.py`:
```python
from pipelines.runner import PipelineRunner
from .agents import MyFirstAgent, MySecondAgent

def build_pipeline() -> PipelineRunner:
    return PipelineRunner([
        MyFirstAgent(),
        MySecondAgent(),
    ])
```

## Testing

```bash
# Run all tests
python -m pytest tests/ -v

# Run specific test file
python -m pytest tests/test_retry_input_loader.py -v

# Run with coverage
python -m pytest tests/ --cov=projects --cov-report=term-missing
```

### Test Coverage

| Test File | Tests | Coverage |
|-----------|-------|----------|
| test_retry_input_loader.py | 58 | Retry agent logic |
| test_retry_pipeline_mode.py | 35 | Mode selection |
| test_pipeline_contract.py | 10 | Formatter/router invariants |
| test_export_fanout.py | 17 | Fan-out export behavior |
| test_sheets_export.py | 2 | Integration tests |
| **Total** | **122** | All passing |

## Dependencies

- `openai` - LLM client
- `python-dotenv` - Environment variable loading
- `pyyaml` - YAML configuration
- `requests` - HTTP client
- `rich` - Colored console output
- `gspread` - Google Sheets API
- `google-auth` - Google authentication

## License

MIT
