# AI Agent Factory - Copilot Instructions

## Architecture Overview

Local-first modular AI agent framework with strict separation between reusable core and project-specific logic.

```
core/           → Reusable base classes and utilities (NEVER project-specific)
projects/       → Project folders with config, agents, tasks, pipeline
pipelines/      → Pipeline execution engine
prompts/        → Prompt templates
data/           → inputs/ and outputs/ for runtime data
```

## Critical Rules

1. **Secrets**: Use environment variables only. Load via `python-dotenv`. Never hardcode API keys.
2. **Folder boundaries**: Core modules must NOT import from `projects/`. Projects import FROM core.
3. **Type hints + docstrings**: Required on all functions and classes.
4. **Composition over inheritance**: Extend via composition; inherit only from base classes.

## Core Module Patterns

### BaseAgent (`core/agent_base.py`)
```python
from abc import ABC, abstractmethod
class BaseAgent(ABC):
    def __init__(self, name: str, tools: list = None): ...
    @abstractmethod
    def run(self, input_data: dict) -> dict: ...
```

### BaseTask (`core/task_base.py`)
```python
@abstractmethod
def execute(self, context: dict) -> dict: ...
```

### LLM Client (`core/llm_client.py`)
- Method: `generate(prompt: str, model: str = "gpt-4o-mini") -> str`
- Load `OPENAI_API_KEY` from env; must be swappable for other providers

### Tool Registry (`core/tool_registry.py`)
- `register(name: str, func: Callable)` and `get(name: str) -> Callable`

## Adding New Projects

1. Create folder: `projects/<project_name>/`
2. Add `project.yaml` with: `project_name`, `description`, `agents`, `inputs`, `outputs`
3. Create `agents.py` (inherit `BaseAgent`), `tasks.py`, `pipeline.py`
4. Expose `build_pipeline()` in `pipeline.py` returning configured `PipelineRunner`

## Pipeline Execution

`PipelineRunner` (in `pipelines/runner.py`):
- Accepts ordered agent list
- Executes sequentially: output of agent N → input of agent N+1
- Logs agent start/end via `core/logger.py`

## Commands

```bash
# Setup
python -m venv venv && venv\Scripts\activate  # Windows
pip install -r requirements.txt

# Run
python main.py
```

## Dependencies

`openai`, `python-dotenv`, `pyyaml`, `requests`, `rich` (for colored logging)

## Copilot Behavior Rules

- Do NOT regenerate folder structure
- Do NOT create new files unless explicitly requested
- Only modify files named in the user request
- Prefer minimal diffs over full rewrites
- Do NOT introduce new dependencies unless explicitly approved
- Avoid cloud services unless explicitly requested
- Do NOT auto-upgrade model names or SDK versions
- Preserve existing architecture boundaries
