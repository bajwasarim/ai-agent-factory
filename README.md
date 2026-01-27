You are a senior Python platform engineer.

Create a LOCAL-FIRST, modular AI Agent Factory framework for running multi-agent automation pipelines.

GOAL:
Build a reusable internal framework that can load project configs, initialize agents, execute pipelines sequentially, and support future expansion to parallel execution and cloud deployment.

CONSTRAINTS:
- Local execution only (no cloud dependencies)
- Lightweight
- Production-quality structure
- Easy extensibility
- No hardcoded project logic in core
- Environment variable driven secrets
- Config-driven projects

----------------------------------
PROJECT ROOT STRUCTURE (MANDATORY)
----------------------------------

Create this exact structure:

ai-agent-factory/
│
├── core/
│   ├── agent_base.py
│   ├── task_base.py
│   ├── llm_client.py
│   ├── tool_registry.py
│   ├── config_loader.py
│   └── logger.py
│
├── projects/
│   └── landing_generator/
│       ├── project.yaml
│       ├── agents.py
│       ├── tasks.py
│       └── pipeline.py
│
├── pipelines/
│   └── runner.py
│
├── prompts/
│   └── templates/
│
├── data/
│   ├── inputs/
│   └── outputs/
│
├── main.py
├── requirements.txt
├── .env.example
└── README.md

----------------------------------
CORE MODULE REQUIREMENTS
----------------------------------

Implement:

1) BaseAgent abstract class
- Located in core/agent_base.py
- Must enforce run(input_data) method
- Accept name and optional tools list

2) BaseTask abstract class
- Located in core/task_base.py
- Must support execute(context) interface

3) LLM Client Wrapper
- Located in core/llm_client.py
- Must support generate(prompt, model="gpt-4o-mini")
- Load API key from environment variable
- Must be easily swappable for Claude later

4) Tool Registry
- Located in core/tool_registry.py
- Must allow register(name, func)
- Must allow get(name)

5) Config Loader
- YAML loader
- Located in core/config_loader.py

6) Logger Utility
- Located in core/logger.py
- Use Python logging module
- Colored console output preferred
- Timestamped logs

----------------------------------
PIPELINE ENGINE
----------------------------------

Implement PipelineRunner in:

pipelines/runner.py

Features:
- Accept ordered list of agents
- Execute sequentially
- Pass output of agent N into agent N+1
- Log agent start/end

----------------------------------
LANDING GENERATOR PROJECT (SKELETON ONLY)

Create placeholder project that demonstrates structure.

landing_generator/project.yaml:

Include fields:
- project_name
- description
- agents list
- inputs
- outputs

landing_generator/agents.py:

Create placeholder agents:
- ScraperAgent (stub)
- FilterAgent (stub)
- ContentAgent (stub)

Each must inherit BaseAgent.

landing_generator/tasks.py:

Define placeholder task objects if needed.

landing_generator/pipeline.py:

Expose function:

build_pipeline()

Which returns PipelineRunner instance with initialized agents.

----------------------------------
MAIN ENTRYPOINT

main.py must:

- Load landing generator pipeline
- Pass sample input:
    city: "Austin"
    niche: "dentists"
- Run pipeline
- Print completion message

----------------------------------
REQUIREMENTS.TXT

Include:

openai
python-dotenv
pyyaml
requests
rich

----------------------------------
ENV FILE

Create .env.example with:

OPENAI_API_KEY=
SERPER_API_KEY=

----------------------------------
README.md

Include:

- Setup instructions
- Virtualenv setup
- How to run main.py
- Folder explanation
- How to add new projects

----------------------------------
CODE QUALITY RULES

- Use type hints
- Use docstrings
- Avoid global state
- Modular imports only
- No hardcoded secrets
- Fail gracefully with errors

----------------------------------
OUTPUT

Generate all files with production-ready boilerplate and placeholder implementations that run without crashing.

After generation, ensure main.py can run and execute stub pipeline successfully.

Now generate the full project scaffold.
