"""Sequential pipeline execution engine."""
import core.tools.serper_tool
from typing import Any, Dict, List

from core.agent_base import BaseAgent
from core.logger import get_logger
from dotenv import load_dotenv
logger = get_logger(__name__)
load_dotenv()

class PipelineRunner:
    """
    Sequential pipeline executor.
    
    Executes agents in order, passing mutable context dict between them.
    Architecture: Input → Agent1 → Agent2 → Agent3 → Output
    """

    def __init__(self, agents: List[BaseAgent]) -> None:
        """
        Initialize the pipeline runner.

        Args:
            agents: Ordered list of agents to execute sequentially.

        Raises:
            ValueError: If agents list is empty.
        """
        if not agents:
            raise ValueError("Pipeline must contain at least one agent")
        self.agents = agents

    def run(self, initial_context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute pipeline sequentially.

        Each agent receives the accumulated context from previous agents.
        Agent output is merged into the context for the next agent.

        Args:
            initial_context: Initial input data dict.

        Returns:
            Final context dict after all agents have executed.

        Raises:
            TypeError: If an agent returns non-dict output.
            RuntimeError: If any agent fails during execution.
        """
        context = dict(initial_context)
        total_agents = len(self.agents)

        logger.info(f"Pipeline started with {total_agents} agent(s)")

        for idx, agent in enumerate(self.agents, start=1):
            agent_name = getattr(agent, "name", agent.__class__.__name__)
            logger.info(f"Agent {idx}/{total_agents} started: {agent_name}")

            try:
                result = agent.run(context)

                if not isinstance(result, dict):
                    raise TypeError(
                        f"Agent '{agent_name}' returned {type(result).__name__}, expected dict"
                    )

                context.update(result)
                logger.info(f"Agent '{agent_name}' completed successfully")
                logger.info(f"Context keys after {agent_name}: {context.keys()}")

            except ValueError as e:
                if "SERPER_API_KEY" in str(e):
                    logger.error("Missing SERPER_API_KEY — skipping pipeline with empty results")
                    return {"leads": []}
                raise
            except TypeError:
                raise
            except Exception as e:
                logger.exception(f"Agent '{agent_name}' failed with error: {e}")
                raise RuntimeError(
                    f"Pipeline stopped at agent '{agent_name}': {e}"
                ) from e

        logger.info("Pipeline completed successfully")
        return context

    def __repr__(self) -> str:
        """Return string representation of pipeline."""
        agent_names = [getattr(a, "name", a.__class__.__name__) for a in self.agents]
        return f"PipelineRunner(agents={agent_names})"
