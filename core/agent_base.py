"""Abstract base class for all agents."""

from abc import ABC, abstractmethod
from typing import Any, Callable, Optional


class BaseAgent(ABC):
    """
    Abstract base class that all agents must inherit from.

    Agents are the primary execution units in pipelines. Each agent
    receives input data, processes it, and returns output data.
    """

    def __init__(
        self,
        name: str,
        tools: Optional[list[Callable[..., Any]]] = None,
    ) -> None:
        """
        Initialize the agent.

        Args:
            name: Unique identifier for this agent.
            tools: Optional list of callable tools the agent can use.
        """
        self.name = name
        self.tools = tools or []

    @abstractmethod
    def run(self, input_data: dict[str, Any]) -> dict[str, Any]:
        """
        Execute the agent's main logic.

        Args:
            input_data: Dictionary containing input parameters.

        Returns:
            Dictionary containing output results.
        """
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r})"
