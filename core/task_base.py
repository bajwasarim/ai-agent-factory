"""Abstract base class for all tasks."""

from abc import ABC, abstractmethod
from typing import Any


class BaseTask(ABC):
    """
    Abstract base class for discrete units of work.

    Tasks are smaller, reusable operations that agents can compose
    to accomplish their goals.
    """

    def __init__(self, name: str) -> None:
        """
        Initialize the task.

        Args:
            name: Unique identifier for this task.
        """
        self.name = name

    @abstractmethod
    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        """
        Execute the task.

        Args:
            context: Dictionary containing execution context and parameters.

        Returns:
            Dictionary containing task results.
        """
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}(name={self.name!r})"
