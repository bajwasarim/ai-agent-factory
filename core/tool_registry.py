"""Tool registry for dependency injection of callable tools."""

from typing import Any, Callable, Optional


class ToolRegistry:
    """
    Registry for managing callable tools.

    Supports dependency injection pattern - tools can be registered
    at startup and retrieved by agents at runtime.
    """

    def __init__(self) -> None:
        """Initialize an empty tool registry."""
        self._tools: dict[str, Callable[..., Any]] = {}

    def register(self, name: str, func: Callable[..., Any]) -> None:
        """
        Register a tool function.

        Args:
            name: Unique name to identify the tool.
            func: Callable function to register.

        Raises:
            ValueError: If a tool with this name already exists.
        """
        if name in self._tools:
            raise ValueError(f"Tool '{name}' is already registered")
        self._tools[name] = func

    def get(self, name: str) -> Optional[Callable[..., Any]]:
        """
        Retrieve a registered tool by name.

        Args:
            name: Name of the tool to retrieve.

        Returns:
            The registered callable, or None if not found.
        """
        return self._tools.get(name)

    def get_or_raise(self, name: str) -> Callable[..., Any]:
        """
        Retrieve a tool, raising if not found.

        Args:
            name: Name of the tool to retrieve.

        Returns:
            The registered callable.

        Raises:
            KeyError: If tool is not registered.
        """
        if name not in self._tools:
            raise KeyError(f"Tool '{name}' is not registered")
        return self._tools[name]

    def list_tools(self) -> list[str]:
        """
        List all registered tool names.

        Returns:
            List of registered tool names.
        """
        return list(self._tools.keys())

    def unregister(self, name: str) -> bool:
        """
        Remove a tool from the registry.

        Args:
            name: Name of the tool to remove.

        Returns:
            True if tool was removed, False if it didn't exist.
        """
        if name in self._tools:
            del self._tools[name]
            return True
        return False


# Global default registry instance - import this for hot-swappable tools
tool_registry = ToolRegistry()

# Backward compatibility alias
default_registry = tool_registry
