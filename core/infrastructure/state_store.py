"""In-memory state store for pipeline state persistence.

This module provides a simple key-value state store with:
- get/set operations
- append for list values
- compare_and_set for atomic updates (async safety)

Current implementation is in-memory dict.
Future versions can swap to Redis, Postgres, or other backends.
"""

from typing import Any, Dict, List, Optional
from core.logger import get_logger

logger = get_logger(__name__)


class StateStore:
    """
    In-memory key-value state store.

    Supports:
    - Simple get/set operations
    - List append operations
    - Compare-and-set for atomic updates

    Thread-safe for single-threaded pipelines.
    For multi-threaded use, add locking.

    Usage:
        store = StateStore()
        store.set("lead:123:status", "scheduled")
        status = store.get("lead:123:status")
        store.compare_and_set("lead:123:status", "scheduled", "sent")
    """

    def __init__(self) -> None:
        """Initialize empty state store."""
        self._data: Dict[str, Any] = {}
        logger.debug("StateStore initialized")

    def get(self, key: str, default: Any = None) -> Any:
        """
        Get value for a key.

        Args:
            key: State key to retrieve
            default: Value to return if key not found

        Returns:
            Stored value or default
        """
        return self._data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """
        Set value for a key.

        Args:
            key: State key to set
            value: Value to store
        """
        self._data[key] = value
        logger.debug(f"State set: {key}")

    def delete(self, key: str) -> bool:
        """
        Delete a key from the store.

        Args:
            key: State key to delete

        Returns:
            True if key existed and was deleted, False otherwise
        """
        if key in self._data:
            del self._data[key]
            logger.debug(f"State deleted: {key}")
            return True
        return False

    def exists(self, key: str) -> bool:
        """
        Check if a key exists.

        Args:
            key: State key to check

        Returns:
            True if key exists, False otherwise
        """
        return key in self._data

    def append(self, key: str, value: Any) -> int:
        """
        Append value to a list stored at key.

        Creates list if key doesn't exist.
        Raises TypeError if key exists but is not a list.

        Args:
            key: State key for list
            value: Value to append

        Returns:
            New length of list

        Raises:
            TypeError: If existing value is not a list
        """
        if key not in self._data:
            self._data[key] = []

        if not isinstance(self._data[key], list):
            raise TypeError(f"Cannot append to non-list value at key '{key}'")

        self._data[key].append(value)
        logger.debug(f"State appended to: {key}")
        return len(self._data[key])

    def compare_and_set(
        self,
        key: str,
        expected: Any,
        new_value: Any,
    ) -> bool:
        """
        Atomically set value only if current value matches expected.

        This is the critical primitive for async safety:
        - Prevents race conditions in state transitions
        - Returns False if value changed since read

        Args:
            key: State key to update
            expected: Expected current value (or None if key should not exist)
            new_value: New value to set if expected matches

        Returns:
            True if update succeeded, False if current value != expected
        """
        current = self._data.get(key)

        if current != expected:
            logger.debug(
                f"CAS failed for {key}: expected={expected}, current={current}"
            )
            return False

        self._data[key] = new_value
        logger.debug(f"CAS succeeded for {key}: {expected} -> {new_value}")
        return True

    def get_all_keys(self, prefix: Optional[str] = None) -> List[str]:
        """
        Get all keys, optionally filtered by prefix.

        Args:
            prefix: Optional prefix to filter keys

        Returns:
            List of matching keys
        """
        if prefix is None:
            return list(self._data.keys())
        return [k for k in self._data.keys() if k.startswith(prefix)]

    def clear(self) -> None:
        """Clear all state."""
        self._data = {}
        logger.debug("StateStore cleared")

    def size(self) -> int:
        """Get number of keys in store."""
        return len(self._data)


# Singleton instance for global access
_state_store: Optional[StateStore] = None


def get_state_store() -> StateStore:
    """
    Get the global StateStore instance.

    Returns:
        Singleton StateStore instance
    """
    global _state_store
    if _state_store is None:
        _state_store = StateStore()
    return _state_store


def reset_state_store() -> None:
    """Reset the global StateStore instance (for testing)."""
    global _state_store
    _state_store = None
