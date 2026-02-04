"""In-memory message bus for event-driven architecture.

This module provides a simple publish/subscribe message bus.
Events are dispatched synchronously to all registered handlers.

CRITICAL INVARIANT: publish() MUST NOT raise exceptions.
Failures are logged and dropped to prevent pipeline blocking.

Future versions can swap to Redis Pub/Sub, Kafka, or RabbitMQ.
"""

from typing import Any, Callable, Dict, List, Optional
from core.logger import get_logger

logger = get_logger(__name__)


class MessageBus:
    """
    In-memory publish/subscribe message bus.

    Thread-safe for single-threaded pipelines.
    For multi-threaded use, add locking.

    Usage:
        bus = MessageBus()
        bus.subscribe("lead.created", handler_func)
        bus.publish("lead.created", {"lead_id": "123"})
    """

    def __init__(self) -> None:
        """Initialize empty message bus."""
        self._subscribers: Dict[str, List[Callable[[Dict[str, Any]], None]]] = {}
        self._event_history: List[Dict[str, Any]] = []
        self._max_history: int = 1000
        logger.debug("MessageBus initialized")

    def subscribe(
        self,
        event_name: str,
        handler: Callable[[Dict[str, Any]], None],
    ) -> None:
        """
        Subscribe a handler to an event type.

        Args:
            event_name: Event type to subscribe to (e.g., "lead.created")
            handler: Callback function that accepts event payload dict
        """
        if event_name not in self._subscribers:
            self._subscribers[event_name] = []
        self._subscribers[event_name].append(handler)
        logger.debug(f"Handler subscribed to '{event_name}'")

    def unsubscribe(
        self,
        event_name: str,
        handler: Callable[[Dict[str, Any]], None],
    ) -> bool:
        """
        Unsubscribe a handler from an event type.

        Args:
            event_name: Event type to unsubscribe from
            handler: Handler function to remove

        Returns:
            True if handler was found and removed, False otherwise
        """
        if event_name not in self._subscribers:
            return False
        try:
            self._subscribers[event_name].remove(handler)
            logger.debug(f"Handler unsubscribed from '{event_name}'")
            return True
        except ValueError:
            return False

    def publish(
        self,
        event_name: str,
        payload: Dict[str, Any],
    ) -> int:
        """
        Publish an event to all subscribers.

        CRITICAL: This method MUST NOT raise exceptions.
        Handler failures are logged and dropped.

        Args:
            event_name: Event type to publish (e.g., "lead.landing.generated")
            payload: Event data dictionary

        Returns:
            Number of handlers that successfully processed the event
        """
        # Record in history (for testing/debugging)
        event_record = {
            "event_name": event_name,
            "payload": payload,
        }
        self._event_history.append(event_record)
        if len(self._event_history) > self._max_history:
            self._event_history = self._event_history[-self._max_history:]

        handlers = self._subscribers.get(event_name, [])
        if not handlers:
            logger.debug(f"No handlers for event '{event_name}'")
            return 0

        success_count = 0
        for handler in handlers:
            try:
                handler(payload)
                success_count += 1
            except Exception as e:
                # Log and drop - NEVER raise from publish()
                logger.warning(
                    f"Handler failed for event '{event_name}': {e}. "
                    "Event dropped (non-blocking)."
                )

        logger.debug(
            f"Event '{event_name}' delivered to {success_count}/{len(handlers)} handlers"
        )
        return success_count

    def get_event_history(
        self,
        event_name: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get event history for testing/debugging.

        Args:
            event_name: Filter by event type, or None for all events

        Returns:
            List of event records (event_name, payload)
        """
        if event_name is None:
            return list(self._event_history)
        return [e for e in self._event_history if e["event_name"] == event_name]

    def clear_history(self) -> None:
        """Clear event history."""
        self._event_history = []

    def get_subscriber_count(self, event_name: str) -> int:
        """Get number of subscribers for an event type."""
        return len(self._subscribers.get(event_name, []))


# Singleton instance for global access
_message_bus: Optional[MessageBus] = None


def get_message_bus() -> MessageBus:
    """
    Get the global MessageBus instance.

    Returns:
        Singleton MessageBus instance
    """
    global _message_bus
    if _message_bus is None:
        _message_bus = MessageBus()
    return _message_bus


def reset_message_bus() -> None:
    """Reset the global MessageBus instance (for testing)."""
    global _message_bus
    _message_bus = None
