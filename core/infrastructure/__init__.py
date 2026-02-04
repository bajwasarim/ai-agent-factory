"""Infrastructure components for event-driven architecture.

This module provides swappable infrastructure interfaces:
- MessageBus: Publish/subscribe event system
- StateStore: Key-value state persistence

Current implementations are in-memory for simplicity.
Future versions can swap to Redis, Kafka, or Postgres.
"""

from core.infrastructure.message_bus import MessageBus, get_message_bus
from core.infrastructure.state_store import StateStore, get_state_store

__all__ = [
    "MessageBus",
    "get_message_bus",
    "StateStore",
    "get_state_store",
]
