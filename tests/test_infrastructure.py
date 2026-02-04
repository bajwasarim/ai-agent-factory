"""Tests for infrastructure components (MessageBus, StateStore).

These tests validate:
- MessageBus publish/subscribe semantics
- MessageBus never-raise guarantee
- StateStore CRUD operations
- StateStore compare-and-set (CAS) semantics
"""

import pytest
from core.infrastructure.message_bus import (
    MessageBus,
    get_message_bus,
    reset_message_bus,
)
from core.infrastructure.state_store import (
    StateStore,
    get_state_store,
    reset_state_store,
)


# ============================================================================
# MessageBus Tests
# ============================================================================


class TestMessageBusBasics:
    """Basic MessageBus functionality tests."""

    def test_publish_without_subscribers_returns_zero(self):
        """Publishing to event with no subscribers returns 0."""
        bus = MessageBus()
        result = bus.publish("test.event", {"data": "value"})
        assert result == 0

    def test_subscribe_and_publish_calls_handler(self):
        """Subscribed handler receives published event."""
        bus = MessageBus()
        received = []

        def handler(payload):
            received.append(payload)

        bus.subscribe("test.event", handler)
        bus.publish("test.event", {"key": "value"})

        assert len(received) == 1
        assert received[0] == {"key": "value"}

    def test_multiple_subscribers_all_called(self):
        """All subscribers receive the event."""
        bus = MessageBus()
        results = []

        def handler1(payload):
            results.append(("h1", payload))

        def handler2(payload):
            results.append(("h2", payload))

        bus.subscribe("test.event", handler1)
        bus.subscribe("test.event", handler2)
        count = bus.publish("test.event", {"data": 1})

        assert count == 2
        assert len(results) == 2

    def test_different_events_isolated(self):
        """Subscribers only receive their subscribed events."""
        bus = MessageBus()
        received_a = []
        received_b = []

        bus.subscribe("event.a", lambda p: received_a.append(p))
        bus.subscribe("event.b", lambda p: received_b.append(p))

        bus.publish("event.a", {"type": "a"})

        assert len(received_a) == 1
        assert len(received_b) == 0

    def test_unsubscribe_removes_handler(self):
        """Unsubscribed handler no longer receives events."""
        bus = MessageBus()
        received = []

        def handler(payload):
            received.append(payload)

        bus.subscribe("test.event", handler)
        bus.unsubscribe("test.event", handler)
        bus.publish("test.event", {"data": 1})

        assert len(received) == 0

    def test_unsubscribe_returns_false_if_not_found(self):
        """Unsubscribe returns False if handler not found."""
        bus = MessageBus()
        result = bus.unsubscribe("nonexistent", lambda p: None)
        assert result is False

    def test_get_subscriber_count(self):
        """Subscriber count is accurate."""
        bus = MessageBus()
        assert bus.get_subscriber_count("test.event") == 0

        bus.subscribe("test.event", lambda p: None)
        assert bus.get_subscriber_count("test.event") == 1

        bus.subscribe("test.event", lambda p: None)
        assert bus.get_subscriber_count("test.event") == 2


class TestMessageBusNeverRaises:
    """CRITICAL: publish() must never raise exceptions."""

    def test_publish_catches_handler_exception(self):
        """Handler exception is caught, not propagated."""
        bus = MessageBus()

        def failing_handler(payload):
            raise ValueError("Handler failed!")

        bus.subscribe("test.event", failing_handler)

        # This MUST NOT raise
        result = bus.publish("test.event", {"data": 1})

        # Handler failed, so 0 successful deliveries
        assert result == 0

    def test_publish_continues_after_handler_failure(self):
        """Other handlers still called after one fails."""
        bus = MessageBus()
        results = []

        def failing_handler(payload):
            raise RuntimeError("Oops")

        def good_handler(payload):
            results.append(payload)

        bus.subscribe("test.event", failing_handler)
        bus.subscribe("test.event", good_handler)

        count = bus.publish("test.event", {"data": 1})

        assert count == 1  # Only good_handler succeeded
        assert len(results) == 1

    def test_publish_with_none_payload_does_not_raise(self):
        """Publish with unusual payloads does not raise."""
        bus = MessageBus()
        # Empty payload
        result = bus.publish("test.event", {})
        assert result == 0  # No subscribers, but no error


class TestMessageBusEventHistory:
    """Event history for testing/debugging."""

    def test_event_history_records_events(self):
        """Published events are recorded in history."""
        bus = MessageBus()
        bus.publish("event.a", {"key": 1})
        bus.publish("event.b", {"key": 2})

        history = bus.get_event_history()
        assert len(history) == 2
        assert history[0]["event_name"] == "event.a"
        assert history[1]["event_name"] == "event.b"

    def test_event_history_filter_by_name(self):
        """Can filter history by event name."""
        bus = MessageBus()
        bus.publish("event.a", {"key": 1})
        bus.publish("event.b", {"key": 2})
        bus.publish("event.a", {"key": 3})

        history_a = bus.get_event_history("event.a")
        assert len(history_a) == 2
        assert all(e["event_name"] == "event.a" for e in history_a)

    def test_clear_history(self):
        """History can be cleared."""
        bus = MessageBus()
        bus.publish("event.a", {"key": 1})
        bus.clear_history()
        assert len(bus.get_event_history()) == 0


class TestMessageBusSingleton:
    """Singleton pattern tests."""

    def setup_method(self):
        """Reset singleton before each test."""
        reset_message_bus()

    def teardown_method(self):
        """Reset singleton after each test."""
        reset_message_bus()

    def test_get_message_bus_returns_singleton(self):
        """get_message_bus returns same instance."""
        bus1 = get_message_bus()
        bus2 = get_message_bus()
        assert bus1 is bus2

    def test_reset_creates_new_instance(self):
        """reset_message_bus creates new instance."""
        bus1 = get_message_bus()
        reset_message_bus()
        bus2 = get_message_bus()
        assert bus1 is not bus2


# ============================================================================
# StateStore Tests
# ============================================================================


class TestStateStoreBasics:
    """Basic StateStore CRUD operations."""

    def test_get_nonexistent_returns_default(self):
        """Get on missing key returns default."""
        store = StateStore()
        assert store.get("missing") is None
        assert store.get("missing", "default") == "default"

    def test_set_and_get(self):
        """Set then get retrieves value."""
        store = StateStore()
        store.set("key1", "value1")
        assert store.get("key1") == "value1"

    def test_set_overwrites(self):
        """Set overwrites existing value."""
        store = StateStore()
        store.set("key1", "value1")
        store.set("key1", "value2")
        assert store.get("key1") == "value2"

    def test_delete_existing_key(self):
        """Delete removes existing key."""
        store = StateStore()
        store.set("key1", "value1")
        result = store.delete("key1")
        assert result is True
        assert store.get("key1") is None

    def test_delete_nonexistent_returns_false(self):
        """Delete on missing key returns False."""
        store = StateStore()
        result = store.delete("missing")
        assert result is False

    def test_exists(self):
        """Exists correctly reports key presence."""
        store = StateStore()
        assert store.exists("key1") is False
        store.set("key1", "value1")
        assert store.exists("key1") is True

    def test_size(self):
        """Size returns correct count."""
        store = StateStore()
        assert store.size() == 0
        store.set("key1", "value1")
        store.set("key2", "value2")
        assert store.size() == 2

    def test_clear(self):
        """Clear removes all keys."""
        store = StateStore()
        store.set("key1", "value1")
        store.set("key2", "value2")
        store.clear()
        assert store.size() == 0


class TestStateStoreAppend:
    """List append operations."""

    def test_append_creates_list(self):
        """Append to missing key creates list."""
        store = StateStore()
        length = store.append("list1", "item1")
        assert length == 1
        assert store.get("list1") == ["item1"]

    def test_append_to_existing_list(self):
        """Append adds to existing list."""
        store = StateStore()
        store.append("list1", "item1")
        store.append("list1", "item2")
        assert store.get("list1") == ["item1", "item2"]

    def test_append_to_non_list_raises(self):
        """Append to non-list raises TypeError."""
        store = StateStore()
        store.set("key1", "string_value")
        with pytest.raises(TypeError) as exc_info:
            store.append("key1", "item")
        assert "non-list" in str(exc_info.value)


class TestStateStoreCompareAndSet:
    """Compare-and-set (CAS) atomic operations."""

    def test_cas_succeeds_when_expected_matches(self):
        """CAS succeeds when current value matches expected."""
        store = StateStore()
        store.set("status", "scheduled")

        result = store.compare_and_set("status", "scheduled", "sent")

        assert result is True
        assert store.get("status") == "sent"

    def test_cas_fails_when_expected_differs(self):
        """CAS fails when current value differs from expected."""
        store = StateStore()
        store.set("status", "sent")  # Already changed

        result = store.compare_and_set("status", "scheduled", "delivered")

        assert result is False
        assert store.get("status") == "sent"  # Unchanged

    def test_cas_with_none_expected_for_new_key(self):
        """CAS with None expected works for new key."""
        store = StateStore()

        result = store.compare_and_set("new_key", None, "initial")

        assert result is True
        assert store.get("new_key") == "initial"

    def test_cas_with_none_expected_fails_if_key_exists(self):
        """CAS with None expected fails if key already exists."""
        store = StateStore()
        store.set("existing", "value")

        result = store.compare_and_set("existing", None, "new_value")

        assert result is False
        assert store.get("existing") == "value"

    def test_cas_state_machine_transitions(self):
        """CAS enables safe state machine transitions."""
        store = StateStore()
        store.set("lead:123:status", "scheduled")

        # Valid transition: scheduled -> sent
        assert store.compare_and_set("lead:123:status", "scheduled", "sent")

        # Invalid transition: scheduled -> delivered (already sent)
        assert not store.compare_and_set("lead:123:status", "scheduled", "delivered")

        # Valid transition: sent -> delivered
        assert store.compare_and_set("lead:123:status", "sent", "delivered")

        assert store.get("lead:123:status") == "delivered"


class TestStateStoreKeyOperations:
    """Key listing and filtering."""

    def test_get_all_keys(self):
        """Get all keys in store."""
        store = StateStore()
        store.set("a", 1)
        store.set("b", 2)
        store.set("c", 3)

        keys = store.get_all_keys()
        assert set(keys) == {"a", "b", "c"}

    def test_get_all_keys_with_prefix(self):
        """Get keys filtered by prefix."""
        store = StateStore()
        store.set("lead:123:status", "sent")
        store.set("lead:123:channel", "email")
        store.set("lead:456:status", "scheduled")
        store.set("campaign:1:name", "test")

        lead_123_keys = store.get_all_keys("lead:123:")
        assert set(lead_123_keys) == {"lead:123:status", "lead:123:channel"}

        lead_keys = store.get_all_keys("lead:")
        assert len(lead_keys) == 3


class TestStateStoreSingleton:
    """Singleton pattern tests."""

    def setup_method(self):
        """Reset singleton before each test."""
        reset_state_store()

    def teardown_method(self):
        """Reset singleton after each test."""
        reset_state_store()

    def test_get_state_store_returns_singleton(self):
        """get_state_store returns same instance."""
        store1 = get_state_store()
        store2 = get_state_store()
        assert store1 is store2

    def test_reset_creates_new_instance(self):
        """reset_state_store creates new instance."""
        store1 = get_state_store()
        reset_state_store()
        store2 = get_state_store()
        assert store1 is not store2
