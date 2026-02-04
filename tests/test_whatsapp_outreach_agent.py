"""Tests for WhatsAppOutreachAgent - Phase 6 Channel Agent."""

import pytest
from unittest.mock import MagicMock, patch

from core.infrastructure import MessageBus, StateStore
from pipelines.maps_web_missing.agents.outreach_orchestrator import (
    OutreachOrchestrator,
    OutreachEvent,
    OutreachState,
)
from pipelines.maps_web_missing.agents.whatsapp_outreach_agent import (
    WhatsAppOutreachAgent,
    MockWhatsAppSender,
    DEFAULT_WHATSAPP_TEMPLATE,
)


@pytest.fixture
def fresh_state_store() -> StateStore:
    """Provide a fresh state store for each test."""
    store = StateStore()
    store._data.clear()
    return store


@pytest.fixture
def fresh_message_bus() -> MessageBus:
    """Provide a fresh message bus for each test."""
    bus = MessageBus()
    bus._subscribers.clear()
    bus._event_history.clear()
    return bus


@pytest.fixture
def orchestrator(fresh_state_store: StateStore, fresh_message_bus: MessageBus) -> OutreachOrchestrator:
    """Create orchestrator with fresh infrastructure."""
    return OutreachOrchestrator(
        state_store=fresh_state_store,
        message_bus=fresh_message_bus,
    )


@pytest.fixture
def mock_sender() -> MockWhatsAppSender:
    """Create a mock WhatsApp sender."""
    return MockWhatsAppSender()


@pytest.fixture
def whatsapp_agent(
    orchestrator: OutreachOrchestrator,
    fresh_message_bus: MessageBus,
    mock_sender: MockWhatsAppSender,
) -> WhatsAppOutreachAgent:
    """Create WhatsApp outreach agent with mock sender."""
    agent = WhatsAppOutreachAgent(
        orchestrator=orchestrator,
        message_bus=fresh_message_bus,
        sender=mock_sender,
    )
    agent.start()
    return agent


# =============================================================================
# Initialization Tests
# =============================================================================

class TestWhatsAppOutreachAgentInit:
    """Test WhatsAppOutreachAgent initialization."""

    def test_init_with_defaults(
        self, 
        orchestrator: OutreachOrchestrator,
        fresh_message_bus: MessageBus,
    ):
        """Agent initializes with default mock sender."""
        agent = WhatsAppOutreachAgent(
            orchestrator=orchestrator,
            message_bus=fresh_message_bus,
        )
        
        assert agent.orchestrator is orchestrator
        assert agent.sender is not None
        assert agent._is_mock is True
        assert agent.template == DEFAULT_WHATSAPP_TEMPLATE

    def test_init_with_custom_sender(
        self, 
        orchestrator: OutreachOrchestrator,
        fresh_message_bus: MessageBus,
        mock_sender: MockWhatsAppSender,
    ):
        """Agent can use custom sender."""
        agent = WhatsAppOutreachAgent(
            orchestrator=orchestrator,
            message_bus=fresh_message_bus,
            sender=mock_sender,
        )
        assert agent.sender is mock_sender

    def test_init_with_custom_template(
        self, 
        orchestrator: OutreachOrchestrator,
        fresh_message_bus: MessageBus,
    ):
        """Agent accepts custom template."""
        agent = WhatsAppOutreachAgent(
            orchestrator=orchestrator,
            message_bus=fresh_message_bus,
            template="Custom msg for {business_name}",
        )
        
        assert agent.template == "Custom msg for {business_name}"


# =============================================================================
# Subscription Tests
# =============================================================================

class TestWhatsAppOutreachAgentSubscription:
    """Test event subscription behavior."""

    def test_start_subscribes_to_whatsapp_send(
        self,
        orchestrator: OutreachOrchestrator,
        fresh_message_bus: MessageBus,
    ):
        """start() subscribes to WHATSAPP_SEND events."""
        agent = WhatsAppOutreachAgent(
            orchestrator=orchestrator,
            message_bus=fresh_message_bus,
        )
        
        # Not subscribed yet
        event_key = OutreachEvent.WHATSAPP_SEND.value
        assert event_key not in fresh_message_bus._subscribers
        
        agent.start()
        
        # Now subscribed
        assert event_key in fresh_message_bus._subscribers
        assert agent._handle_whatsapp_send in fresh_message_bus._subscribers[event_key]

    def test_stop_unsubscribes(
        self,
        orchestrator: OutreachOrchestrator,
        fresh_message_bus: MessageBus,
    ):
        """stop() unsubscribes from events."""
        agent = WhatsAppOutreachAgent(
            orchestrator=orchestrator,
            message_bus=fresh_message_bus,
        )
        agent.start()
        agent.stop()
        
        event_key = OutreachEvent.WHATSAPP_SEND.value
        if event_key in fresh_message_bus._subscribers:
            assert agent._handle_whatsapp_send not in fresh_message_bus._subscribers[event_key]


# =============================================================================
# Message Sending Tests
# =============================================================================

class TestWhatsAppOutreachAgentSending:
    """Test WhatsApp sending via event handling."""

    def test_message_sent_on_event(
        self,
        whatsapp_agent: WhatsAppOutreachAgent,
        fresh_message_bus: MessageBus,
        mock_sender: MockWhatsAppSender,
    ):
        """Message is sent when WHATSAPP_SEND event fires."""
        payload = {
            "dedup_key": "test-lead-1",
            "lead": {
                "name": "Test Business",
                "phone": "+1234567890",
            },
        }
        
        fresh_message_bus.publish(OutreachEvent.WHATSAPP_SEND.value, payload)
        
        assert len(mock_sender.sent_messages) == 1
        sent = mock_sender.sent_messages[0]
        assert sent["to"] == "+1234567890"
        assert "Test Business" in sent["message"]

    def test_message_uses_template_formatting(
        self,
        orchestrator: OutreachOrchestrator,
        fresh_message_bus: MessageBus,
    ):
        """Message body uses template with business name."""
        sender = MockWhatsAppSender()
        agent = WhatsAppOutreachAgent(
            orchestrator=orchestrator,
            message_bus=fresh_message_bus,
            sender=sender,
            template="Hi {business_name}!",
        )
        agent.start()
        
        payload = {
            "dedup_key": "test-lead-2",
            "lead": {
                "name": "Acme Corp",
                "phone": "+1555555555",
            },
        }
        
        fresh_message_bus.publish(OutreachEvent.WHATSAPP_SEND.value, payload)
        
        assert sender.sent_messages[0]["message"] == "Hi Acme Corp!"

    def test_message_uses_default_name_if_missing(
        self,
        whatsapp_agent: WhatsAppOutreachAgent,
        fresh_message_bus: MessageBus,
        mock_sender: MockWhatsAppSender,
    ):
        """Message uses 'Your Business' if name is missing."""
        payload = {
            "dedup_key": "test-lead-3",
            "lead": {
                "phone": "+1999999999",
            },
        }
        
        fresh_message_bus.publish(OutreachEvent.WHATSAPP_SEND.value, payload)
        
        assert "Your Business" in mock_sender.sent_messages[0]["message"]


# =============================================================================
# Orchestrator Callback Tests
# =============================================================================

class TestWhatsAppOutreachAgentCallbacks:
    """Test callbacks to orchestrator."""

    def test_mark_whatsapp_sent_on_success(
        self,
        orchestrator: OutreachOrchestrator,
        whatsapp_agent: WhatsAppOutreachAgent,
        fresh_message_bus: MessageBus,
        fresh_state_store: StateStore,
    ):
        """Orchestrator is notified of successful send."""
        dedup_key = "callback-test-1"
        state_key = f"outreach:{dedup_key}"
        
        # Set up lead at QUEUED_WHATSAPP state directly
        fresh_state_store.set(state_key, {"state": OutreachState.QUEUED_WHATSAPP.value})
        
        # Simulate the event
        payload = {
            "dedup_key": dedup_key,
            "lead": {"name": "Test", "phone": "+1234567890"},
        }
        fresh_message_bus.publish(OutreachEvent.WHATSAPP_SEND.value, payload)
        
        # Check state advanced
        state = fresh_state_store.get(state_key)
        assert state["state"] == OutreachState.WHATSAPP_SENT.value


# =============================================================================
# Edge Case Tests
# =============================================================================

class TestWhatsAppOutreachAgentEdgeCases:
    """Test edge cases and error handling."""

    def test_handles_missing_dedup_key(
        self,
        whatsapp_agent: WhatsAppOutreachAgent,
        fresh_message_bus: MessageBus,
        mock_sender: MockWhatsAppSender,
    ):
        """Agent handles missing dedup_key gracefully."""
        payload = {
            "lead": {"phone": "+1234567890"},
        }
        
        # Should not raise, just log warning
        fresh_message_bus.publish(OutreachEvent.WHATSAPP_SEND.value, payload)
        
        # No message should be sent
        assert len(mock_sender.sent_messages) == 0

    def test_handles_missing_phone(
        self,
        orchestrator: OutreachOrchestrator,
        whatsapp_agent: WhatsAppOutreachAgent,
        fresh_message_bus: MessageBus,
        fresh_state_store: StateStore,
        mock_sender: MockWhatsAppSender,
    ):
        """Agent handles missing phone number."""
        dedup_key = "no-phone"
        state_key = f"outreach:{dedup_key}"
        fresh_state_store.set(state_key, {"state": OutreachState.QUEUED_WHATSAPP.value})
        
        payload = {
            "dedup_key": dedup_key,
            "lead": {"name": "No Phone Business"},  # No phone!
        }
        fresh_message_bus.publish(OutreachEvent.WHATSAPP_SEND.value, payload)
        
        # No message sent
        assert len(mock_sender.sent_messages) == 0
        
        # State should advance (marked as sent to exhaust channel)
        state = fresh_state_store.get(state_key)
        assert state["state"] == OutreachState.WHATSAPP_SENT.value

    def test_handles_sender_exception(
        self,
        orchestrator: OutreachOrchestrator,
        fresh_message_bus: MessageBus,
        fresh_state_store: StateStore,
    ):
        """Agent handles sender exceptions gracefully."""
        class FailingSender:
            def send(self, to, message, from_number=None):
                raise RuntimeError("API Error")
        
        agent = WhatsAppOutreachAgent(
            orchestrator=orchestrator,
            message_bus=fresh_message_bus,
            sender=FailingSender(),
        )
        agent.start()
        
        dedup_key = "failing-send"
        state_key = f"outreach:{dedup_key}"
        fresh_state_store.set(state_key, {"state": OutreachState.QUEUED_WHATSAPP.value})
        
        payload = {
            "dedup_key": dedup_key,
            "lead": {"phone": "+1234567890"},
        }
        
        # Should not raise
        fresh_message_bus.publish(OutreachEvent.WHATSAPP_SEND.value, payload)
        
        # State should still advance
        state = fresh_state_store.get(state_key)
        assert state["state"] == OutreachState.WHATSAPP_SENT.value

    def test_handles_sender_returning_false(
        self,
        orchestrator: OutreachOrchestrator,
        fresh_message_bus: MessageBus,
        fresh_state_store: StateStore,
    ):
        """Agent handles sender returning False."""
        class FalseSender:
            def send(self, to, message, from_number=None):
                return False
        
        agent = WhatsAppOutreachAgent(
            orchestrator=orchestrator,
            message_bus=fresh_message_bus,
            sender=FalseSender(),
        )
        agent.start()
        
        dedup_key = "false-send"
        state_key = f"outreach:{dedup_key}"
        fresh_state_store.set(state_key, {"state": OutreachState.QUEUED_WHATSAPP.value})
        
        payload = {
            "dedup_key": dedup_key,
            "lead": {"phone": "+1234567890"},
        }
        fresh_message_bus.publish(OutreachEvent.WHATSAPP_SEND.value, payload)
        
        state = fresh_state_store.get(state_key)
        assert state["state"] == OutreachState.WHATSAPP_SENT.value

    def test_get_sent_count_mock_sender(
        self,
        whatsapp_agent: WhatsAppOutreachAgent,
        fresh_message_bus: MessageBus,
    ):
        """get_sent_count returns count for mock sender."""
        assert whatsapp_agent.get_sent_count() == 0
        
        payload = {
            "dedup_key": "count-test",
            "lead": {"phone": "+1234567890"},
        }
        fresh_message_bus.publish(OutreachEvent.WHATSAPP_SEND.value, payload)
        
        assert whatsapp_agent.get_sent_count() == 1


# =============================================================================
# MockWhatsAppSender Tests
# =============================================================================

class TestMockWhatsAppSender:
    """Test MockWhatsAppSender directly."""

    def test_mock_sender_records_messages(self):
        """MockWhatsAppSender records all sent messages."""
        sender = MockWhatsAppSender()
        
        sender.send("+111", "Message A")
        sender.send("+222", "Message B", from_number="+000")
        
        assert len(sender.sent_messages) == 2
        assert sender.sent_messages[0]["to"] == "+111"
        assert sender.sent_messages[1]["from"] == "+000"

    def test_mock_sender_includes_timestamp(self):
        """MockWhatsAppSender includes timestamp."""
        sender = MockWhatsAppSender()
        sender.send("+123", "Test message")
        
        assert "sent_at" in sender.sent_messages[0]

    def test_mock_sender_always_returns_true(self):
        """MockWhatsAppSender always succeeds."""
        sender = MockWhatsAppSender()
        result = sender.send("+123", "Test")
        assert result is True


# =============================================================================
# Integration-Like Tests
# =============================================================================

class TestWhatsAppAgentIntegration:
    """Test WhatsApp agent in integration scenarios."""

    def test_full_outreach_flow(
        self,
        orchestrator: OutreachOrchestrator,
        fresh_message_bus: MessageBus,
        fresh_state_store: StateStore,
    ):
        """Test complete flow from email through whatsapp."""
        from pipelines.maps_web_missing.agents.email_outreach_agent import (
            EmailOutreachAgent,
            MockEmailSender,
        )
        
        # Set up both agents
        email_sender = MockEmailSender()
        whatsapp_sender = MockWhatsAppSender()
        
        email_agent = EmailOutreachAgent(
            orchestrator=orchestrator,
            message_bus=fresh_message_bus,
            sender=email_sender,
        )
        email_agent.start()
        
        whatsapp_agent = WhatsAppOutreachAgent(
            orchestrator=orchestrator,
            message_bus=fresh_message_bus,
            sender=whatsapp_sender,
        )
        whatsapp_agent.start()
        
        # Set up lead at QUEUED_EMAIL state
        dedup_key = "full-flow-test"
        state_key = f"outreach:{dedup_key}"
        lead = {
            "name": "Full Flow Business",
            "email": "flow@example.com",
            "phone": "+1555123456",
        }
        
        fresh_state_store.set(state_key, {"state": OutreachState.QUEUED_EMAIL.value})
        
        # Email event fires - agent handles it
        email_event = {
            "dedup_key": dedup_key,
            "lead": lead,
        }
        fresh_message_bus.publish(OutreachEvent.EMAIL_SEND.value, email_event)
        
        # Verify email sent
        assert len(email_sender.sent_emails) == 1
        
        # Check email_sent state
        state = fresh_state_store.get(state_key)
        assert state["state"] == OutreachState.EMAIL_SENT.value
        
        # Set up for WhatsApp (advance state as orchestrator would)
        fresh_state_store.set(state_key, {"state": OutreachState.QUEUED_WHATSAPP.value})
        
        # Now WhatsApp event fires
        whatsapp_event = {
            "dedup_key": dedup_key,
            "lead": lead,
        }
        fresh_message_bus.publish(OutreachEvent.WHATSAPP_SEND.value, whatsapp_event)
        
        # Verify WhatsApp sent
        assert len(whatsapp_sender.sent_messages) == 1
        
        # State should be WHATSAPP_SENT
        state = fresh_state_store.get(state_key)
        assert state["state"] == OutreachState.WHATSAPP_SENT.value
