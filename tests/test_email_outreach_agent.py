"""Tests for EmailOutreachAgent - Phase 6 Channel Agent."""

import pytest
from unittest.mock import MagicMock, patch

from core.infrastructure import MessageBus, StateStore
from pipelines.maps_web_missing.agents.outreach_orchestrator import (
    OutreachOrchestrator,
    OutreachEvent,
    OutreachState,
)
from pipelines.maps_web_missing.agents.email_outreach_agent import (
    EmailOutreachAgent,
    MockEmailSender,
    DEFAULT_EMAIL_SUBJECT,
    DEFAULT_EMAIL_TEMPLATE,
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
def mock_sender() -> MockEmailSender:
    """Create a mock email sender."""
    return MockEmailSender()


@pytest.fixture
def email_agent(
    orchestrator: OutreachOrchestrator,
    fresh_message_bus: MessageBus,
    mock_sender: MockEmailSender,
) -> EmailOutreachAgent:
    """Create email outreach agent with mock sender."""
    agent = EmailOutreachAgent(
        orchestrator=orchestrator,
        message_bus=fresh_message_bus,
        sender=mock_sender,
    )
    agent.start()
    return agent


# =============================================================================
# Initialization Tests
# =============================================================================

class TestEmailOutreachAgentInit:
    """Test EmailOutreachAgent initialization."""

    def test_init_with_defaults(
        self, 
        orchestrator: OutreachOrchestrator,
        fresh_message_bus: MessageBus,
    ):
        """Agent initializes with default mock sender."""
        agent = EmailOutreachAgent(
            orchestrator=orchestrator,
            message_bus=fresh_message_bus,
        )
        
        assert agent.orchestrator is orchestrator
        assert agent.sender is not None
        assert agent._is_mock is True
        assert agent.subject == DEFAULT_EMAIL_SUBJECT
        assert agent.template == DEFAULT_EMAIL_TEMPLATE

    def test_init_with_custom_sender(
        self, 
        orchestrator: OutreachOrchestrator,
        fresh_message_bus: MessageBus,
        mock_sender: MockEmailSender,
    ):
        """Agent can use custom sender."""
        agent = EmailOutreachAgent(
            orchestrator=orchestrator,
            message_bus=fresh_message_bus,
            sender=mock_sender,
        )
        assert agent.sender is mock_sender

    def test_init_with_custom_templates(
        self, 
        orchestrator: OutreachOrchestrator,
        fresh_message_bus: MessageBus,
    ):
        """Agent accepts custom subject and template."""
        agent = EmailOutreachAgent(
            orchestrator=orchestrator,
            message_bus=fresh_message_bus,
            subject="Custom Subject",
            template="Custom body {business_name}",
        )
        
        assert agent.subject == "Custom Subject"
        assert agent.template == "Custom body {business_name}"


# =============================================================================
# Subscription Tests
# =============================================================================

class TestEmailOutreachAgentSubscription:
    """Test event subscription behavior."""

    def test_start_subscribes_to_email_send(
        self,
        orchestrator: OutreachOrchestrator,
        fresh_message_bus: MessageBus,
    ):
        """start() subscribes to EMAIL_SEND events."""
        agent = EmailOutreachAgent(
            orchestrator=orchestrator,
            message_bus=fresh_message_bus,
        )
        
        # Not subscribed yet
        event_key = OutreachEvent.EMAIL_SEND.value
        assert event_key not in fresh_message_bus._subscribers
        
        agent.start()
        
        # Now subscribed
        assert event_key in fresh_message_bus._subscribers
        assert agent._handle_email_send in fresh_message_bus._subscribers[event_key]

    def test_stop_unsubscribes(
        self,
        orchestrator: OutreachOrchestrator,
        fresh_message_bus: MessageBus,
    ):
        """stop() unsubscribes from events."""
        agent = EmailOutreachAgent(
            orchestrator=orchestrator,
            message_bus=fresh_message_bus,
        )
        agent.start()
        agent.stop()
        
        event_key = OutreachEvent.EMAIL_SEND.value
        if event_key in fresh_message_bus._subscribers:
            assert agent._handle_email_send not in fresh_message_bus._subscribers[event_key]


# =============================================================================
# Email Sending Tests
# =============================================================================

class TestEmailOutreachAgentSending:
    """Test email sending via event handling."""

    def test_email_sent_on_event(
        self,
        email_agent: EmailOutreachAgent,
        fresh_message_bus: MessageBus,
        mock_sender: MockEmailSender,
    ):
        """Email is sent when EMAIL_SEND event fires."""
        payload = {
            "dedup_key": "test-lead-1",
            "lead": {
                "name": "Test Business",
                "email": "test@example.com",
            },
        }
        
        fresh_message_bus.publish(OutreachEvent.EMAIL_SEND.value, payload)
        
        assert len(mock_sender.sent_emails) == 1
        sent = mock_sender.sent_emails[0]
        assert sent["to"] == "test@example.com"
        assert sent["subject"] == DEFAULT_EMAIL_SUBJECT
        assert "Test Business" in sent["body"]

    def test_email_uses_template_formatting(
        self,
        orchestrator: OutreachOrchestrator,
        fresh_message_bus: MessageBus,
    ):
        """Email body uses template with business name."""
        sender = MockEmailSender()
        agent = EmailOutreachAgent(
            orchestrator=orchestrator,
            message_bus=fresh_message_bus,
            sender=sender,
            template="Hello {business_name}!",
        )
        agent.start()
        
        payload = {
            "dedup_key": "test-lead-2",
            "lead": {
                "name": "Acme Corp",
                "email": "contact@acme.com",
            },
        }
        
        fresh_message_bus.publish(OutreachEvent.EMAIL_SEND.value, payload)
        
        assert sender.sent_emails[0]["body"] == "Hello Acme Corp!"

    def test_email_uses_default_name_if_missing(
        self,
        email_agent: EmailOutreachAgent,
        fresh_message_bus: MessageBus,
        mock_sender: MockEmailSender,
    ):
        """Email uses 'Your Business' if name is missing."""
        payload = {
            "dedup_key": "test-lead-3",
            "lead": {
                "email": "unknown@example.com",
            },
        }
        
        fresh_message_bus.publish(OutreachEvent.EMAIL_SEND.value, payload)
        
        assert "Your Business" in mock_sender.sent_emails[0]["body"]


# =============================================================================
# Orchestrator Callback Tests
# =============================================================================

class TestEmailOutreachAgentCallbacks:
    """Test callbacks to orchestrator."""

    def test_mark_email_sent_on_success(
        self,
        orchestrator: OutreachOrchestrator,
        email_agent: EmailOutreachAgent,
        fresh_message_bus: MessageBus,
        fresh_state_store: StateStore,
    ):
        """Orchestrator is notified of successful send."""
        # Set up lead at QUEUED_EMAIL state
        dedup_key = "callback-test-1"
        state_key = f"outreach:{dedup_key}"
        fresh_state_store.set(state_key, {"state": OutreachState.QUEUED_EMAIL.value})
        
        # Simulate the event (which orchestrator would publish)
        payload = {
            "dedup_key": dedup_key,
            "lead": {"name": "Test", "email": "test@test.com"},
        }
        fresh_message_bus.publish(OutreachEvent.EMAIL_SEND.value, payload)
        
        # Check state advanced
        state = fresh_state_store.get(state_key)
        assert state["state"] == OutreachState.EMAIL_SENT.value

    def test_mark_email_failed_on_no_email(
        self,
        orchestrator: OutreachOrchestrator,
        email_agent: EmailOutreachAgent,
        fresh_message_bus: MessageBus,
        fresh_state_store: StateStore,
    ):
        """Orchestrator notified of failure when no email address."""
        dedup_key = "no-email-lead"
        state_key = f"outreach:{dedup_key}"
        fresh_state_store.set(state_key, {"state": OutreachState.QUEUED_EMAIL.value})
        
        payload = {
            "dedup_key": dedup_key,
            "lead": {"name": "No Email Business"},  # No email!
        }
        fresh_message_bus.publish(OutreachEvent.EMAIL_SEND.value, payload)
        
        # Should move to EMAIL_FAILED after email failure
        state = fresh_state_store.get(state_key)
        assert state["state"] == OutreachState.EMAIL_FAILED.value


# =============================================================================
# Edge Case Tests
# =============================================================================

class TestEmailOutreachAgentEdgeCases:
    """Test edge cases and error handling."""

    def test_handles_missing_dedup_key(
        self,
        email_agent: EmailOutreachAgent,
        fresh_message_bus: MessageBus,
        mock_sender: MockEmailSender,
    ):
        """Agent handles missing dedup_key gracefully."""
        payload = {
            "lead": {"email": "test@test.com"},
        }
        
        # Should not raise, just log warning
        fresh_message_bus.publish(OutreachEvent.EMAIL_SEND.value, payload)
        
        # No email should be sent
        assert len(mock_sender.sent_emails) == 0

    def test_handles_sender_exception(
        self,
        orchestrator: OutreachOrchestrator,
        fresh_message_bus: MessageBus,
        fresh_state_store: StateStore,
    ):
        """Agent handles sender exceptions gracefully."""
        # Create sender that raises
        class FailingSender:
            def send(self, to, subject, body, from_email=None):
                raise RuntimeError("SMTP Error")
        
        agent = EmailOutreachAgent(
            orchestrator=orchestrator,
            message_bus=fresh_message_bus,
            sender=FailingSender(),
        )
        agent.start()
        
        dedup_key = "failing-send"
        state_key = f"outreach:{dedup_key}"
        fresh_state_store.set(state_key, {"state": OutreachState.QUEUED_EMAIL.value})
        
        payload = {
            "dedup_key": dedup_key,
            "lead": {"email": "test@test.com"},
        }
        
        # Should not raise
        fresh_message_bus.publish(OutreachEvent.EMAIL_SEND.value, payload)
        
        # State should reflect failure (EMAIL_FAILED)
        state = fresh_state_store.get(state_key)
        assert state["state"] == OutreachState.EMAIL_FAILED.value

    def test_handles_sender_returning_false(
        self,
        orchestrator: OutreachOrchestrator,
        fresh_message_bus: MessageBus,
        fresh_state_store: StateStore,
    ):
        """Agent handles sender returning False."""
        class FalseSender:
            def send(self, to, subject, body, from_email=None):
                return False
        
        agent = EmailOutreachAgent(
            orchestrator=orchestrator,
            message_bus=fresh_message_bus,
            sender=FalseSender(),
        )
        agent.start()
        
        dedup_key = "false-send"
        state_key = f"outreach:{dedup_key}"
        fresh_state_store.set(state_key, {"state": OutreachState.QUEUED_EMAIL.value})
        
        payload = {
            "dedup_key": dedup_key,
            "lead": {"email": "test@test.com"},
        }
        fresh_message_bus.publish(OutreachEvent.EMAIL_SEND.value, payload)
        
        state = fresh_state_store.get(state_key)
        assert state["state"] == OutreachState.EMAIL_FAILED.value

    def test_get_sent_count_mock_sender(
        self,
        email_agent: EmailOutreachAgent,
        fresh_message_bus: MessageBus,
    ):
        """get_sent_count returns count for mock sender."""
        assert email_agent.get_sent_count() == 0
        
        payload = {
            "dedup_key": "count-test",
            "lead": {"email": "test@test.com"},
        }
        fresh_message_bus.publish(OutreachEvent.EMAIL_SEND.value, payload)
        
        assert email_agent.get_sent_count() == 1


# =============================================================================
# MockEmailSender Tests
# =============================================================================

class TestMockEmailSender:
    """Test MockEmailSender directly."""

    def test_mock_sender_records_emails(self):
        """MockEmailSender records all sent emails."""
        sender = MockEmailSender()
        
        sender.send("a@a.com", "Subject A", "Body A")
        sender.send("b@b.com", "Subject B", "Body B", from_email="from@test.com")
        
        assert len(sender.sent_emails) == 2
        assert sender.sent_emails[0]["to"] == "a@a.com"
        assert sender.sent_emails[1]["from"] == "from@test.com"

    def test_mock_sender_includes_timestamp(self):
        """MockEmailSender includes timestamp."""
        sender = MockEmailSender()
        sender.send("test@test.com", "Test", "Body")
        
        assert "sent_at" in sender.sent_emails[0]

    def test_mock_sender_always_returns_true(self):
        """MockEmailSender always succeeds."""
        sender = MockEmailSender()
        result = sender.send("test@test.com", "Test", "Body")
        assert result is True
